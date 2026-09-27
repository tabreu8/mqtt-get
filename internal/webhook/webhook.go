// Package webhook forwards MQTT messages to HTTP endpoints.
//
// Each webhook has its own bounded queue and worker goroutines so a slow
// endpoint never blocks ingestion or other webhooks: when a queue is full,
// messages for that webhook are dropped and counted. Matching uses an
// immutable topic trie swapped atomically on configuration changes, so the
// hot path takes no locks.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
	"github.com/tabreu8/mqtt-get/internal/topic"
)

// Stats are per-webhook counters.
type Stats struct {
	Delivered uint64    `json:"delivered"`
	Failed    uint64    `json:"failed"`
	Dropped   uint64    `json:"dropped"`
	Requests  uint64    `json:"requests"`
	Queued    int       `json:"queued"`
	LastError string    `json:"last_error,omitempty"`
	LastErrAt time.Time `json:"last_error_at,omitzero"`
}

type hook struct {
	cfg   config.Webhook
	queue chan *store.Entry
	stop  chan struct{}
	wg    sync.WaitGroup

	delivered, failed, dropped, requests atomic.Uint64
	errMu                                sync.Mutex
	lastErr                              string
	lastErrAt                            time.Time
}

var matchPool = sync.Pool{New: func() any { b := make([]*hook, 0, 8); return &b }}

// Dispatcher routes messages to webhooks.
type Dispatcher struct {
	log    *slog.Logger
	client *http.Client

	mu    sync.Mutex
	hooks map[string]*hook
	trie  atomic.Pointer[topic.Trie[*hook]]
}

// New creates a dispatcher.
func New(log *slog.Logger) *Dispatcher {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return &Dispatcher{
		log:    log,
		client: &http.Client{Transport: tr},
		hooks:  map[string]*hook{},
	}
}

// Dispatch enqueues e for every matching webhook. It never blocks.
// The entry is only copied to the heap when a webhook matches.
func (d *Dispatcher) Dispatch(e store.Entry) {
	t := d.trie.Load()
	if t == nil || t.Len() == 0 {
		return
	}
	// Pooled match buffer: the trie's recursive matcher would otherwise
	// force a heap allocation on every message.
	bp := matchPool.Get().(*[]*hook)
	defer matchPool.Put(bp)
	matches := t.Match(e.Topic, (*bp)[:0])
	*bp = matches[:0]
	if len(matches) == 0 {
		return
	}
	p := new(store.Entry)
	*p = e
	for _, h := range matches {
		select {
		case h.queue <- p:
		default:
			h.dropped.Add(1)
		}
	}
}

// Set replaces the complete set of webhooks.
func (d *Dispatcher) Set(hooks []config.Webhook) {
	d.mu.Lock()
	defer d.mu.Unlock()
	keep := map[string]bool{}
	for _, w := range hooks {
		keep[w.ID] = true
		d.putLocked(w)
	}
	for id := range d.hooks {
		if !keep[id] {
			d.removeLocked(id)
		}
	}
	d.rebuildLocked()
}

// Put adds or replaces one webhook.
func (d *Dispatcher) Put(w config.Webhook) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.putLocked(w)
	d.rebuildLocked()
}

// Remove deletes a webhook.
func (d *Dispatcher) Remove(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.removeLocked(id)
	d.rebuildLocked()
}

// Close stops all workers.
func (d *Dispatcher) Close() { d.Set(nil) }

func (d *Dispatcher) putLocked(w config.Webhook) {
	w.Normalize()
	if old, ok := d.hooks[w.ID]; ok {
		d.stopHook(old)
	}
	h := &hook{cfg: w, queue: make(chan *store.Entry, w.QueueSize), stop: make(chan struct{})}
	d.hooks[w.ID] = h
	if !w.IsEnabled() {
		return
	}
	for i := 0; i < w.Concurrency; i++ {
		h.wg.Add(1)
		go d.worker(h)
	}
}

func (d *Dispatcher) removeLocked(id string) {
	if h, ok := d.hooks[id]; ok {
		d.stopHook(h)
		delete(d.hooks, id)
	}
}

func (d *Dispatcher) stopHook(h *hook) {
	close(h.stop)
	// Don't wait for in-flight requests while holding the lock for long:
	// workers exit after their current request.
	go h.wg.Wait()
}

func (d *Dispatcher) rebuildLocked() {
	t := topic.NewTrie[*hook]()
	for _, h := range d.hooks {
		if !h.cfg.IsEnabled() {
			continue
		}
		for _, f := range h.cfg.Topics {
			t.Insert(f, h)
		}
	}
	d.trie.Store(t)
}

// Stats returns counters for a webhook.
func (d *Dispatcher) Stats(id string) (Stats, bool) {
	d.mu.Lock()
	h, ok := d.hooks[id]
	d.mu.Unlock()
	if !ok {
		return Stats{}, false
	}
	return h.stats(), true
}

// AllStats returns counters for every webhook.
func (d *Dispatcher) AllStats() map[string]Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]Stats, len(d.hooks))
	for id, h := range d.hooks {
		out[id] = h.stats()
	}
	return out
}

func (h *hook) stats() Stats {
	h.errMu.Lock()
	defer h.errMu.Unlock()
	return Stats{
		Delivered: h.delivered.Load(),
		Failed:    h.failed.Load(),
		Dropped:   h.dropped.Load(),
		Requests:  h.requests.Load(),
		Queued:    len(h.queue),
		LastError: h.lastErr,
		LastErrAt: h.lastErrAt,
	}
}

func (h *hook) setErr(err error) {
	h.errMu.Lock()
	h.lastErr = err.Error()
	h.lastErrAt = time.Now()
	h.errMu.Unlock()
}

func (d *Dispatcher) worker(h *hook) {
	defer h.wg.Done()
	cfg := h.cfg
	interval := time.Duration(cfg.BatchIntervalMs) * time.Millisecond
	batch := make([]*store.Entry, 0, cfg.BatchSize)
	var buf []byte
	for {
		batch = batch[:0]
		select {
		case <-h.stop:
			return
		case e := <-h.queue:
			batch = append(batch, e)
		}
		if cfg.BatchSize > 1 {
			timer := time.NewTimer(interval)
		collect:
			for len(batch) < cfg.BatchSize {
				select {
				case e := <-h.queue:
					batch = append(batch, e)
				case <-timer.C:
					break collect
				case <-h.stop:
					break collect
				}
			}
			timer.Stop()
		}
		buf = d.deliver(h, batch, buf[:0])
		if cap(buf) > 4<<20 {
			buf = nil // don't retain huge buffers
		}
	}
}

// deliver sends a batch with retries and updates counters. It returns the
// (reusable) body buffer.
func (d *Dispatcher) deliver(h *hook, batch []*store.Entry, buf []byte) []byte {
	body, contentType := encode(h.cfg, batch, buf)
	var err error
	backoff := 250 * time.Millisecond
	for attempt := 0; ; attempt++ {
		var retry bool
		retry, err = d.send(h, body, contentType, batch)
		h.requests.Add(1)
		if err == nil {
			h.delivered.Add(uint64(len(batch)))
			return body
		}
		if !retry || attempt >= h.cfg.Retries() {
			break
		}
		select {
		case <-h.stop:
			attempt = h.cfg.Retries()
		case <-time.After(backoff):
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
	h.failed.Add(uint64(len(batch)))
	h.setErr(err)
	d.log.Debug("webhook delivery failed", "id", h.cfg.ID, "err", err)
	return body
}

func encode(cfg config.Webhook, batch []*store.Entry, buf []byte) ([]byte, string) {
	if cfg.Format == "raw" {
		e := batch[0]
		ct := "application/octet-stream"
		switch store.Encoding(e.Payload) {
		case store.EncJSON:
			ct = "application/json"
		case store.EncUTF8:
			ct = "text/plain; charset=utf-8"
		}
		return e.Payload, ct
	}
	if cfg.BatchSize > 1 {
		buf = append(buf, `{"messages":[`...)
		for i, e := range batch {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = store.AppendJSON(buf, e)
		}
		buf = append(buf, "]}"...)
		return buf, "application/json"
	}
	return store.AppendJSON(buf, batch[0]), "application/json"
}

func (d *Dispatcher) send(h *hook, body []byte, contentType string, batch []*store.Entry) (retry bool, err error) {
	cfg := h.cfg
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TimeoutMs)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "mqtt-get")
	req.Header.Set("X-MQTT-Get-Webhook", cfg.ID)
	if cfg.Format == "raw" {
		e := batch[0]
		req.Header.Set("X-MQTT-Topic", e.Topic)
		req.Header.Set("X-MQTT-QoS", strconv.Itoa(int(e.QoS)))
		req.Header.Set("X-MQTT-Retained", strconv.FormatBool(e.Retained))
		req.Header.Set("X-MQTT-Timestamp", time.Unix(0, e.Time).UTC().Format(time.RFC3339Nano))
		e.Props.WriteHeaders(req.Header.Add)
		if e.Props != nil && e.Props.ContentType != "" {
			req.Header.Set("Content-Type", e.Props.ContentType)
		}
	}
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	if cfg.Secret != "" {
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		mac := hmac.New(sha256.New, []byte(cfg.Secret))
		mac.Write([]byte(ts))
		mac.Write([]byte{'.'})
		mac.Write(body)
		req.Header.Set("X-MQTT-Get-Timestamp", ts)
		req.Header.Set("X-MQTT-Get-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return true, err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return false, nil
	}
	err = fmt.Errorf("%s %s: HTTP %d", cfg.Method, cfg.URL, resp.StatusCode)
	return resp.StatusCode >= 500 || resp.StatusCode == 429 || resp.StatusCode == 408, err
}

// Test sends a synthetic message to the webhook synchronously and returns
// the result, without touching counters.
func (d *Dispatcher) Test(w config.Webhook, e *store.Entry) error {
	w.Normalize()
	h := &hook{cfg: w, stop: make(chan struct{})}
	body, ct := encode(w, []*store.Entry{e}, nil)
	_, err := d.send(h, body, ct, []*store.Entry{e})
	return err
}
