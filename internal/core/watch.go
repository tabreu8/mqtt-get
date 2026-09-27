package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/store"
	"github.com/tabreu8/mqtt-get/internal/topic"
)

// MaxWatchers bounds concurrent waits and subscriptions.
const MaxWatchers = 10000

// Wait limits.
const (
	DefaultWaitTimeout = 30 * time.Second
	MaxWaitTimeout     = 5 * time.Minute
)

var watchPool = sync.Pool{New: func() any { b := make([]*watcher, 0, 8); return &b }}

type watcher struct {
	filter string
	fn     func(*store.Entry)
}

// watchers lets callers observe live messages. Like webhooks, matching uses
// an immutable trie swapped atomically, and costs one atomic load per
// message when nobody is watching.
type watchers struct {
	mu   sync.Mutex
	set  map[*watcher]struct{}
	trie atomic.Pointer[topic.Trie[*watcher]]
	n    atomic.Int32
}

func newWatchers() *watchers { return &watchers{set: map[*watcher]struct{}{}} }

func (w *watchers) count() int { return int(w.n.Load()) }

func (w *watchers) dispatch(e store.Entry) {
	if w.n.Load() == 0 {
		return
	}
	t := w.trie.Load()
	bp := watchPool.Get().(*[]*watcher)
	defer watchPool.Put(bp)
	matches := t.Match(e.Topic, (*bp)[:0])
	*bp = matches[:0]
	if len(matches) == 0 {
		return
	}
	p := new(store.Entry)
	*p = e
	for _, x := range matches {
		x.fn(p)
	}
}

func (w *watchers) add(filter string, fn func(*store.Entry)) (func(), error) {
	x := &watcher{filter: filter, fn: fn}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.set) >= MaxWatchers {
		return nil, &Error{Kind: Busy, Msg: "too many active watches; try again later"}
	}
	w.set[x] = struct{}{}
	w.rebuildLocked()
	var once sync.Once
	return func() {
		once.Do(func() {
			w.mu.Lock()
			delete(w.set, x)
			w.rebuildLocked()
			w.mu.Unlock()
		})
	}, nil
}

func (w *watchers) rebuildLocked() {
	t := topic.NewTrie[*watcher]()
	for x := range w.set {
		t.Insert(x.filter, x)
	}
	w.trie.Store(t)
	w.n.Store(int32(len(w.set)))
}

// Watch calls fn for every new message matching filter until the returned
// cancel function is called. fn runs on the ingest path: it must not block.
func (s *Service) Watch(filter string, fn func(*store.Entry)) (cancel func(), err error) {
	if err := topic.ValidateFilter(filter); err != nil {
		return nil, invalid(err)
	}
	return s.watch.add(filter, fn)
}

func clampTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return DefaultWaitTimeout
	}
	if d > MaxWaitTimeout {
		return MaxWaitTimeout
	}
	return d
}

// Wait blocks until a message matching filter arrives and returns it. It
// returns (nil, nil) when timeout expires first. With includeCurrent, an
// already-stored matching value is returned immediately (the newest one).
func (s *Service) Wait(ctx context.Context, filter string, timeout time.Duration, includeCurrent bool) (*store.Entry, error) {
	if err := topic.ValidateFilter(filter); err != nil {
		return nil, invalid(err)
	}
	ch := make(chan *store.Entry, 1)
	cancel, err := s.watch.add(filter, func(e *store.Entry) {
		select {
		case ch <- e:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	defer cancel()
	if includeCurrent {
		if e := s.newest(filter); e != nil {
			return e, nil
		}
	}
	return waitCh(ctx, ch, clampTimeout(timeout))
}

func waitCh(ctx context.Context, ch <-chan *store.Entry, timeout time.Duration) (*store.Entry, error) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case e := <-ch:
		return e, nil
	case <-t.C:
		return nil, nil
	case <-ctx.Done():
		return nil, &Error{Kind: Unavailable, Msg: "cancelled: " + ctx.Err().Error()}
	}
}

// newest returns the most recently received stored value matching filter.
func (s *Service) newest(filter string) *store.Entry {
	if !topic.HasWildcard(filter) {
		e, _ := s.store.Get(filter)
		return e
	}
	res, _ := s.store.Query(filter, 0)
	var best *store.Entry
	for _, e := range res {
		if best == nil || e.Time > best.Time {
			best = e
		}
	}
	return best
}

// PublishAndWait publishes msg and waits for the first message matching
// responseFilter (request/response, e.g. command topic -> state topic).
// Messages on msg.Topic itself are ignored so the command's own echo never
// counts as the response. Returns (nil, nil) on timeout.
//
// Over MQTT 5 it uses native request/response: the request carries a
// response topic (when responseFilter is a plain topic) and random
// correlation data, and replies carrying other correlation data (answers to
// someone else's request) are ignored. Replies without correlation data
// (plain state updates) still count.
func (s *Service) PublishAndWait(ctx context.Context, msg mqttc.Message, responseFilter string, timeout time.Duration) (*store.Entry, error) {
	if err := topic.ValidateFilter(responseFilter); err != nil {
		return nil, invalid(err)
	}
	var correlation []byte
	if s.mqtt.Config().ProtocolVersion == 5 {
		p := store.Props{}
		if msg.Props != nil {
			p = *msg.Props
		}
		if len(p.CorrelationData) == 0 {
			var b [12]byte
			_, _ = rand.Read(b[:])
			p.CorrelationData = []byte("mqtt-get-" + hex.EncodeToString(b[:]))
		}
		if p.ResponseTopic == "" && !topic.HasWildcard(responseFilter) {
			p.ResponseTopic = responseFilter
		}
		correlation = p.CorrelationData
		msg.Props = &p
	}
	ch := make(chan *store.Entry, 1)
	cancel, err := s.watch.add(responseFilter, func(e *store.Entry) {
		if e.Topic == msg.Topic {
			return
		}
		if correlation != nil && e.Props != nil && len(e.Props.CorrelationData) > 0 &&
			!bytes.Equal(e.Props.CorrelationData, correlation) {
			return // a response to another request
		}
		select {
		case ch <- e:
		default:
		}
	})
	if err != nil {
		return nil, err
	}
	defer cancel()
	if _, err := s.Publish(ctx, []mqttc.Message{msg}); err != nil {
		return nil, err
	}
	return waitCh(ctx, ch, clampTimeout(timeout))
}
