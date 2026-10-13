package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// WebSocket stream limits (variables so tests can shrink them).
var (
	wsQueue        = 4096 // messages buffered per connection before dropping
	wsMaxSubs      = 100  // subscriptions per connection
	wsMaxCurrent   = 1000 // current values sent for include_current
	wsAuthTimeout  = 10 * time.Second
	wsPingInterval = 30 * time.Second
	wsPongWait     = 75 * time.Second
	wsWriteTimeout = 10 * time.Second
)

// wsRequest is a message from the client.
type wsRequest struct {
	Type           string          `json:"type"`
	ID             json.RawMessage `json:"id,omitempty"`
	Key            string          `json:"key,omitempty"`
	Filter         string          `json:"filter,omitempty"`
	Filters        []string        `json:"filters,omitempty"`
	IncludeCurrent bool            `json:"include_current,omitempty"`
	core.PublishRequest
}

type wsConn struct {
	s *Server
	c *websocket.Conn
	p *core.Principal
	// out is the single, ordered queue of frames. Live messages (pushed
	// from the ingest path) are dropped when it is full; replies and
	// current values (sent from the connection's own goroutine) wait.
	out     chan []byte
	done    chan struct{}
	dropped atomic.Uint64

	mu   sync.Mutex
	subs map[string]func()
}

func (s *Server) checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true // not a browser
	}
	if u, err := url.Parse(origin); err == nil && strings.EqualFold(u.Host, r.Host) {
		return true
	}
	for _, o := range strings.Split(s.cfg.CORSOrigins, ",") {
		if o = strings.TrimSpace(o); o == "*" || strings.EqualFold(o, origin) {
			return true
		}
	}
	return false
}

// handleWS streams messages over a WebSocket. The API key comes from the
// usual headers, or (for browsers, which can't set headers on WebSockets)
// from a first {"type":"auth","key":"..."} message. ?filter= (repeatable)
// subscribes on connect.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	var p *core.Principal
	if key := APIKey(r, s.cfg.AllowQueryAPIKey); key != "" || s.cfg.AuthDisabled {
		var err error
		if p, err = s.svc.Authenticate(key); err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="mqtt-get"`)
			writeError(w, err)
			return
		}
	}
	up := websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 16 << 10, CheckOrigin: s.checkOrigin}
	c, err := up.Upgrade(w, r, nil)
	if err != nil {
		return // the upgrader already replied
	}
	wc := &wsConn{s: s, c: c, p: p, out: make(chan []byte, wsQueue), done: make(chan struct{}), subs: map[string]func(){}}
	c.SetReadLimit(max(s.cfg.MaxBodyBytes, 64<<10))
	go wc.writer()
	defer wc.close()

	if wc.p == nil && !wc.authenticate() {
		return
	}
	q := r.URL.Query()
	current, _ := strconv.ParseBool(q.Get("include_current"))
	for _, f := range q["filter"] {
		wc.subscribe(nil, f, current)
	}
	wc.reader()
}

func (wc *wsConn) close() {
	wc.mu.Lock()
	for f, cancel := range wc.subs {
		cancel()
		delete(wc.subs, f)
	}
	wc.mu.Unlock()
	select {
	case <-wc.done:
	default:
		close(wc.done)
	}
	_ = wc.c.Close()
}

// authenticate waits for the auth message.
func (wc *wsConn) authenticate() bool {
	_ = wc.c.SetReadDeadline(time.Now().Add(wsAuthTimeout))
	var req wsRequest
	if err := wc.c.ReadJSON(&req); err != nil || req.Type != "auth" {
		wc.closeWith(4401, "authenticate first: send {\"type\":\"auth\",\"key\":\"<API key>\"}")
		return false
	}
	p, err := wc.s.svc.Authenticate(req.Key)
	if err != nil {
		wc.closeWith(4401, err.Error())
		return false
	}
	wc.p = p
	wc.reply(req.ID, map[string]any{"type": "authenticated", "name": p.Name, "scopes": p.Scopes})
	return true
}

func (wc *wsConn) closeWith(code int, reason string) {
	msg := websocket.FormatCloseMessage(code, reason)
	_ = wc.c.WriteControl(websocket.CloseMessage, msg, time.Now().Add(time.Second))
}

func (wc *wsConn) reader() {
	_ = wc.c.SetReadDeadline(time.Now().Add(wsPongWait))
	wc.c.SetPongHandler(func(string) error { return wc.c.SetReadDeadline(time.Now().Add(wsPongWait)) })
	for {
		var req wsRequest
		_, raw, err := wc.c.ReadMessage()
		if err != nil {
			return
		}
		_ = wc.c.SetReadDeadline(time.Now().Add(wsPongWait))
		if err := json.Unmarshal(raw, &req); err != nil {
			wc.fail(nil, "invalid JSON: "+err.Error())
			continue
		}
		switch req.Type {
		case "subscribe":
			filters := req.Filters
			if req.Filter != "" {
				filters = append(filters, req.Filter)
			}
			if len(filters) == 0 {
				wc.fail(req.ID, "subscribe needs filter or filters")
			}
			for _, f := range filters {
				wc.subscribe(req.ID, f, req.IncludeCurrent)
			}
		case "unsubscribe":
			filters := req.Filters
			if req.Filter != "" {
				filters = append(filters, req.Filter)
			}
			for _, f := range filters {
				wc.mu.Lock()
				cancel, ok := wc.subs[f]
				delete(wc.subs, f)
				wc.mu.Unlock()
				if ok {
					cancel()
				}
				wc.reply(req.ID, map[string]any{"type": "unsubscribed", "filter": f, "found": ok})
			}
		case "publish":
			wc.publish(req)
		case "ping":
			wc.reply(req.ID, map[string]any{"type": "pong"})
		case "auth":
			wc.fail(req.ID, "already authenticated")
		default:
			wc.fail(req.ID, `unknown type: use "subscribe", "unsubscribe", "publish" or "ping"`)
		}
	}
}

func (wc *wsConn) subscribe(id json.RawMessage, filter string, current bool) {
	if err := core.Require(wc.p, config.ScopeRead); err != nil {
		wc.fail(id, err.Error())
		return
	}
	wc.mu.Lock()
	_, exists := wc.subs[filter]
	full := len(wc.subs) >= wsMaxSubs
	wc.mu.Unlock()
	if exists {
		wc.reply(id, map[string]any{"type": "subscribed", "filter": filter, "already": true})
		return
	}
	if full {
		wc.fail(id, "too many subscriptions on this connection (max "+strconv.Itoa(wsMaxSubs)+")")
		return
	}
	cancel, err := wc.s.svc.Watch(filter, func(e *store.Entry) { wc.push(filter, e, false) })
	if err != nil {
		wc.fail(id, err.Error())
		return
	}
	wc.mu.Lock()
	wc.subs[filter] = cancel
	wc.mu.Unlock()
	wc.reply(id, map[string]any{"type": "subscribed", "filter": filter})
	if current {
		res, _, err := wc.s.svc.Query(filter, wsMaxCurrent)
		if err == nil {
			for _, e := range res {
				wc.push(filter, e, true)
			}
		}
	}
}

// push queues a message frame. Live messages run on the ingest path and
// never block (they are dropped and counted when the queue is full);
// current values wait for room.
func (wc *wsConn) push(filter string, e *store.Entry, current bool) {
	buf := make([]byte, 0, 96+len(filter)+len(e.Topic)+len(e.Payload))
	buf = append(buf, `{"type":"message","filter":`...)
	buf = store.AppendString(buf, filter)
	if current {
		buf = append(buf, `,"current":true`...)
	}
	buf = append(buf, `,"data":`...)
	buf = store.AppendJSON(buf, e)
	buf = append(buf, '}')
	if current {
		select {
		case wc.out <- buf:
		case <-wc.done:
		}
		return
	}
	select {
	case wc.out <- buf:
	default:
		wc.dropped.Add(1)
	}
}

func (wc *wsConn) publish(req wsRequest) {
	if err := core.Require(wc.p, config.ScopePublish); err != nil {
		wc.fail(req.ID, err.Error())
		return
	}
	msg, err := req.PublishRequest.Message()
	if err != nil {
		wc.fail(req.ID, err.Error())
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-wc.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	defer cancel()
	n, err := wc.s.svc.Publish(ctx, []mqttc.Message{msg})
	if err != nil {
		wc.fail(req.ID, err.Error())
		return
	}
	wc.reply(req.ID, map[string]any{"type": "published", "topic": msg.Topic, "published": n})
}

func (wc *wsConn) reply(id json.RawMessage, v map[string]any) {
	if len(id) > 0 {
		v["id"] = id
	}
	b, _ := json.Marshal(v)
	select {
	case wc.out <- b:
	case <-wc.done:
	}
}

func (wc *wsConn) fail(id json.RawMessage, msg string) {
	wc.reply(id, map[string]any{"type": "error", "error": msg})
}

// writer is the only goroutine writing to the connection.
func (wc *wsConn) writer() {
	ping := time.NewTicker(wsPingInterval)
	defer ping.Stop()
	write := func(b []byte) bool {
		_ = wc.c.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
		return wc.c.WriteMessage(websocket.TextMessage, b) == nil
	}
	for {
		var b []byte
		select {
		case b = <-wc.out:
		case <-ping.C:
			if err := wc.c.WriteControl(websocket.PingMessage, nil, time.Now().Add(wsWriteTimeout)); err != nil {
				wc.c.Close()
				return
			}
			continue
		case <-wc.done:
			return
		}
		if !write(b) {
			wc.c.Close()
			return
		}
		if n := wc.dropped.Swap(0); n > 0 {
			if !write([]byte(`{"type":"dropped","count":` + strconv.FormatUint(n, 10) + `}`)) {
				wc.c.Close()
				return
			}
		}
	}
}
