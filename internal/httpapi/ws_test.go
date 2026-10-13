package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
	spb "github.com/tabreu8/mqtt-get/sparkplug"
)

// wsEnv is a server without a broker: messages are injected with Ingest.
type wsEnv struct {
	svc *core.Service
	srv *httptest.Server
}

func newWSEnv(t *testing.T, cfg config.Server) *wsEnv {
	t.Helper()
	st, _ := state.Open("")
	st.SetEnvKeys([]string{"admin-key"}, []string{"read-key"}, nil)
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 1 << 20
	}
	cfg.MaxTopics = 100000
	cfg.PublishTimeout = time.Second
	svc := core.New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), st)
	t.Cleanup(svc.Close)
	srv := httptest.NewServer(New(svc, nil).Handler())
	t.Cleanup(srv.Close)
	return &wsEnv{svc: svc, srv: srv}
}

func (e *wsEnv) dial(t *testing.T, path string, hdr http.Header) *websocket.Conn {
	t.Helper()
	c, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(e.srv.URL, "http")+path, hdr)
	if err != nil {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("dial %s: %v (HTTP %d)", path, err, code)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func (e *wsEnv) ingest(topic, payload string) {
	e.svc.Ingest(store.Entry{Topic: topic, Payload: []byte(payload), Time: time.Now().UnixNano()})
}

var readerHdr = http.Header{"Authorization": {"Bearer read-key"}}

func TestWebSocketSlowConsumerDrops(t *testing.T) {
	old := wsQueue
	wsQueue = 4
	defer func() { wsQueue = old }()
	e := newWSEnv(t, config.Server{})
	c := e.dial(t, "/api/v1/ws?filter=big/%23", readerHdr)
	if m := wsRead(t, c); m["type"] != "subscribed" {
		t.Fatal(m)
	}
	// Don't read while a burst far larger than the socket buffers arrives:
	// ingest must never block, and the client is told what it missed.
	big := strings.Repeat("x", 64<<10)
	start := time.Now()
	for i := 0; i < 500; i++ {
		e.ingest("big/"+strconv.Itoa(i), big)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("ingest blocked by a slow WebSocket client: %v", d)
	}
	var got, dropped int
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	for got+dropped < 500 {
		var m map[string]any
		if err := c.ReadJSON(&m); err != nil {
			t.Fatalf("after %d messages and %d dropped: %v", got, dropped, err)
		}
		switch m["type"] {
		case "message":
			got++
		case "dropped":
			dropped += int(m["count"].(float64))
		}
	}
	if dropped == 0 || got == 0 || got+dropped != 500 {
		t.Fatalf("received %d, dropped %d", got, dropped)
	}
}

func TestWebSocketProtocolErrorsAndLimits(t *testing.T) {
	oldSubs, oldAuth := wsMaxSubs, wsAuthTimeout
	wsMaxSubs, wsAuthTimeout = 3, 200*time.Millisecond
	defer func() { wsMaxSubs, wsAuthTimeout = oldSubs, oldAuth }()
	e := newWSEnv(t, config.Server{})

	c := e.dial(t, "/api/v1/ws", readerHdr)
	for _, raw := range []string{`not json`, `{"type":"dance","id":1}`, `{"type":"subscribe"}`, `{"type":"subscribe","filter":"a/#/b"}`, `{"type":"auth","key":"x"}`} {
		_ = c.WriteMessage(websocket.TextMessage, []byte(raw))
		if m := wsUntil(t, c, "error"); m["error"] == "" {
			t.Fatalf("%s: %v", raw, m)
		}
	}
	for i := 0; i < 3; i++ {
		_ = c.WriteJSON(map[string]any{"type": "subscribe", "filter": "f/" + strconv.Itoa(i)})
		wsUntil(t, c, "subscribed")
	}
	_ = c.WriteJSON(map[string]any{"type": "subscribe", "filter": "f/9", "id": "x"})
	if m := wsUntil(t, c, "error"); !strings.Contains(m["error"].(string), "too many subscriptions") || m["id"] != "x" {
		t.Fatalf("subscription limit: %v", m)
	}
	// Unsubscribing stops delivery.
	_ = c.WriteJSON(map[string]any{"type": "unsubscribe", "filter": "f/0"})
	wsUntil(t, c, "unsubscribed")
	e.ingest("f/0", "gone")
	e.ingest("f/1", "kept")
	if m := wsUntil(t, c, "message"); m["data"].(map[string]any)["topic"] != "f/1" {
		t.Fatalf("message after unsubscribe: %v", m)
	}

	// No auth message in time: closed with 4401.
	b := e.dial(t, "/api/v1/ws", nil)
	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := b.ReadMessage(); !websocket.IsCloseError(err, 4401) {
		t.Fatalf("auth timeout: %v", err)
	}
	// A wrong key in the auth message: closed with 4401 too.
	b = e.dial(t, "/api/v1/ws", nil)
	_ = b.WriteJSON(map[string]any{"type": "auth", "key": "wrong"})
	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := b.ReadMessage(); !websocket.IsCloseError(err, 4401) {
		t.Fatalf("bad auth key: %v", err)
	}
}

func TestWebSocketOrderingAndManyClients(t *testing.T) {
	e := newWSEnv(t, config.Server{})
	const clients, msgs = 20, 500
	conns := make([]*websocket.Conn, clients)
	for i := range conns {
		conns[i] = e.dial(t, "/api/v1/ws?filter=seq", readerHdr)
		wsUntil(t, conns[i], "subscribed")
	}
	var wg sync.WaitGroup
	errs := make(chan string, clients)
	for i, c := range conns {
		wg.Add(1)
		go func(i int, c *websocket.Conn) {
			defer wg.Done()
			_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
			for want := 0; want < msgs; want++ {
				var m struct {
					Type string
					Data struct{ Payload json.Number }
				}
				if err := c.ReadJSON(&m); err != nil {
					errs <- "client " + strconv.Itoa(i) + ": " + err.Error()
					return
				}
				if m.Type != "message" || m.Data.Payload.String() != strconv.Itoa(want) {
					errs <- "client " + strconv.Itoa(i) + ": got " + m.Type + " " + m.Data.Payload.String() + ", want " + strconv.Itoa(want)
					return
				}
			}
		}(i, c)
	}
	for i := 0; i < msgs; i++ {
		e.ingest("seq", strconv.Itoa(i))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestWebSocketCurrentValuesCap(t *testing.T) {
	old := wsMaxCurrent
	wsMaxCurrent = 50
	defer func() { wsMaxCurrent = old }()
	e := newWSEnv(t, config.Server{})
	for i := 0; i < 80; i++ {
		e.ingest("cur/"+strconv.Itoa(i), "1")
	}
	c := e.dial(t, "/api/v1/ws", readerHdr)
	_ = c.WriteJSON(map[string]any{"type": "subscribe", "filter": "cur/#", "include_current": true})
	wsUntil(t, c, "subscribed")
	n := 0
	_ = c.WriteJSON(map[string]any{"type": "ping"})
	for {
		m := wsRead(t, c)
		if m["type"] == "pong" {
			break
		}
		if m["current"] == true {
			n++
		}
	}
	if n != 50 {
		t.Fatalf("current values: %d, want 50", n)
	}
}

func TestWebSocketAuthDisabledAndOrigins(t *testing.T) {
	e := newWSEnv(t, config.Server{AuthDisabled: true, CORSOrigins: "https://dash.example"})
	c := e.dial(t, "/api/v1/ws?filter=x", http.Header{"Origin": {"https://dash.example"}})
	if m := wsRead(t, c); m["type"] != "subscribed" {
		t.Fatalf("auth disabled: %v", m)
	}
	_, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(e.srv.URL, "http")+"/api/v1/ws", http.Header{"Origin": {"https://other.example"}})
	if err == nil || resp.StatusCode != 403 {
		t.Fatalf("origin not in CORS_ORIGINS: %v", err)
	}
}

func TestWebSocketSparkplugWrite(t *testing.T) {
	e := newWSEnv(t, config.Server{})
	if _, err := e.svc.SetSparkplug(config.Sparkplug{Enabled: true, Commands: true}); err != nil {
		t.Fatal(err)
	}
	e.svc.Ingest(store.Entry{Topic: "spBv1.0/g/DBIRTH/e/d", Time: time.Now().UnixNano(), Payload: spb.Encode(&spb.Payload{
		Metrics: []spb.Metric{{Name: "Speed", Datatype: spb.TypeInt16, Value: spb.Value{Kind: spb.KindInt}}}})})
	c := e.dial(t, "/api/v1/ws", http.Header{"Authorization": {"Bearer admin-key"}})
	// Validation runs before the (here unavailable) broker is needed.
	_ = c.WriteJSON(map[string]any{"type": "publish", "topic": "spb/g/e/d/Speed", "payload": 99999})
	if m := wsUntil(t, c, "error"); !strings.Contains(m["error"].(string), "out of range") {
		t.Fatalf("invalid Sparkplug write: %v", m)
	}
	_ = c.WriteJSON(map[string]any{"type": "publish", "topic": "spb/g/e/d/Speed", "payload": 12})
	if m := wsUntil(t, c, "error"); !strings.Contains(m["error"].(string), "not connected") {
		t.Fatalf("valid write without a broker: %v", m)
	}
}
