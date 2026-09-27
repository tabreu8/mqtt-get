package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/mcp"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
)

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func startBroker(t *testing.T) (*mochi.Server, string) {
	t.Helper()
	b := mochi.New(&mochi.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	err := b.AddHook(new(auth.Hook), &auth.Options{Ledger: &auth.Ledger{
		Auth: auth.AuthRules{{Username: "user", Password: "secret", Allow: true}},
		ACL:  auth.ACLRules{{Username: "user", Filters: auth.Filters{"#": auth.ReadWrite}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr := freeAddr(t)
	if err := b.AddListener(listeners.NewTCP(listeners.Config{ID: "tcp", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go func() { _ = b.Serve() }()
	t.Cleanup(func() { _ = b.Close() })
	return b, addr
}

type env struct {
	t      *testing.T
	svc    *core.Service
	st     *state.State
	http   *httptest.Server
	broker *mochi.Server
	admin  string
}

func setup(t *testing.T, password string) *env {
	t.Helper()
	broker, addr := startBroker(t)
	cfg := config.Server{MaxTopics: 1000, MaxBodyBytes: 1 << 20, UIEnabled: true, MCPEnabled: true, PublishTimeout: 5 * time.Second}
	st, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st.SetEnvKeys([]string{"admin-key"}, []string{"read-key"}, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := core.New(cfg, log, st)
	b := config.Broker{URLs: []string{"tcp://" + addr}, Username: "user", Password: password, ClientID: "test"}
	if err := svc.Start(b, true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	// Wire it exactly like main: REST + MCP over the same core.
	ms := mcp.New(svc, log, mcp.Options{})
	t.Cleanup(ms.Close)
	mh := ms.HTTPHandler(func(r *http.Request) (*core.Principal, error) { return svc.Authenticate(APIKey(r, false)) }, 0)
	hs := httptest.NewServer(New(svc, mh).Handler())
	t.Cleanup(hs.Close)
	return &env{t: t, svc: svc, st: st, http: hs, broker: broker, admin: "admin-key"}
}

func (e *env) waitConnected() {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !e.svc.MQTTStatus().Connected {
		if time.Now().After(deadline) {
			e.t.Fatalf("not connected: %+v", e.svc.MQTTStatus())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // let the subscription settle
}

func (e *env) do(method, path, key, body string) (int, string, http.Header) {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.http.URL+path, strings.NewReader(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestEndToEnd(t *testing.T) {
	e := setup(t, "secret")
	e.waitConnected()

	// Auth.
	if code, _, _ := e.do("GET", "/api/v1/status", "", ""); code != 401 {
		t.Fatalf("no key: %d", code)
	}
	if code, _, _ := e.do("POST", "/api/v1/publish/a", "read-key", "x"); code != 403 {
		t.Fatalf("read key publish: %d", code)
	}

	// Message from another MQTT client lands in the store.
	_ = e.broker.Publish("sensors/kitchen/temp", []byte(`{"c":21.5}`), false, 0)
	eventually(t, func() bool { _, ok := e.svc.Store().Get("sensors/kitchen/temp"); return ok })
	code, body, hdr := e.do("GET", "/api/v1/values/sensors/kitchen/temp", "read-key", "")
	if code != 200 || !strings.Contains(body, `"payload":{"c":21.5},"encoding":"json"`) {
		t.Fatalf("get: %d %s", code, body)
	}
	if hdr.Get("X-MQTT-Topic") != "sensors/kitchen/temp" {
		t.Fatal("missing topic header")
	}
	code, body, _ = e.do("GET", "/api/v1/values/sensors/kitchen/temp?format=raw", "read-key", "")
	if code != 200 || body != `{"c":21.5}` {
		t.Fatalf("raw: %d %s", code, body)
	}
	if code, _, _ := e.do("GET", "/api/v1/values/nope", "read-key", ""); code != 404 {
		t.Fatalf("missing topic: %d", code)
	}

	// Publish over HTTP (raw and JSON batch) and read back.
	if code, body, _ := e.do("POST", "/api/v1/publish/cmd/light?qos=1&retain=true", e.admin, "on"); code != 200 {
		t.Fatalf("publish raw: %d %s", code, body)
	}
	batch := `[{"topic":"cmd/a","payload":"hello"},{"topic":"cmd/b","payload":{"x":1},"qos":1}]`
	if code, body, _ := e.do("POST", "/api/v1/publish", e.admin, batch); code != 200 || !strings.Contains(body, `"published":2`) {
		t.Fatalf("publish json: %d %s", code, body)
	}
	eventually(t, func() bool { _, ok := e.svc.Store().Get("cmd/b"); return ok })
	if v, _ := e.svc.Store().Get("cmd/light"); v == nil || string(v.Payload) != "on" {
		t.Fatalf("cmd/light = %+v", v)
	}
	code, body, _ = e.do("GET", "/api/v1/values?filter=cmd/%2B", "read-key", "")
	if code != 200 || !strings.Contains(body, `"total":3`) {
		t.Fatalf("query: %d %s", code, body)
	}

	// Webhook, batched.
	var mu sync.Mutex
	var got []string
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, string(b))
		mu.Unlock()
		if r.Header.Get("X-MQTT-Get-Signature") == "" {
			w.WriteHeader(400)
		}
	}))
	defer hook.Close()
	wh := `{"url":"` + hook.URL + `","topics":["events/#"],"secret":"s3cr3t","batch_size":10,"batch_interval_ms":50}`
	code, body, _ = e.do("POST", "/api/v1/webhooks", e.admin, wh)
	if code != 201 || !strings.Contains(body, `"secret":"********"`) {
		t.Fatalf("create webhook: %d %s", code, body)
	}
	for i := 0; i < 5; i++ {
		_ = e.broker.Publish("events/x", []byte("e"), false, 0)
	}
	_ = e.broker.Publish("other/x", []byte("ignored"), false, 0)
	var delivered uint64
	eventually(t, func() bool {
		for _, st := range e.svc.WebhookStats() {
			delivered = st.Delivered
		}
		return delivered == 5
	})
	mu.Lock()
	joined := strings.Join(got, "")
	mu.Unlock()
	if !strings.Contains(joined, `{"messages":[`) || strings.Contains(joined, "ignored") {
		t.Fatalf("webhook bodies: %s", joined)
	}

	// Metrics.
	if code, body, _ := e.do("GET", "/metrics", "read-key", ""); code != 200 || !strings.Contains(body, "mqttget_webhook_delivered_total") {
		t.Fatalf("metrics: %d %s", code, body)
	}

	// API keys.
	code, body, _ = e.do("POST", "/api/v1/keys", e.admin, `{"name":"svc","scopes":["publish"]}`)
	if code != 201 {
		t.Fatalf("create key: %d %s", code, body)
	}
	var created struct{ Key string }
	_ = json.Unmarshal([]byte(body), &created)
	if code, _, _ := e.do("POST", "/api/v1/publish/k", created.Key, "1"); code != 200 {
		t.Fatalf("publish with new key: %d", code)
	}
	if code, _, _ := e.do("GET", "/api/v1/webhooks", created.Key, ""); code != 403 {
		t.Fatalf("publish key on admin endpoint: %d", code)
	}
}

func TestMCP(t *testing.T) {
	e := setup(t, "secret")
	e.waitConnected()
	_ = e.broker.Publish("room/1", []byte("22"), false, 0)
	eventually(t, func() bool { _, ok := e.svc.Store().Get("room/1"); return ok })

	rpc := func(key, body string) map[string]any {
		code, out, _ := e.do("POST", "/mcp", key, body)
		if code != 200 {
			t.Fatalf("mcp %s: %d %s", body, code, out)
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(out), &m); err != nil {
			t.Fatalf("%s: %v", out, err)
		}
		return m
	}
	init := rpc(e.admin, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}`)
	if init["result"].(map[string]any)["protocolVersion"] != "2025-03-26" {
		t.Fatalf("initialize: %v", init)
	}
	if code, _, _ := e.do("POST", "/mcp", e.admin, `{"jsonrpc":"2.0","method":"notifications/initialized"}`); code != 202 {
		t.Fatalf("notification: %d", code)
	}
	list := rpc("read-key", `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	tools := list["result"].(map[string]any)["tools"].([]any)
	for _, tl := range tools {
		if tl.(map[string]any)["name"] == "configure_broker" {
			t.Fatal("read key must not see admin tools")
		}
	}
	res := rpc("read-key", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_value","arguments":{"topic":"room/1"}}}`)
	text := res["result"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, `"payload":22`) {
		t.Fatalf("get_latest_value: %s", text)
	}
	res = rpc(e.admin, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"publish","arguments":{"topic":"room/2","payload":"hi"}}}`)
	if res["result"].(map[string]any)["isError"] == true {
		t.Fatalf("publish: %v", res)
	}
	eventually(t, func() bool { _, ok := e.svc.Store().Get("room/2"); return ok })
}

func TestBrokerConfigViaAPI(t *testing.T) {
	e := setup(t, "wrong-password")
	time.Sleep(300 * time.Millisecond)
	if e.svc.MQTTStatus().Connected {
		t.Fatal("should not connect with wrong password")
	}
	code, body, _ := e.do("GET", "/api/v1/broker", e.admin, "")
	if code != 200 || !strings.Contains(body, `"password":"********"`) {
		t.Fatalf("get broker: %d %s", code, body)
	}
	var view struct{ Config config.Broker }
	_ = json.Unmarshal([]byte(body), &view)
	view.Config.Password = "secret"
	b, _ := json.Marshal(view.Config)
	if code, body, _ := e.do("PUT", "/api/v1/broker", e.admin, string(b)); code != 200 {
		t.Fatalf("put broker: %d %s", code, body)
	}
	e.waitConnected()
	// Saved config is persisted and redacted secrets are preserved on re-save.
	view.Config.Password = config.Redacted
	b, _ = json.Marshal(view.Config)
	if code, _, _ := e.do("PUT", "/api/v1/broker", e.admin, string(b)); code != 200 {
		t.Fatal("re-put")
	}
	if saved, ok := e.st.Broker(); !ok || saved.Password != "secret" {
		t.Fatalf("persisted: %+v", saved)
	}
	e.waitConnected()
}

func BenchmarkGetValueHTTP(b *testing.B) {
	st, _ := state.Open("")
	st.SetEnvKeys([]string{"k"}, nil, nil)
	svc := core.New(config.Server{MaxBodyBytes: 1 << 20}, slog.New(slog.NewTextHandler(io.Discard, nil)), st)
	svc.Ingest(*storeEntry("a/b", `{"v":1}`))
	h := New(svc, nil).Handler()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		req := httptest.NewRequest("GET", "/api/v1/values/a/b", nil)
		req.Header.Set("Authorization", "Bearer k")
		for pb.Next() {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != 200 {
				b.Fatal(w.Code)
			}
		}
	})
}

func storeEntry(topic, payload string) *store.Entry {
	return &store.Entry{Topic: topic, Payload: []byte(payload), Time: time.Now().UnixNano()}
}

func BenchmarkIngest(b *testing.B) {
	st, _ := state.Open("")
	svc := core.New(config.Server{}, slog.New(slog.NewTextHandler(io.Discard, nil)), st)
	if _, err := svc.PutWebhook(config.Webhook{ID: "w", URL: "http://127.0.0.1:1", Topics: []string{"other/#"}}, true); err != nil {
		b.Fatal(err)
	}
	topics := make([]string, 10000)
	for i := range topics {
		topics[i] = "devices/" + strconv.Itoa(i) + "/state"
	}
	p := []byte(`{"on":true}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		svc.Ingest(store.Entry{Topic: topics[i%len(topics)], Payload: p, Time: int64(i)})
	}
}
