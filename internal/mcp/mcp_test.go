package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// --- fixtures -----------------------------------------------------------

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

var (
	admin   = &core.Principal{ID: "a", Name: "admin", Scopes: []string{config.ScopeAdmin}}
	reader  = &core.Principal{ID: "r", Name: "reader", Scopes: []string{config.ScopeRead}}
	operatr = &core.Principal{ID: "p", Name: "operator", Scopes: []string{config.ScopeRead, config.ScopePublish}}
)

// newService returns a core service; with a broker it is connected to an
// embedded MQTT broker (returned) whose inline client can play a device.
func newService(t *testing.T, withBroker bool) (*core.Service, *mochi.Server) {
	t.Helper()
	st, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st.SetEnvKeys([]string{"admin-key"}, []string{"read-key"}, []string{"pub-key"})
	var b *mochi.Server
	var addr string
	if withBroker {
		// Created (and its cleanup registered) before the service, so the
		// service disconnects first: cleanups run in reverse order, and
		// mochi's Close can block on a client that is still connecting.
		b = mochi.New(&mochi.Options{InlineClient: true, Logger: quiet()})
		_ = b.AddHook(new(auth.AllowHook), nil)
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		addr = l.Addr().String()
		l.Close()
		if err := b.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: addr})); err != nil {
			t.Fatal(err)
		}
		go func() { _ = b.Serve() }()
		t.Cleanup(func() { _ = b.Close() })
	}
	svc := core.New(config.Server{MaxTopics: 100000, MaxBodyBytes: 1 << 20, PublishTimeout: 5 * time.Second}, quiet(), st)
	t.Cleanup(svc.Close)
	if !withBroker {
		return svc, nil
	}
	if err := svc.Start(config.Broker{URLs: []string{"tcp://" + addr}, ClientID: "mcp-test", Password: "pw", Username: "u"}, true); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "broker connection", func() bool { return svc.MQTTStatus().Connected })
	time.Sleep(150 * time.Millisecond)
	return svc, b
}

func waitUntil(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func ingest(svc *core.Service, topic, payload string) {
	svc.Ingest(&store.Entry{Topic: topic, Payload: []byte(payload), Time: time.Now().UnixNano()})
}

// stdioClient drives ServeStdio through pipes, like a real local agent.
type stdioClient struct {
	t      *testing.T
	w      io.WriteCloser
	nextID atomic.Int64
	mu     sync.Mutex
	resps  map[string]chan map[string]any
	notes  chan map[string]any
	done   chan struct{}
}

func newStdio(t *testing.T, s *Server, p *core.Principal) *stdioClient {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &stdioClient{t: t, w: inW, resps: map[string]chan map[string]any{}, notes: make(chan map[string]any, 1000), done: make(chan struct{})}
	go func() {
		_ = s.ServeStdio(context.Background(), inR, outW, p)
		outW.Close()
		close(c.done)
	}()
	go func() {
		sc := bufio.NewScanner(outR)
		sc.Buffer(make([]byte, 1<<20), 16<<20)
		for sc.Scan() {
			var m map[string]any
			if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
				t.Errorf("server wrote invalid JSON: %s", sc.Text())
				continue
			}
			if id, ok := m["id"]; ok {
				c.mu.Lock()
				ch := c.resps[fmt.Sprint(id)]
				c.mu.Unlock()
				if ch != nil {
					ch <- m
				}
				continue
			}
			c.notes <- m
		}
	}()
	t.Cleanup(func() { inW.Close() })
	return c
}

func (c *stdioClient) send(v any) {
	b, _ := json.Marshal(v)
	if _, err := c.w.Write(append(b, '\n')); err != nil {
		c.t.Fatal(err)
	}
}

// start sends a request and returns its id and response channel.
func (c *stdioClient) start(method string, params any) (int64, chan map[string]any) {
	id := c.nextID.Add(1)
	ch := make(chan map[string]any, 1)
	c.mu.Lock()
	c.resps[fmt.Sprint(float64(id))] = ch
	c.mu.Unlock()
	c.send(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	return id, ch
}

func (c *stdioClient) call(method string, params any) map[string]any {
	c.t.Helper()
	_, ch := c.start(method, params)
	select {
	case m := <-ch:
		return m
	case <-time.After(10 * time.Second):
		c.t.Fatalf("no response to %s", method)
		return nil
	}
}

func result(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	if e, ok := m["error"]; ok {
		t.Fatalf("unexpected JSON-RPC error: %v", e)
	}
	return m["result"].(map[string]any)
}

// tool calls a tool and returns (structured result, isError, text).
func (c *stdioClient) tool(name string, args any) (map[string]any, bool, string) {
	c.t.Helper()
	r := result(c.t, c.call("tools/call", map[string]any{"name": name, "arguments": args}))
	text := r["content"].([]any)[0].(map[string]any)["text"].(string)
	isErr, _ := r["isError"].(bool)
	sc, _ := r["structuredContent"].(map[string]any)
	return sc, isErr, text
}

func (c *stdioClient) mustTool(name string, args any) map[string]any {
	c.t.Helper()
	sc, isErr, text := c.tool(name, args)
	if isErr {
		c.t.Fatalf("%s failed: %s", name, text)
	}
	return sc
}

func (c *stdioClient) initialize(version string) map[string]any {
	r := result(c.t, c.call("initialize", map[string]any{
		"protocolVersion": version, "capabilities": map[string]any{},
		"clientInfo": map[string]any{"name": "test", "version": "1"}}))
	c.send(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	return r
}

// --- tests --------------------------------------------------------------

func TestInitialize(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	c := newStdio(t, s, reader)
	r := c.initialize("2025-06-18")
	if r["protocolVersion"] != "2025-06-18" {
		t.Fatalf("version %v", r["protocolVersion"])
	}
	caps := r["capabilities"].(map[string]any)
	for _, k := range []string{"tools", "resources", "prompts", "completions"} {
		if _, ok := caps[k]; !ok {
			t.Errorf("missing capability %s", k)
		}
	}
	if caps["resources"].(map[string]any)["subscribe"] != true {
		t.Error("stdio sessions must support subscriptions")
	}
	if !strings.Contains(r["instructions"].(string), "describe_topic_tree") {
		t.Error("instructions should guide agents")
	}
	// Unknown version -> server proposes its latest.
	c2 := newStdio(t, s, reader)
	if v := c2.initialize("1999-01-01")["protocolVersion"]; v != ProtocolVersions[0] {
		t.Fatalf("fallback version %v", v)
	}
	if m := c.call("nope/nope", nil); m["error"].(map[string]any)["code"].(float64) != codeMethodNotFound {
		t.Fatal("unknown method must be -32601")
	}
	if r := result(t, c.call("ping", nil)); len(r) != 0 {
		t.Fatal("ping")
	}
}

func toolNames(t *testing.T, c *stdioClient) map[string]map[string]any {
	res := result(t, c.call("tools/list", nil))
	out := map[string]map[string]any{}
	for _, x := range res["tools"].([]any) {
		m := x.(map[string]any)
		out[m["name"].(string)] = m
	}
	return out
}

func TestToolsPerScope(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	counts := map[string]int{}
	for name, p := range map[string]*core.Principal{"read": reader, "publish": operatr, "admin": admin} {
		c := newStdio(t, s, p)
		c.initialize("2025-06-18")
		tl := toolNames(t, c)
		counts[name] = len(tl)
		for n, def := range tl {
			schema := def["inputSchema"].(map[string]any)
			if schema["type"] != "object" || schema["additionalProperties"] != false {
				t.Errorf("%s: bad schema %v", n, schema)
			}
			ann := def["annotations"].(map[string]any)
			if _, ok := ann["readOnlyHint"]; !ok {
				t.Errorf("%s: missing annotations", n)
			}
			if def["description"] == "" || def["title"] == "" {
				t.Errorf("%s: missing description/title", n)
			}
		}
		if _, ok := tl["publish"]; ok != (name != "read") {
			t.Errorf("%s key: publish visible=%v", name, ok)
		}
		if _, ok := tl["configure_broker"]; ok != (name == "admin") {
			t.Errorf("%s key: configure_broker visible=%v", name, ok)
		}
		if name != "read" && tl["publish"]["annotations"].(map[string]any)["destructiveHint"] != true {
			t.Error("publish must be flagged destructive (it can actuate devices)")
		}
	}
	if !(counts["read"] < counts["publish"] && counts["publish"] < counts["admin"]) || counts["admin"] != len(tools) {
		t.Fatalf("tool counts %v (total %d)", counts, len(tools))
	}
	// Hidden tools are also enforced, not just hidden.
	c := newStdio(t, s, reader)
	c.initialize("2025-06-18")
	if _, isErr, text := c.tool("publish", map[string]any{"topic": "x", "payload": "y"}); !isErr || !strings.Contains(text, "publish scope") {
		t.Fatalf("read key publishing: %v %s", isErr, text)
	}
	if m := c.call("tools/call", map[string]any{"name": "no_such_tool"}); m["error"] == nil {
		t.Fatal("unknown tool must be a protocol error")
	}
}

func TestReadTools(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	ingest(svc, "home/kitchen/temperature", `{"celsius":21.5}`)
	ingest(svc, "home/kitchen/humidity", `44`)
	ingest(svc, "home/lamp/state", `ON`)
	ingest(svc, "office/printer/status", "\x00\xff")
	c := newStdio(t, s, reader)
	c.initialize("2025-06-18")

	v := c.mustTool("get_value", map[string]any{"topic": "home/kitchen/temperature"})
	if v["encoding"] != "json" || v["payload"].(map[string]any)["celsius"] != 21.5 || v["age_seconds"] == nil {
		t.Fatalf("get_value: %v", v)
	}
	if v := c.mustTool("get_value", map[string]any{"topic": "office/printer/status"}); v["encoding"] != "base64" {
		t.Fatalf("binary: %v", v)
	}
	// A near-miss gets suggestions an agent can act on.
	_, isErr, text := c.tool("get_value", map[string]any{"topic": "home/kitchen/temp"})
	if !isErr || !strings.Contains(text, "home/kitchen/temperature") {
		t.Fatalf("suggestions: %s", text)
	}
	if _, isErr, text := c.tool("get_value", map[string]any{"topic": "home/+/state"}); !isErr || !strings.Contains(text, "get_values") {
		t.Fatalf("wildcard: %s", text)
	}
	time.Sleep(30 * time.Millisecond)
	if _, isErr, text := c.tool("get_value", map[string]any{"topic": "home/lamp/state", "max_age_seconds": 0.001}); !isErr || !strings.Contains(text, "older") {
		t.Fatalf("max_age: %s", text)
	}
	// Invented argument names are rejected with a clear message.
	if _, isErr, text := c.tool("get_value", map[string]any{"topik": "x"}); !isErr || !strings.Contains(text, "topik") {
		t.Fatalf("unknown arg: %s", text)
	}

	vals := c.mustTool("get_values", map[string]any{"filter": "home/#"})
	if vals["total"].(float64) != 3 {
		t.Fatalf("get_values filter: %v", vals)
	}
	vals = c.mustTool("get_values", map[string]any{"topics": []string{"home/lamp/state", "nope"}})
	if len(vals["values"].([]any)) != 1 || vals["missing"].([]any)[0] != "nope" {
		t.Fatalf("get_values topics: %v", vals)
	}
	if _, isErr, _ := c.tool("get_values", map[string]any{}); !isErr {
		t.Fatal("get_values needs topics or filter")
	}

	lt := c.mustTool("list_topics", map[string]any{"filter": "home/+/+"})
	if lt["total"].(float64) != 3 {
		t.Fatalf("list_topics: %v", lt)
	}

	tree := c.mustTool("describe_topic_tree", map[string]any{})
	if tree["topics"].(float64) != 4 || len(tree["children"].([]any)) != 2 {
		t.Fatalf("tree: %v", tree)
	}
	home := c.mustTool("describe_topic_tree", map[string]any{"prefix": "home", "depth": 1})
	var names []string
	for _, ch := range home["children"].([]any) {
		m := ch.(map[string]any)
		names = append(names, fmt.Sprintf("%s:%v", m["name"], m["topics"]))
	}
	if strings.Join(names, ",") != "kitchen:2,lamp:1" {
		t.Fatalf("subtree: %v", names)
	}

	st := c.mustTool("get_status", nil)
	if st["topics"].(float64) != 4 {
		t.Fatalf("status: %v", st)
	}
}

func TestLargePayloadIsTruncated(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	big := `{"data":"` + strings.Repeat("x", 40000) + `"}`
	ingest(svc, "big", big)
	c := newStdio(t, s, reader)
	c.initialize("2025-06-18")
	v := c.mustTool("get_value", map[string]any{"topic": "big"})
	if v["truncated"] != true || v["payload_bytes"].(float64) != float64(len(big)) || len(v["payload"].(string)) != maxPayloadBytes {
		t.Fatalf("truncation: truncated=%v bytes=%v len=%d", v["truncated"], v["payload_bytes"], len(v["payload"].(string)))
	}
}

func TestWaitForMessage(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	c := newStdio(t, s, reader)
	c.initialize("2025-06-18")

	// Nothing arrives: returns received=false after the timeout.
	start := time.Now()
	r := c.mustTool("wait_for_message", map[string]any{"filter": "alarms/#", "timeout_seconds": 0.2})
	if r["received"] != false || time.Since(start) > 2*time.Second {
		t.Fatalf("timeout: %v", r)
	}
	// A message arrives while waiting; other requests are not blocked.
	_, ch := c.start("tools/call", map[string]any{"name": "wait_for_message", "arguments": map[string]any{"filter": "alarms/#", "timeout_seconds": 10}})
	waitUntil(t, "watcher registered", func() bool { return svc.Status().Watchers == 1 })
	if r := result(t, c.call("ping", nil)); r == nil {
		t.Fatal("ping blocked by a pending wait")
	}
	ingest(svc, "other/x", "ignored")
	ingest(svc, "alarms/press", `{"level":"high"}`)
	select {
	case m := <-ch:
		sc := result(t, m)["structuredContent"].(map[string]any)
		if sc["received"] != true || sc["message"].(map[string]any)["topic"] != "alarms/press" {
			t.Fatalf("wait result: %v", sc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait did not return")
	}
	waitUntil(t, "watcher cleanup", func() bool { return svc.Status().Watchers == 0 })
	// include_current returns the newest stored value at once.
	ingest(svc, "alarms/door", "open")
	r = c.mustTool("wait_for_message", map[string]any{"filter": "alarms/+", "include_current": true, "timeout_seconds": 5})
	if r["message"].(map[string]any)["topic"] != "alarms/door" {
		t.Fatalf("include_current: %v", r)
	}
}

func TestCancellation(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	c := newStdio(t, s, reader)
	c.initialize("2025-06-18")
	id, ch := c.start("tools/call", map[string]any{"name": "wait_for_message", "arguments": map[string]any{"filter": "#", "timeout_seconds": 60}})
	waitUntil(t, "watcher", func() bool { return svc.Status().Watchers == 1 })
	c.send(map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": map[string]any{"requestId": id, "reason": "user"}})
	waitUntil(t, "cancelled wait to release its watcher", func() bool { return svc.Status().Watchers == 0 })
	select {
	case m := <-ch:
		t.Fatalf("a cancelled request must not get a response: %v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestPublishAndDeviceRoundTrip(t *testing.T) {
	svc, broker := newService(t, true)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	// A simulated lamp: on "home/lamp/set" it reports its new state.
	err := broker.Subscribe("home/lamp/set", 1, func(_ *mochi.Client, _ packets.Subscription, pk packets.Packet) {
		var cmd struct{ State string }
		_ = json.Unmarshal(pk.Payload, &cmd)
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = broker.Publish("home/lamp/state", []byte(`{"state":"`+cmd.State+`"}`), true, 0)
		}()
	})
	if err != nil {
		t.Fatal(err)
	}
	c := newStdio(t, s, operatr)
	c.initialize("2025-06-18")

	r := c.mustTool("publish_and_wait", map[string]any{
		"topic": "home/lamp/set", "payload": map[string]any{"state": "ON"}, "qos": 1,
		"response_filter": "home/lamp/+", "timeout_seconds": 5})
	resp := r["response"].(map[string]any)
	if r["response_received"] != true || resp["topic"] != "home/lamp/state" || resp["payload"].(map[string]any)["state"] != "ON" {
		t.Fatalf("publish_and_wait: %v", r)
	}
	// Plain publish; the value comes back through the subscription.
	c.mustTool("publish", map[string]any{"topic": "notes/1", "payload": "hello", "retain": true})
	waitUntil(t, "published value ingested", func() bool { e, err := svc.Latest("notes/1"); return err == nil && string(e.Payload) == "hello" })
	// Timeout path reports clearly instead of failing.
	r = c.mustTool("publish_and_wait", map[string]any{"topic": "dead/set", "payload": 1, "response_filter": "dead/state", "timeout_seconds": 0.3})
	if r["response_received"] != false || r["hint"] == nil {
		t.Fatalf("no response: %v", r)
	}
	if _, isErr, text := c.tool("publish", map[string]any{"topic": "x"}); !isErr || !strings.Contains(text, "payload") {
		t.Fatalf("missing payload: %s", text)
	}
}

func TestAdminTools(t *testing.T) {
	svc, _ := newService(t, true)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	c := newStdio(t, s, admin)
	c.initialize("2025-06-18")

	// Partial update keeps everything else (URL, credentials).
	b := c.mustTool("configure_broker", map[string]any{"subscriptions": []any{map[string]any{"filter": "home/#", "qos": 1}}})
	cfg := b["config"].(map[string]any)
	if len(cfg["urls"].([]any)) != 1 || cfg["password"] != config.Redacted || cfg["subscriptions"].([]any)[0].(map[string]any)["filter"] != "home/#" {
		t.Fatalf("configure_broker: %v", cfg)
	}
	waitUntil(t, "reconnect", func() bool { return svc.MQTTStatus().Connected })
	if _, isErr, text := c.tool("configure_broker", map[string]any{"url": "tcp://x"}); !isErr || !strings.Contains(text, "url") {
		t.Fatalf("misspelled field must be rejected: %s", text)
	}

	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer hook.Close()
	wh := c.mustTool("create_webhook", map[string]any{"url": hook.URL, "topics": []string{"alarms/#"}, "secret": "s"})
	id := wh["id"].(string)
	if wh["secret"] != config.Redacted {
		t.Fatal("secret must be redacted")
	}
	wh = c.mustTool("update_webhook", map[string]any{"id": id, "batch_size": 10})
	if wh["batch_size"].(float64) != 10 || wh["url"] != hook.URL {
		t.Fatalf("update_webhook: %v", wh)
	}
	c.mustTool("test_webhook", map[string]any{"id": id, "payload": map[string]any{"x": 1}})
	if l := c.mustTool("list_webhooks", nil); len(l["webhooks"].([]any)) != 1 {
		t.Fatal("list_webhooks")
	}
	c.mustTool("delete_webhook", map[string]any{"id": id})

	k := c.mustTool("create_api_key", map[string]any{"name": "grafana", "scopes": []string{"read"}})
	if !strings.HasPrefix(k["key"].(string), "mg_") {
		t.Fatalf("create_api_key: %v", k)
	}
	if p, err := svc.Authenticate(k["key"].(string)); err != nil || !p.Can(config.ScopeRead) || p.Can(config.ScopePublish) {
		t.Fatal("created key must work with read scope only")
	}
	keyID := k["info"].(map[string]any)["id"].(string)
	c.mustTool("revoke_api_key", map[string]any{"id": keyID})
	if _, err := svc.Authenticate(k["key"].(string)); err == nil {
		t.Fatal("revoked key still works")
	}
	c.mustTool("reconnect_broker", nil)
}

func TestResources(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	for i := 0; i < 250; i++ {
		ingest(svc, fmt.Sprintf("sensors/%03d", i), fmt.Sprint(i))
	}
	odd := "plant 1/tank%level/äöü"
	ingest(svc, odd, `{"l":3}`)
	c := newStdio(t, s, reader)
	c.initialize("2025-06-18")

	var all []string
	cursor := ""
	for pages := 0; ; pages++ {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		r := result(t, c.call("resources/list", params))
		for _, x := range r["resources"].([]any) {
			all = append(all, x.(map[string]any)["uri"].(string))
		}
		next, _ := r["nextCursor"].(string)
		if next == "" {
			break
		}
		cursor = next
		if pages > 10 {
			t.Fatal("pagination does not end")
		}
	}
	if len(all) != 251+2 || all[0] != uriStatus {
		t.Fatalf("listed %d resources (want 253), first %v", len(all), all[0])
	}
	oddURI := TopicURI(odd)
	found := false
	for _, u := range all {
		found = found || u == oddURI
	}
	if !found {
		t.Fatalf("topic with special characters not listed as %s", oddURI)
	}

	r := result(t, c.call("resources/read", map[string]any{"uri": oddURI}))
	txt := r["contents"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(txt, `"topic":"plant 1/tank%level/äöü"`) || !strings.Contains(txt, `"payload":{"l":3}`) {
		t.Fatalf("read topic: %s", txt)
	}
	r = result(t, c.call("resources/read", map[string]any{"uri": "mqtt-get://values?filter=sensors%2F%23&limit=5"}))
	txt = r["contents"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(txt, `"total":250`) {
		t.Fatalf("read values: %.200s", txt)
	}
	for _, u := range []string{uriStatus, uriTree} {
		result(t, c.call("resources/read", map[string]any{"uri": u}))
	}
	m := c.call("resources/read", map[string]any{"uri": TopicURI("does/not/exist")})
	if m["error"].(map[string]any)["code"].(float64) != codeNotFound {
		t.Fatalf("missing resource: %v", m)
	}
	if m := c.call("resources/read", map[string]any{"uri": uriBroker}); m["error"] == nil {
		t.Fatal("broker resource needs admin")
	}
	tpl := result(t, c.call("resources/templates/list", nil))["resourceTemplates"].([]any)
	if len(tpl) != 2 {
		t.Fatalf("templates: %v", tpl)
	}
}

func TestSubscriptionsCoalesce(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{NotifyInterval: 200 * time.Millisecond})
	defer s.Close()
	c := newStdio(t, s, reader)
	c.initialize("2025-06-18")
	uri := TopicURI("home/lamp/state")
	result(t, c.call("resources/subscribe", map[string]any{"uri": uri}))
	vuri := "mqtt-get://values?filter=" + "alarms%2F%23"
	result(t, c.call("resources/subscribe", map[string]any{"uri": vuri}))

	ingest(svc, "home/lamp/state", "ON")
	n := <-c.notes
	if n["method"] != "notifications/resources/updated" || n["params"].(map[string]any)["uri"] != uri {
		t.Fatalf("notification: %v", n)
	}
	// A burst of 100 updates is coalesced to a couple of notifications,
	// and the last change is still announced.
	for i := 0; i < 100; i++ {
		ingest(svc, "home/lamp/state", fmt.Sprint(i))
	}
	time.Sleep(600 * time.Millisecond)
	count := 0
	for len(c.notes) > 0 {
		<-c.notes
		count++
	}
	if count < 1 || count > 3 {
		t.Fatalf("burst produced %d notifications, want 1-3", count)
	}
	ingest(svc, "alarms/fire", "1")
	if n := <-c.notes; n["params"].(map[string]any)["uri"] != vuri {
		t.Fatalf("filter subscription: %v", n)
	}
	result(t, c.call("resources/unsubscribe", map[string]any{"uri": uri}))
	result(t, c.call("resources/unsubscribe", map[string]any{"uri": vuri}))
	waitUntil(t, "watchers released", func() bool { return svc.Status().Watchers == 0 })
	ingest(svc, "home/lamp/state", "OFF")
	select {
	case n := <-c.notes:
		t.Fatalf("notification after unsubscribe: %v", n)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestCompletion(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	for _, tp := range []string{"home/kitchen/temp", "home/kitchen/hum", "home/lamp/state", "office/x"} {
		ingest(svc, tp, "1")
	}
	c := newStdio(t, s, reader)
	c.initialize("2025-06-18")
	complete := func(v string) []string {
		r := result(t, c.call("completion/complete", map[string]any{
			"ref":      map[string]any{"type": "ref/resource", "uri": topicTemplate},
			"argument": map[string]any{"name": "topic", "value": v}}))
		var out []string
		for _, x := range r["completion"].(map[string]any)["values"].([]any) {
			out = append(out, x.(string))
		}
		sort.Strings(out)
		return out
	}
	cases := map[string]string{
		"ho":          "home/",
		"home/":       "home/kitchen/,home/lamp/",
		"home/lamp/":  "home/lamp/state",
		"":            "home/,office/",
		"home/kitche": "home/kitchen/",
	}
	for in, want := range cases {
		if got := strings.Join(complete(in), ","); got != want {
			t.Errorf("complete(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPrompts(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	for p, want := range map[*core.Principal]int{reader: 2, operatr: 3, admin: 4} {
		c := newStdio(t, s, p)
		c.initialize("2025-06-18")
		if n := len(result(t, c.call("prompts/list", nil))["prompts"].([]any)); n != want {
			t.Errorf("%s sees %d prompts, want %d", p.Name, n, want)
		}
	}
	// Strict clients (e.g. the official TypeScript SDK) reject null arrays.
	if b, _ := json.Marshal(s.listPrompts(&session{principal: admin})); strings.Contains(string(b), "null") {
		t.Fatalf("prompts/list must not contain null: %s", b)
	}
	c := newStdio(t, s, admin)
	c.initialize("2025-06-18")
	r := result(t, c.call("prompts/get", map[string]any{"name": "monitor_topics", "arguments": map[string]any{"filter": "alarms/#"}}))
	text := r["messages"].([]any)[0].(map[string]any)["content"].(map[string]any)["text"].(string)
	if !strings.Contains(text, "alarms/#") || !strings.Contains(text, "wait_for_message") {
		t.Fatalf("monitor prompt: %s", text)
	}
	if m := c.call("prompts/get", map[string]any{"name": "control_device"}); m["error"] == nil {
		t.Fatal("missing required prompt arguments must fail")
	}
	r = result(t, c.call("prompts/get", map[string]any{"name": "troubleshoot_connection"}))
	if len(r["messages"].([]any)) != 3 {
		t.Fatalf("troubleshoot prompt should embed status and broker resources: %v", r)
	}
	cr := newStdio(t, s, reader)
	cr.initialize("2025-06-18")
	if m := cr.call("prompts/get", map[string]any{"name": "troubleshoot_connection"}); m["error"] == nil {
		t.Fatal("admin prompt must be refused for read keys")
	}
}

// --- HTTP transport ------------------------------------------------------

type httpEnv struct {
	t   *testing.T
	svc *core.Service
	srv *Server
	url string
}

func newHTTP(t *testing.T) *httpEnv {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{NotifyInterval: 50 * time.Millisecond, AllowedOrigins: []string{"https://dash.example.com"}})
	t.Cleanup(s.Close)
	h := s.HTTPHandler(func(r *http.Request) (*core.Principal, error) {
		return svc.Authenticate(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	}, 0)
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return &httpEnv{t: t, svc: svc, srv: s, url: hs.URL}
}

func (e *httpEnv) post(key, session, body string, hdr ...string) (*http.Response, map[string]any) {
	e.t.Helper()
	req, _ := http.NewRequest("POST", e.url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	if session != "" {
		req.Header.Set("Mcp-Session-Id", session)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return resp, m
}

const initBody = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}`

func TestHTTPSessionsAndSSE(t *testing.T) {
	e := newHTTP(t)
	ingest(e.svc, "home/lamp/state", "OFF")

	resp, m := e.post("read-key", "", initBody)
	sid := resp.Header.Get("Mcp-Session-Id")
	if resp.StatusCode != 200 || sid == "" || m["result"].(map[string]any)["capabilities"].(map[string]any)["resources"].(map[string]any)["subscribe"] != true {
		t.Fatalf("initialize: %d %q %v", resp.StatusCode, sid, m)
	}
	if resp, _ := e.post("read-key", sid, `{"jsonrpc":"2.0","method":"notifications/initialized"}`); resp.StatusCode != 202 {
		t.Fatalf("notification: %d", resp.StatusCode)
	}

	// Open the notification stream.
	req, _ := http.NewRequest("GET", e.url, nil)
	req.Header.Set("Authorization", "Bearer read-key")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Mcp-Session-Id", sid)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil || stream.StatusCode != 200 || !strings.HasPrefix(stream.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("stream: %v %v", err, stream)
	}
	events := make(chan string, 10)
	go func() {
		sc := bufio.NewScanner(stream.Body)
		for sc.Scan() {
			if d, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
				events <- d
			}
		}
	}()
	// A second stream for the same session is refused.
	req2 := req.Clone(context.Background())
	if r2, err := http.DefaultClient.Do(req2); err != nil || r2.StatusCode != http.StatusConflict {
		t.Fatalf("second stream: %v %v", err, r2.StatusCode)
	}

	uri := TopicURI("home/lamp/state")
	if _, m := e.post("read-key", sid, `{"jsonrpc":"2.0","id":2,"method":"resources/subscribe","params":{"uri":"`+uri+`"}}`); m["error"] != nil {
		t.Fatalf("subscribe: %v", m)
	}
	ingest(e.svc, "home/lamp/state", "ON")
	select {
	case ev := <-events:
		if !strings.Contains(ev, "notifications/resources/updated") || !strings.Contains(ev, uri) {
			t.Fatalf("event: %s", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no SSE notification")
	}

	// Batch request in the session.
	req3, _ := http.NewRequest("POST", e.url, strings.NewReader(`[{"jsonrpc":"2.0","id":3,"method":"ping"},{"jsonrpc":"2.0","id":4,"method":"tools/list"}]`))
	req3.Header.Set("Authorization", "Bearer read-key")
	req3.Header.Set("Mcp-Session-Id", sid)
	r3, _ := http.DefaultClient.Do(req3)
	var batch []map[string]any
	_ = json.NewDecoder(r3.Body).Decode(&batch)
	r3.Body.Close()
	if len(batch) != 2 {
		t.Fatalf("batch: %v", batch)
	}

	// Another key cannot use this session.
	if resp, _ := e.post("admin-key", sid, `{"jsonrpc":"2.0","id":5,"method":"ping"}`); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign key on session: %d", resp.StatusCode)
	}

	// End the session: stream closes, id becomes unknown, watchers released.
	del, _ := http.NewRequest("DELETE", e.url, nil)
	del.Header.Set("Authorization", "Bearer read-key")
	del.Header.Set("Mcp-Session-Id", sid)
	if r, _ := http.DefaultClient.Do(del); r.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: %d", r.StatusCode)
	}
	if resp, _ := e.post("read-key", sid, `{"jsonrpc":"2.0","id":6,"method":"ping"}`); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expired session: %d", resp.StatusCode)
	}
	waitUntil(t, "watchers released", func() bool { return e.svc.Status().Watchers == 0 })
}

func TestHTTPStatelessAndSecurity(t *testing.T) {
	e := newHTTP(t)
	ingest(e.svc, "a/b", "1")
	// Stateless: tools work without a session...
	resp, m := e.post("read-key", "", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_value","arguments":{"topic":"a/b"}}}`)
	if resp.StatusCode != 200 || resp.Header.Get("Mcp-Session-Id") != "" || m["result"] == nil {
		t.Fatalf("stateless call: %d %v", resp.StatusCode, m)
	}
	// ...but subscriptions need one.
	if _, m := e.post("read-key", "", `{"jsonrpc":"2.0","id":2,"method":"resources/subscribe","params":{"uri":"mqtt-get://topic/a/b"}}`); m["error"] == nil {
		t.Fatal("stateless subscribe must fail")
	}
	if e.srv.Sessions() != 0 {
		t.Fatal("stateless requests must not create sessions")
	}
	if resp, _ := e.post("", "", initBody); resp.StatusCode != 401 {
		t.Fatalf("no key: %d", resp.StatusCode)
	}
	if resp, _ := e.post("read-key", "", initBody, "Origin", "https://evil.example"); resp.StatusCode != 403 {
		t.Fatalf("foreign origin: %d", resp.StatusCode)
	}
	if resp, _ := e.post("read-key", "", initBody, "Origin", "https://dash.example.com"); resp.StatusCode != 200 {
		t.Fatalf("allowed origin: %d", resp.StatusCode)
	}
	if resp, _ := e.post("read-key", "", initBody, "MCP-Protocol-Version", "2000-01-01"); resp.StatusCode != 400 {
		t.Fatalf("bad protocol version: %d", resp.StatusCode)
	}
	if resp, _ := e.post("read-key", "", `{not json`); resp.StatusCode != 400 {
		t.Fatalf("parse error: %d", resp.StatusCode)
	}
	if resp, _ := e.post("read-key", "nope", `{"jsonrpc":"2.0","id":1,"method":"ping"}`); resp.StatusCode != 404 {
		t.Fatalf("unknown session: %d", resp.StatusCode)
	}
	req, _ := http.NewRequest("GET", e.url, nil)
	req.Header.Set("Authorization", "Bearer read-key")
	if r, _ := http.DefaultClient.Do(req); r.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET without event-stream accept: %d", r.StatusCode)
	}
}
