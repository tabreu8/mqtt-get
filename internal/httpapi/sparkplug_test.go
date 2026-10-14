package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/gorilla/websocket"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/sparkplug"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
	spb "github.com/tabreu8/mqtt-get/sparkplug"
)

// edgeNode plays a Sparkplug B edge node with one device on the embedded
// broker: it answers rebirth requests and applies DCMD writes.
type edgeNode struct {
	t      *testing.T
	b      *mochi.Server
	mu     sync.Mutex
	seq    uint64
	setp   float64
	cmds   []spb.Metric
	births int
}

func (n *edgeNode) next() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.seq % 256
	n.seq++
	return s
}

func (n *edgeNode) publish(typ, dev string, metrics ...spb.Metric) {
	n.t.Helper()
	topic := "spBv1.0/plant/" + typ + "/gw1"
	if dev != "" {
		topic += "/" + dev
	}
	p := &spb.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Metrics: metrics}
	if typ != sparkplug.NDEATH {
		p.Seq, p.HasSeq = n.next(), true
	}
	if err := n.b.Publish(topic, spb.Encode(p), false, 0); err != nil {
		n.t.Fatal(err)
	}
}

func (n *edgeNode) birth() {
	n.mu.Lock()
	n.seq, n.births = 0, n.births+1
	setp := n.setp
	n.mu.Unlock()
	n.publish(sparkplug.NBIRTH, "",
		spb.Metric{Name: "bdSeq", Datatype: spb.TypeUInt64, Value: spb.Value{Kind: spb.KindLong, Uint: 1}},
		spb.Metric{Name: sparkplug.RebirthMetric, Datatype: spb.TypeBoolean, Value: spb.Value{Kind: spb.KindBool}})
	n.publish(sparkplug.DBIRTH, "press",
		spb.Metric{Name: "Temperature", Alias: 1, HasAlias: true, Datatype: spb.TypeDouble, Value: spb.Value{Kind: spb.KindDouble, Float: 20},
			Properties: &spb.PropertySet{Keys: []string{"engUnit"}, Values: []spb.PropertyValue{{Type: spb.TypeString, Value: spb.Value{Kind: spb.KindString, Str: "C"}}}}},
		spb.Metric{Name: "Setpoint", Alias: 2, HasAlias: true, Datatype: spb.TypeFloat, Value: spb.Value{Kind: spb.KindFloat, Float: setp}})
}

func (n *edgeNode) temperature(v float64) {
	n.publish(sparkplug.DDATA, "press", spb.Metric{Alias: 1, HasAlias: true, Value: spb.Value{Kind: spb.KindDouble, Float: v}})
}

func (n *edgeNode) listen() {
	handle := func(_ *mochi.Client, _ packets.Subscription, pk packets.Packet) {
		p, err := spb.Decode(pk.Payload)
		if err != nil {
			n.t.Errorf("edge node got an invalid command on %s: %v", pk.TopicName, err)
			return
		}
		for _, m := range p.Metrics {
			n.mu.Lock()
			n.cmds = append(n.cmds, m)
			n.mu.Unlock()
			switch {
			case m.Name == sparkplug.RebirthMetric && m.Value.Bool:
				go n.birth()
			case m.Name == "Setpoint":
				n.mu.Lock()
				n.setp = m.Value.Float
				n.mu.Unlock()
				go n.publish(sparkplug.DDATA, "press", spb.Metric{Alias: 2, HasAlias: true, Value: m.Value})
			}
		}
	}
	if err := n.b.Subscribe("spBv1.0/plant/NCMD/gw1", 1, handle); err != nil {
		n.t.Fatal(err)
	}
	if err := n.b.Subscribe("spBv1.0/plant/DCMD/gw1/+", 2, handle); err != nil {
		n.t.Fatal(err)
	}
}

func (n *edgeNode) commands() []spb.Metric {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]spb.Metric(nil), n.cmds...)
}

func TestSparkplugEndToEnd(t *testing.T) {
	e := setup(t, "secret")
	e.waitConnected()
	edge := &edgeNode{t: t, b: e.broker, setp: 50}
	edge.listen()

	// Disabled by default: the raw message is stored as-is.
	edge.birth()
	eventually(t, func() bool { _, ok := e.svc.Store().Get("spBv1.0/plant/DBIRTH/gw1/press"); return ok })
	if code, out, _ := e.do("GET", "/api/v1/sparkplug/nodes", "read-key", ""); code != 400 || !strings.Contains(out, "disabled") {
		t.Fatalf("nodes while disabled: %d %s", code, out)
	}

	if code, _, _ := e.do("PUT", "/api/v1/sparkplug", "read-key", `{"enabled":true}`); code != 403 {
		t.Fatal("read key configuring Sparkplug")
	}
	if code, out, _ := e.do("PUT", "/api/v1/sparkplug", e.admin, `{"enabled":true,"prefix":"spBv1.0/x"}`); code != 400 {
		t.Fatalf("prefix inside the namespace: %d %s", code, out)
	}
	code, out, _ := e.do("PUT", "/api/v1/sparkplug", e.admin, `{"enabled":true,"rebirth":true,"rebirth_interval_sec":1,"commands":true}`)
	if code != 200 || !strings.Contains(out, `"subscribed":true`) || !strings.Contains(out, `"source":"saved"`) {
		t.Fatalf("enable: %d %s", code, out)
	}

	// Data from a node whose birth mqtt-get missed (it was disabled): it asks
	// for a rebirth, the node answers, and the values become readable.
	edge.temperature(21.5)
	eventually(t, func() bool {
		v, ok := e.svc.Store().Get("spb/plant/gw1/press/Temperature")
		return ok && strings.Contains(string(v.Payload), `"online":true`)
	})
	edge.mu.Lock()
	births := edge.births
	edge.mu.Unlock()
	if births < 2 {
		t.Fatal("no rebirth requested")
	}
	edge.temperature(22.75)
	eventually(t, func() bool {
		v, _ := e.svc.Store().Get("spb/plant/gw1/press/Temperature")
		return v != nil && strings.Contains(string(v.Payload), `"value":22.75`)
	})
	_, out, _ = e.do("GET", "/api/v1/values/spb/plant/gw1/press/Temperature", "read-key", "")
	var val struct {
		Payload struct {
			Value  float64
			Type   string
			Unit   string
			Online bool
		}
	}
	if json.Unmarshal([]byte(out), &val) != nil || val.Payload.Value != 22.75 || val.Payload.Type != "Double" || val.Payload.Unit != "C" || !val.Payload.Online {
		t.Fatalf("translated value: %s", out)
	}
	if _, ok := e.svc.Store().Get("spBv1.0/plant/DDATA/gw1/press"); ok {
		t.Fatal("raw message stored although keep_raw is false")
	}
	_, out, _ = e.do("GET", "/api/v1/sparkplug/nodes?metrics=true", "read-key", "")
	if !strings.Contains(out, `"edge_node":"gw1"`) || !strings.Contains(out, `"id":"press"`) || !strings.Contains(out, `"topic":"spb/plant/gw1/press/Setpoint"`) {
		t.Fatalf("nodes: %s", out)
	}

	// Writes: a publish to the translated topic becomes a typed DCMD, and
	// publish-and-wait sees the device's answer on the same topic.
	if code, _, _ := e.do("POST", "/api/v1/publish/spb/plant/gw1/press/Setpoint", "read-key", "61.5"); code != 403 {
		t.Fatal("read key writing a Sparkplug metric")
	}
	if code, out, _ := e.do("POST", "/api/v1/publish/spb/plant/gw1/press/Setpoint", e.admin, "61.5"); code != 200 {
		t.Fatalf("write: %d %s", code, out)
	}
	eventually(t, func() bool {
		for _, m := range edge.commands() {
			if m.Name == "Setpoint" && m.Datatype == spb.TypeFloat && m.Value.Kind == spb.KindFloat && m.Value.Float == 61.5 {
				return true
			}
		}
		return false
	})
	eventually(t, func() bool {
		v, _ := e.svc.Store().Get("spb/plant/gw1/press/Setpoint")
		return v != nil && strings.Contains(string(v.Payload), `"value":61.5`)
	})
	for _, bad := range []struct {
		path, body string
		code       int
	}{
		{"/api/v1/publish/spb/plant/gw1/press/Setpoint", "hot", 400},
		{"/api/v1/publish/spb/plant/gw1/press/Nope", "1", 404},
		{"/api/v1/publish/spb/plant/gw1/press/Setpoint?retain=true", "1", 400},
	} {
		if code, out, _ := e.do("POST", bad.path, e.admin, bad.body); code != bad.code {
			t.Errorf("%s %s: %d %s", bad.path, bad.body, code, out)
		}
	}

	// commands_filter limits which metrics are writable.
	if code, out, _ := e.do("PUT", "/api/v1/sparkplug", e.admin, `{"enabled":true,"rebirth":true,"rebirth_interval_sec":1,"commands":true,"commands_filter":["spb/+/+/+/Other/#"]}`); code != 200 {
		t.Fatalf("commands_filter: %d %s", code, out)
	}
	if code, out, _ := e.do("POST", "/api/v1/publish/spb/plant/gw1/press/Setpoint", e.admin, "62"); code != 403 || !strings.Contains(out, "commands_filter") {
		t.Fatalf("write outside commands_filter: %d %s", code, out)
	}
	if code, _, _ := e.do("PUT", "/api/v1/sparkplug", e.admin, `{"enabled":true,"commands":true,"commands_filter":["a/#/b"]}`); code != 400 {
		t.Fatal("invalid commands_filter accepted")
	}

	// Manual rebirth, rate limited per node.
	time.Sleep(1100 * time.Millisecond)
	if code, out, _ := e.do("POST", "/api/v1/sparkplug/nodes/plant/gw1/rebirth", e.admin, ""); code != 200 {
		t.Fatalf("rebirth: %d %s", code, out)
	}
	if code, _, _ := e.do("POST", "/api/v1/sparkplug/nodes/plant/gw1/rebirth", e.admin, ""); code != 429 {
		t.Fatal("rebirth not rate limited")
	}
	if code, _, _ := e.do("POST", "/api/v1/sparkplug/nodes/plant/nope/rebirth", e.admin, ""); code != 404 {
		t.Fatal("rebirth of an unknown node")
	}

	// Death marks the values offline.
	edge.publish(sparkplug.NDEATH, "", spb.Metric{Name: "bdSeq", Value: spb.Value{Kind: spb.KindLong, Uint: 1}})
	eventually(t, func() bool {
		v, _ := e.svc.Store().Get("spb/plant/gw1/press/Temperature")
		return v != nil && strings.Contains(string(v.Payload), `"online":false`)
	})
	if _, out, _ := e.do("GET", "/api/v1/status", "read-key", ""); !strings.Contains(out, `"sparkplug":{"messages":`) || !strings.Contains(out, `"nodes_online":0`) {
		t.Fatalf("status: %s", out)
	}
	if _, out, _ := e.do("GET", "/metrics", "read-key", ""); !strings.Contains(out, "mqttget_sparkplug_commands_total 1") {
		t.Fatalf("metrics: %s", out)
	}

	// Disabling forgets the translated values; the setting is persisted.
	if code, _, _ := e.do("PUT", "/api/v1/sparkplug", e.admin, `{"enabled":false}`); code != 200 {
		t.Fatal("disable")
	}
	if _, ok := e.svc.Store().Get("spb/plant/gw1/press/Temperature"); ok {
		t.Fatal("translated values kept after disabling")
	}
	if c, ok := e.st.Sparkplug(); !ok || c.Enabled {
		t.Fatalf("persisted %+v", c)
	}
}

func wsDial(t *testing.T, e *env, path string, hdr http.Header) *websocket.Conn {
	t.Helper()
	c, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(e.http.URL, "http")+path, hdr)
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

func wsRead(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var m map[string]any
	if err := c.ReadJSON(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

// wsUntil reads until a frame of the given type arrives.
func wsUntil(t *testing.T, c *websocket.Conn, typ string) map[string]any {
	t.Helper()
	for i := 0; i < 50; i++ {
		if m := wsRead(t, c); m["type"] == typ {
			return m
		}
	}
	t.Fatalf("no %s frame", typ)
	return nil
}

func TestWebSocketStream(t *testing.T) {
	e := setup(t, "secret")
	e.waitConnected()
	_ = e.broker.Publish("ws/a", []byte(`{"n":1}`), true, 0)
	eventually(t, func() bool { _, ok := e.svc.Store().Get("ws/a"); return ok })

	// Header auth plus a subscription in the URL, with the current values.
	hdr := http.Header{"Authorization": {"Bearer read-key"}}
	c := wsDial(t, e, "/api/v1/ws?filter=ws/%23&include_current=true", hdr)
	if m := wsRead(t, c); m["type"] != "subscribed" || m["filter"] != "ws/#" {
		t.Fatalf("subscribed: %v", m)
	}
	m := wsRead(t, c)
	if m["type"] != "message" || m["current"] != true || m["data"].(map[string]any)["topic"] != "ws/a" {
		t.Fatalf("current value: %v", m)
	}
	_ = e.broker.Publish("ws/b", []byte("hello"), false, 0)
	m = wsUntil(t, c, "message")
	if d := m["data"].(map[string]any); d["topic"] != "ws/b" || d["payload"] != "hello" || m["current"] != nil {
		t.Fatalf("live message: %v", m)
	}
	// A read key can't publish.
	_ = c.WriteJSON(map[string]any{"type": "publish", "id": 7, "topic": "ws/c", "payload": "x"})
	if m := wsUntil(t, c, "error"); !strings.Contains(m["error"].(string), "publish scope") || m["id"] != 7.0 {
		t.Fatalf("publish with a read key: %v", m)
	}
	_ = c.WriteJSON(map[string]any{"type": "unsubscribe", "filter": "ws/#"})
	if m := wsUntil(t, c, "unsubscribed"); m["found"] != true {
		t.Fatalf("unsubscribe: %v", m)
	}

	// Browser style: no header, auth message first; then subscribe and
	// publish over the same socket.
	b := wsDial(t, e, "/api/v1/ws", nil)
	_ = b.WriteJSON(map[string]any{"type": "subscribe", "filter": "x"})
	_ = b.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := b.ReadMessage(); !websocket.IsCloseError(err, 4401) {
		t.Fatalf("unauthenticated socket: %v", err)
	}
	b = wsDial(t, e, "/api/v1/ws", nil)
	_ = b.WriteJSON(map[string]any{"type": "auth", "key": e.admin})
	if m := wsRead(t, b); m["type"] != "authenticated" {
		t.Fatalf("auth: %v", m)
	}
	_ = b.WriteJSON(map[string]any{"type": "subscribe", "filters": []string{"echo/+", "echo/+"}, "id": "s1"})
	wsUntil(t, b, "subscribed")
	if m := wsUntil(t, b, "subscribed"); m["already"] != true {
		t.Fatalf("duplicate subscription: %v", m)
	}
	_ = b.WriteJSON(map[string]any{"type": "publish", "id": "p1", "topic": "echo/1", "payload": map[string]any{"v": 2}, "qos": 1})
	got := map[string]bool{}
	for len(got) < 2 {
		m := wsRead(t, b)
		switch m["type"] {
		case "published":
			got["published"] = m["id"] == "p1"
		case "message":
			got["message"] = m["data"].(map[string]any)["payload"].(map[string]any)["v"] == 2.0
		}
	}
	if !got["published"] || !got["message"] {
		t.Fatalf("publish over WebSocket: %v", got)
	}
	_ = b.WriteJSON(map[string]any{"type": "ping", "id": 1})
	wsUntil(t, b, "pong")

	// Bad keys and foreign origins are refused before the upgrade.
	if _, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(e.http.URL, "http")+"/api/v1/ws",
		http.Header{"Authorization": {"Bearer nope"}}); err == nil || resp.StatusCode != 401 {
		t.Fatalf("bad key: %v", err)
	}
	if _, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(e.http.URL, "http")+"/api/v1/ws",
		http.Header{"Origin": {"https://evil.example"}}); err == nil || resp.StatusCode != 403 {
		t.Fatalf("foreign origin: %v", err)
	}
	if e.svc.Status().Watchers == 0 {
		t.Fatal("watchers not registered")
	}
	b.Close()
	c.Close()
	eventually(t, func() bool { return e.svc.Status().Watchers == 0 })
}

func TestSparkplugFromEnvConfig(t *testing.T) {
	c := config.Sparkplug{Enabled: true}
	c.Normalize()
	if c.Prefix != "spb" || c.RebirthIntervalSec != 30 || c.Validate() != nil {
		t.Fatalf("%+v", c)
	}
}

func TestSparkplugWithHistoryRawAndPrefix(t *testing.T) {
	e := newWSEnv(t, config.Server{})
	if _, err := e.svc.PutHistoryRule(config.HistoryRule{ID: "spb", Topics: []string{"spb/#", "uns/#"}}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.SetSparkplug(config.Sparkplug{Enabled: true, KeepRaw: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ing := func(topic string, p *spb.Payload, at time.Time) {
		e.svc.Ingest(store.Entry{Topic: topic, Payload: spb.Encode(p), Time: at.UnixNano()})
	}
	dbl := func(alias uint64, f float64) spb.Metric {
		return spb.Metric{Alias: alias, HasAlias: true, Value: spb.Value{Kind: spb.KindDouble, Float: f}}
	}
	ing("spBv1.0/g/DBIRTH/e/d", &spb.Payload{Seq: 0, HasSeq: true, Metrics: []spb.Metric{
		{Name: "Level", Alias: 1, HasAlias: true, Datatype: spb.TypeDouble, Value: spb.Value{Kind: spb.KindDouble, Float: 1}}}}, now.Add(-3*time.Second))
	ing("spBv1.0/g/DDATA/e/d", &spb.Payload{Seq: 1, HasSeq: true, Metrics: []spb.Metric{dbl(1, 2)}}, now.Add(-2*time.Second))
	ing("spBv1.0/g/DDATA/e/d", &spb.Payload{Seq: 2, HasSeq: true, Metrics: []spb.Metric{dbl(1, 3)}}, now.Add(-time.Second))
	// A backfilled (historical) point older than everything else.
	hist := dbl(1, 0.5)
	hist.IsHistorical, hist.Timestamp, hist.HasTimestamp = true, uint64(now.Add(-time.Hour).UnixMilli()), true
	ing("spBv1.0/g/DDATA/e/d", &spb.Payload{Seq: 3, HasSeq: true, Metrics: []spb.Metric{hist}}, now)

	// The latest value is the newest real-time one, not the backfilled one.
	if v, err := e.svc.Latest("spb/g/e/d/Level"); err != nil || !strings.Contains(string(v.Payload), `"value":3,`) {
		t.Fatalf("latest: %v %v", v, err)
	}
	// History has all four points in time order, and the numeric summary
	// works on the "value" field of the envelopes.
	res, err := e.svc.History("spb/g/e/d/Level", core.HistoryQuery{})
	if err != nil || res.Total != 4 {
		t.Fatalf("history: %v %+v", err, res)
	}
	sum, _, err := e.svc.HistorySummary("spb/g/e/d/Level", core.SummaryQuery{Field: "value"})
	if err != nil || sum.Numeric == nil || sum.Numeric.Min != 0.5 || sum.Numeric.Max != 3 {
		t.Fatalf("summary: %v %+v", err, sum)
	}
	// keep_raw: the raw protobuf message is stored too.
	if _, ok := e.svc.Store().Get("spBv1.0/g/DDATA/e/d"); !ok {
		t.Fatal("raw message not kept")
	}

	// Changing the prefix forgets the old translated values; new messages
	// use the new prefix.
	if _, err := e.svc.SetSparkplug(config.Sparkplug{Enabled: true, Prefix: "uns/spb"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.svc.Store().Get("spb/g/e/d/Level"); ok {
		t.Fatal("old prefix kept")
	}
	ing("spBv1.0/g/DBIRTH/e/d", &spb.Payload{Seq: 0, HasSeq: true, Metrics: []spb.Metric{
		{Name: "Level", Datatype: spb.TypeDouble, Value: spb.Value{Kind: spb.KindDouble, Float: 4}}}}, now)
	if _, err := e.svc.Latest("uns/spb/g/e/d/Level"); err != nil {
		t.Fatalf("new prefix: %v", err)
	}
	// Publishes under the prefix are never sent as plain MQTT: refused
	// while writes are disabled, and for topics that aren't metrics.
	if _, err := e.svc.Publish(t.Context(), []mqttc.Message{{Topic: "uns/spb/g/e/d/Level", Payload: []byte("1")}}); core.KindOf(err) != core.Invalid || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("publish with commands disabled: %v", err)
	}
	if _, err := e.svc.SetSparkplug(config.Sparkplug{Enabled: true, Prefix: "uns/spb", Commands: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Publish(t.Context(), []mqttc.Message{{Topic: "uns/spb/g/e/d/Nope", Payload: []byte("1")}}); core.KindOf(err) != core.NotFound {
		t.Fatalf("publish to an unknown metric: %v", err)
	}
}

func TestSparkplugTransientIsStreamedNotStored(t *testing.T) {
	e := newWSEnv(t, config.Server{})
	if _, err := e.svc.PutHistoryRule(config.HistoryRule{ID: "all", Topics: []string{"spb/#"}}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.SetSparkplug(config.Sparkplug{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	got := make(chan string, 1)
	cancel, err := e.svc.Watch("spb/#", func(en *store.Entry) { got <- string(en.Payload) })
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	e.svc.Ingest(store.Entry{Topic: "spBv1.0/g/NDATA/e", Time: time.Now().UnixNano(), Payload: spb.Encode(&spb.Payload{Metrics: []spb.Metric{
		{Name: "Pulse", Datatype: spb.TypeBoolean, IsTransient: true, Value: spb.Value{Kind: spb.KindBool, Bool: true}}}})})
	select {
	case p := <-got:
		if !strings.Contains(p, `"transient":true`) {
			t.Fatalf("streamed %s", p)
		}
	case <-time.After(time.Second):
		t.Fatal("transient value not delivered to watchers")
	}
	if _, err := e.svc.Latest("spb/g/e/Pulse"); core.KindOf(err) != core.NotFound {
		t.Fatalf("transient value stored: %v", err)
	}
	if _, err := e.svc.History("spb/g/e/Pulse", core.HistoryQuery{}); core.KindOf(err) != core.NotFound {
		t.Fatalf("transient value recorded in history: %v", err)
	}
}

func TestSparkplugReorderTimeoutFlushes(t *testing.T) {
	e := newWSEnv(t, config.Server{})
	if err := e.svc.Start(config.Broker{}, false); err != nil {
		t.Fatal(err)
	}
	timeout := 150
	if _, err := e.svc.SetSparkplug(config.Sparkplug{Enabled: true, ReorderTimeoutMs: &timeout}); err != nil {
		t.Fatal(err)
	}
	send := func(typ string, seq uint64, v float64) {
		e.svc.Ingest(store.Entry{Topic: "spBv1.0/g/" + typ + "/e", Time: time.Now().UnixNano(), Payload: spb.Encode(&spb.Payload{Seq: seq, HasSeq: true,
			Metrics: []spb.Metric{{Name: "A", Datatype: spb.TypeDouble, Value: spb.Value{Kind: spb.KindDouble, Float: v}}}})})
	}
	send("NBIRTH", 0, 0)
	send("NDATA", 2, 2) // 1 never arrives
	if v, _ := e.svc.Store().Get("spb/g/e/A"); v == nil || !strings.Contains(string(v.Payload), `"value":0`) {
		t.Fatal("message ahead of its turn translated before the timeout")
	}
	eventually(t, func() bool {
		v, _ := e.svc.Store().Get("spb/g/e/A")
		return v != nil && strings.Contains(string(v.Payload), `"value":2`)
	})
	if st := e.svc.Sparkplug().Stats; st.SeqErrors != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestSparkplugDefinitionsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := func() *core.Service {
		st, err := state.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		svc := core.New(config.Server{DataDir: dir, MaxTopics: 100}, log, st)
		if err := svc.Start(config.Broker{}, false); err != nil {
			t.Fatal(err)
		}
		return svc
	}
	svc := start()
	if _, err := svc.SetSparkplug(config.Sparkplug{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	svc.Ingest(store.Entry{Topic: "spBv1.0/g/NBIRTH/e", Time: time.Now().UnixNano(), Payload: spb.Encode(&spb.Payload{Seq: 0, HasSeq: true,
		Metrics: []spb.Metric{{Name: "Level", Alias: 4, HasAlias: true, Datatype: spb.TypeFloat, Value: spb.Value{Kind: spb.KindFloat, Float: 1}}}})})
	svc.Close() // saves the definitions

	svc = start()
	defer svc.Close()
	svc.Ingest(store.Entry{Topic: "spBv1.0/g/NDATA/e", Time: time.Now().UnixNano(), Payload: spb.Encode(&spb.Payload{Seq: 1, HasSeq: true,
		Metrics: []spb.Metric{{Alias: 4, HasAlias: true, Value: spb.Value{Kind: spb.KindFloat, Float: 2.5}}}})})
	v, err := svc.Latest("spb/g/e/Level")
	if err != nil || !strings.Contains(string(v.Payload), `"value":2.5`) || !strings.Contains(string(v.Payload), `"type":"Float"`) {
		t.Fatalf("alias-only data after a restart: %v %s", err, v.Payload)
	}
}

// hostRecorder records what client "test" (mqtt-get) sends: its will, then
// "sub <filter>" and "pub <topic> <payload>" in order.
type hostRecorder struct {
	mochi.HookBase
	mu     sync.Mutex
	events []string
	will   packets.Packet
}

func (h *hostRecorder) ID() string { return "host-recorder" }
func (h *hostRecorder) Provides(b byte) bool {
	return b == mochi.OnConnect || b == mochi.OnSubscribe || b == mochi.OnPublish
}
func (h *hostRecorder) OnConnect(cl *mochi.Client, pk packets.Packet) error {
	if cl.ID == "test" {
		h.mu.Lock()
		h.events = append(h.events, "connect")
		h.will = pk
		h.mu.Unlock()
	}
	return nil
}
func (h *hostRecorder) OnSubscribe(cl *mochi.Client, pk packets.Packet) packets.Packet {
	if cl.ID == "test" {
		h.mu.Lock()
		for _, f := range pk.Filters {
			h.events = append(h.events, "sub "+f.Filter)
		}
		h.mu.Unlock()
	}
	return pk
}
func (h *hostRecorder) OnPublish(cl *mochi.Client, pk packets.Packet) (packets.Packet, error) {
	if cl.ID == "test" {
		h.mu.Lock()
		h.events = append(h.events, "pub "+pk.TopicName+" "+string(pk.Payload))
		h.mu.Unlock()
	}
	return pk, nil
}
func (h *hostRecorder) snapshot() ([]string, packets.Packet) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.events...), h.will
}

func TestSparkplugPrimaryHost(t *testing.T) {
	for _, c := range []struct {
		name    string
		version uint
		client  string
	}{{"lean 3.1.1", 4, ""}, {"lean 5", 5, ""}, {"paho 3.1.1", 4, "paho"}, {"paho 5", 5, "paho"}} {
		t.Run(c.name, func(t *testing.T) { testPrimaryHost(t, setupClient(t, "secret", c.version, c.client)) })
	}
}

func testPrimaryHost(t *testing.T, e *env) {
	rec := &hostRecorder{}
	if err := e.broker.AddHook(rec, nil); err != nil {
		t.Fatal(err)
	}
	e.waitConnected()
	if code, _, _ := e.do("PUT", "/api/v1/sparkplug", e.admin, `{"enabled":true,"host_id":"a/b"}`); code != 400 {
		t.Fatal("host_id with a slash accepted")
	}
	if code, out, _ := e.do("PUT", "/api/v1/sparkplug", e.admin, `{"enabled":true,"rebirth":true,"host_id":"mqttget"}`); code != 200 {
		t.Fatalf("host mode: %d %s", code, out)
	}
	// It reconnects with a STATE will, subscribes, then publishes its
	// STATE birth (same timestamp, QoS 1, retained) before anything else.
	var ts int64
	eventually(t, func() bool {
		ev, will := rec.snapshot()
		for _, s := range ev {
			if strings.HasPrefix(s, "pub spBv1.0/STATE/mqttget ") {
				online, t0, ok := sparkplug.ParseState([]byte(strings.TrimPrefix(s, "pub spBv1.0/STATE/mqttget ")))
				wOnline, wts, wok := sparkplug.ParseState(will.Connect.WillPayload)
				if !ok || !online || !wok || wOnline || wts != t0 {
					t.Fatalf("birth %q, will %q", s, will.Connect.WillPayload)
				}
				ts = t0
				return true
			}
		}
		return false
	})
	ev, will := rec.snapshot()
	if !will.Connect.WillFlag || will.Connect.WillTopic != "spBv1.0/STATE/mqttget" || will.Connect.WillQos != 1 || !will.Connect.WillRetain || !will.Connect.Clean {
		t.Fatalf("will %+v", will.Connect)
	}
	// Events of the last connection: subscriptions, then the STATE birth.
	last := 0
	for i, s := range ev {
		if s == "connect" {
			last = i
		}
	}
	ev = ev[last:]
	subs := map[string]bool{}
	for _, s := range ev[1:] {
		if strings.HasPrefix(s, "pub ") {
			if !strings.HasPrefix(s, "pub spBv1.0/STATE/mqttget ") {
				t.Fatalf("published %q before its STATE birth: %v", s, ev)
			}
			break
		}
		subs[strings.TrimPrefix(s, "sub ")] = true
	}
	if !subs["spBv1.0/#"] || !subs["spBv1.0/STATE/mqttget"] {
		t.Fatalf("subscriptions before the STATE birth: %v", ev)
	}

	// Someone says we're offline: it answers with its birth again.
	births := func() int {
		n := 0
		ev, _ := rec.snapshot()
		for _, s := range ev {
			if s == "pub spBv1.0/STATE/mqttget "+string(sparkplug.StatePayload(true, ts)) {
				n++
			}
		}
		return n
	}
	before := births()
	_ = e.broker.Publish("spBv1.0/STATE/mqttget", sparkplug.StatePayload(false, ts-1000), false, 1) // an older session: ignored
	_ = e.broker.Publish("spBv1.0/STATE/mqttget", sparkplug.StatePayload(false, ts), false, 1)
	eventually(t, func() bool { return births() == before+1 })
	time.Sleep(100 * time.Millisecond)
	if births() != before+1 {
		t.Fatal("answered the death of an older session")
	}

	// After losing the connection: a new will and birth, with a new
	// timestamp.
	time.Sleep(5 * time.Millisecond)
	if cl, ok := e.broker.Clients.Get("test"); ok {
		cl.Stop(errors.New("kicked"))
	}
	eventually(t, func() bool {
		ev, will := rec.snapshot()
		_, wts, _ := sparkplug.ParseState(will.Connect.WillPayload)
		return wts != ts && ev[len(ev)-1] == "pub spBv1.0/STATE/mqttget "+string(sparkplug.StatePayload(true, wts))
	})
	_, will = rec.snapshot()
	_, ts, _ = sparkplug.ParseState(will.Connect.WillPayload)

	// A graceful shutdown publishes the STATE death.
	e.svc.Close()
	ev, _ = rec.snapshot()
	if ev[len(ev)-1] != "pub spBv1.0/STATE/mqttget "+string(sparkplug.StatePayload(false, ts)) {
		t.Fatalf("shutdown: %v", ev[len(ev)-3:])
	}
}
