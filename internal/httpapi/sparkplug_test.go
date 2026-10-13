package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/gorilla/websocket"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/sparkplug"
)

// edgeNode plays a Sparkplug B edge node with one device on the embedded
// broker: it answers rebirth requests and applies DCMD writes.
type edgeNode struct {
	t      *testing.T
	b      *mochi.Server
	mu     sync.Mutex
	seq    uint64
	setp   float64
	cmds   []sparkplug.Metric
	births int
}

func (n *edgeNode) next() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.seq % 256
	n.seq++
	return s
}

func (n *edgeNode) publish(typ, dev string, metrics ...sparkplug.Metric) {
	n.t.Helper()
	topic := "spBv1.0/plant/" + typ + "/gw1"
	if dev != "" {
		topic += "/" + dev
	}
	p := &sparkplug.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Metrics: metrics}
	if typ != sparkplug.NDEATH {
		p.Seq, p.HasSeq = n.next(), true
	}
	if err := n.b.Publish(topic, sparkplug.Encode(p), false, 0); err != nil {
		n.t.Fatal(err)
	}
}

func (n *edgeNode) birth() {
	n.mu.Lock()
	n.seq, n.births = 0, n.births+1
	setp := n.setp
	n.mu.Unlock()
	n.publish(sparkplug.NBIRTH, "",
		sparkplug.Metric{Name: "bdSeq", Datatype: sparkplug.TypeUInt64, Value: sparkplug.Value{Kind: sparkplug.KindLong, U: 1}},
		sparkplug.Metric{Name: sparkplug.RebirthMetric, Datatype: sparkplug.TypeBoolean, Value: sparkplug.Value{Kind: sparkplug.KindBool}})
	n.publish(sparkplug.DBIRTH, "press",
		sparkplug.Metric{Name: "Temperature", Alias: 1, HasAlias: true, Datatype: sparkplug.TypeDouble, Value: sparkplug.Value{Kind: sparkplug.KindDouble, F: 20},
			Properties: &sparkplug.PropertySet{Keys: []string{"engUnit"}, Values: []sparkplug.PropertyValue{{Type: sparkplug.TypeString, Value: sparkplug.Value{Kind: sparkplug.KindString, S: "C"}}}}},
		sparkplug.Metric{Name: "Setpoint", Alias: 2, HasAlias: true, Datatype: sparkplug.TypeFloat, Value: sparkplug.Value{Kind: sparkplug.KindFloat, F: setp}})
}

func (n *edgeNode) temperature(v float64) {
	n.publish(sparkplug.DDATA, "press", sparkplug.Metric{Alias: 1, HasAlias: true, Value: sparkplug.Value{Kind: sparkplug.KindDouble, F: v}})
}

func (n *edgeNode) listen() {
	handle := func(_ *mochi.Client, _ packets.Subscription, pk packets.Packet) {
		p, err := sparkplug.Decode(pk.Payload)
		if err != nil {
			n.t.Errorf("edge node got an invalid command on %s: %v", pk.TopicName, err)
			return
		}
		for _, m := range p.Metrics {
			n.mu.Lock()
			n.cmds = append(n.cmds, m)
			n.mu.Unlock()
			switch {
			case m.Name == sparkplug.RebirthMetric && m.Value.U == 1:
				go n.birth()
			case m.Name == "Setpoint":
				n.mu.Lock()
				n.setp = m.Value.F
				n.mu.Unlock()
				go n.publish(sparkplug.DDATA, "press", sparkplug.Metric{Alias: 2, HasAlias: true, Value: m.Value})
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

func (n *edgeNode) commands() []sparkplug.Metric {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]sparkplug.Metric(nil), n.cmds...)
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
			if m.Name == "Setpoint" && m.Datatype == sparkplug.TypeFloat && m.Value.Kind == sparkplug.KindFloat && m.Value.F == 61.5 {
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
	edge.publish(sparkplug.NDEATH, "", sparkplug.Metric{Name: "bdSeq", Value: sparkplug.Value{Kind: sparkplug.KindLong, U: 1}})
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
