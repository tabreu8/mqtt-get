// Package sparkplug runs mqtt-get's Sparkplug B support against an
// independent implementation: an edge node built on pysparkplug
// (edge_node.py), talking to mqtt-get through a real MQTT broker.
//
// It is skipped unless SPARKPLUG_INTEROP_PYTHON names a Python interpreter
// with pysparkplug installed:
//
//	python3 -m venv /tmp/spb && /tmp/spb/bin/pip install pysparkplug==0.6.1
//	SPARKPLUG_INTEROP_PYTHON=/tmp/spb/bin/python go test -v ./test/sparkplug
package sparkplug

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
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/httpapi"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/state"
)

const key = "interop-key"

type harness struct {
	t      *testing.T
	svc    *core.Service
	http   *httptest.Server
	broker string // host:port

	edge   *exec.Cmd
	stdin  io.WriteCloser
	events chan map[string]any
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newHarness(t *testing.T, client string, version uint) *harness {
	t.Helper()
	b := mochi.New(&mochi.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	_ = b.AddHook(new(auth.AllowHook), nil)
	addr := freeAddr(t)
	if err := b.AddListener(listeners.NewTCP(listeners.Config{ID: "tcp", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go func() { _ = b.Serve() }()
	t.Cleanup(func() {
		// mochi's Close can deadlock if a client disconnects while it runs
		// (its client list takes a read lock recursively): wait until the
		// clients are gone, and don't wait forever on Close.
		for deadline := time.Now().Add(5 * time.Second); b.Clients.Len() > 0 && time.Now().Before(deadline); {
			time.Sleep(10 * time.Millisecond)
		}
		done := make(chan struct{})
		go func() { _ = b.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Log("embedded broker did not close in time (known mochi issue); continuing")
		}
	})

	st, err := state.Open("")
	if err != nil {
		t.Fatal(err)
	}
	st.SetEnvKeys([]string{key}, nil, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("SPARKPLUG_INTEROP_LOG") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	svc := core.New(config.Server{MaxTopics: 100000, MaxBodyBytes: 1 << 20, PublishTimeout: 5 * time.Second}, log, st)
	svc.SetEnvSparkplug(config.Sparkplug{Enabled: true, Prefix: "spb", Rebirth: true, RebirthIntervalSec: 1, Commands: true})
	br := config.Broker{URLs: []string{"tcp://" + addr}, ClientID: "mqtt-get-interop", ProtocolVersion: version, Client: client}
	br.Normalize()
	if err := svc.Start(br, true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	hs := httptest.NewServer(httpapi.New(svc, nil).Handler())
	t.Cleanup(hs.Close)
	h := &harness{t: t, svc: svc, http: hs, broker: addr, events: make(chan map[string]any, 100)}
	h.until("broker connection", func() bool { return svc.MQTTStatus().Connected })
	time.Sleep(200 * time.Millisecond) // let the subscription settle
	return h
}

// startEdge launches the pysparkplug edge node and waits for its births.
func (h *harness) startEdge(python, edge string) map[string]any {
	h.t.Helper()
	host, port, _ := net.SplitHostPort(h.broker)
	cmd := exec.Command(python, "-I", "edge_node.py", host, port, "plant", edge)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		h.t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		h.t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	h.edge, h.stdin = cmd, stdin
	h.t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var ev map[string]any
			if json.Unmarshal(sc.Bytes(), &ev) == nil {
				h.events <- ev
			}
		}
	}()
	return h.event("born")
}

func (h *harness) send(cmd map[string]any) {
	h.t.Helper()
	b, _ := json.Marshal(cmd)
	if _, err := h.stdin.Write(append(b, '\n')); err != nil {
		h.t.Fatal(err)
	}
}

// event waits for the next edge-node event of the given kind.
func (h *harness) event(kind string) map[string]any {
	h.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev := <-h.events:
			if ev["event"] == "error" {
				h.t.Fatalf("edge node: %v", ev["error"])
			}
			if ev["event"] == kind {
				return ev
			}
		case <-deadline:
			h.t.Fatalf("no %q event from the edge node", kind)
		}
	}
}

func (h *harness) until(what string, f func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *harness) get(path string) (int, []byte) {
	h.t.Helper()
	req, _ := http.NewRequest("GET", h.http.URL+path, nil)
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (h *harness) post(path, body string) (int, string) {
	h.t.Helper()
	req, _ := http.NewRequest("POST", h.http.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// value returns the translated envelope of a metric topic.
type envelope struct {
	Value     json.RawMessage `json:"value"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Online    bool            `json:"online"`
}

func (h *harness) value(topic string) (envelope, bool) {
	h.t.Helper()
	code, b := h.get("/api/v1/values/" + topic)
	if code != 200 {
		return envelope{}, false
	}
	var v struct{ Payload envelope }
	if err := json.Unmarshal(b, &v); err != nil {
		h.t.Fatalf("%s: %v: %s", topic, err, b)
	}
	return v.Payload, true
}

func compact(t *testing.T, raw json.RawMessage) string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("invalid value %s", raw)
	}
	// Re-encode with exact integers (UseNumber) to compare 64-bit values.
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	_ = dec.Decode(&v)
	b, _ := json.Marshal(v)
	return string(b)
}

// Expected translations of the node metrics published by edge_node.py.
var nodeTypes = map[string]struct{ typ, value string }{
	"Types/Int8":      {"Int8", `-8`},
	"Types/Int16":     {"Int16", `-16000`},
	"Types/Int32":     {"Int32", `-2000000000`},
	"Types/Int64":     {"Int64", `-9000000000000000000`},
	"Types/UInt8":     {"UInt8", `250`},
	"Types/UInt16":    {"UInt16", `65000`},
	"Types/UInt32":    {"UInt32", `4000000000`},
	"Types/UInt64":    {"UInt64", `18000000000000000000`},
	"Types/Float":     {"Float", `1.5`},
	"Types/Double":    {"Double", `-2.125`},
	"Types/Boolean":   {"Boolean", `true`},
	"Types/String":    {"String", `"quote \" and ünïcode"`},
	"Types/Text":      {"Text", `"multi\nline"`},
	"Types/UUID":      {"UUID", `"123e4567-e89b-12d3-a456-426614174000"`},
	"Types/DateTime":  {"DateTime", `"2026-01-02T03:04:05Z"`},
	"Types/Bytes":     {"Bytes", `"AAH+/w=="`},
	"Types/Null":      {"Double", `null`},
	"Arrays/Int8":     {"Int8Array", `[-128,0,127]`},
	"Arrays/Int16":    {"Int16Array", `[-32768,32767]`},
	"Arrays/Int32":    {"Int32Array", `[-1,2,-3]`},
	"Arrays/Int64":    {"Int64Array", `[-5000000000,5000000000]`},
	"Arrays/UInt8":    {"UInt8Array", `[0,255]`},
	"Arrays/UInt16":   {"UInt16Array", `[1,65535]`},
	"Arrays/UInt32":   {"UInt32Array", `[4294967295]`},
	"Arrays/UInt64":   {"UInt64Array", `[18446744073709551615]`},
	"Arrays/Float":    {"FloatArray", `[0.5,-1.25]`},
	"Arrays/Double":   {"DoubleArray", `[3.5,-0.001]`},
	"Arrays/Boolean":  {"BooleanArray", `[true,false,true,true,false,false,false,false,true]`},
	"Arrays/String":   {"StringArray", `["a","","c d"]`},
	"Arrays/DateTime": {"DateTimeArray", `["2026-01-02T03:04:05Z"]`},
}

func TestInterop(t *testing.T) {
	python := os.Getenv("SPARKPLUG_INTEROP_PYTHON")
	if python == "" {
		t.Skip("set SPARKPLUG_INTEROP_PYTHON to a Python with pysparkplug installed")
	}
	for _, c := range []struct {
		name    string
		client  string
		version uint
	}{
		{"lean-mqtt311", config.ClientLean, 4},
		{"lean-mqtt5", config.ClientLean, 5},
		{"paho-mqtt311", config.ClientPaho, 4},
		{"paho-mqtt5", config.ClientPaho, 5},
	} {
		t.Run(c.name, func(t *testing.T) { run(t, python, c.client, c.version) })
	}
}

func run(t *testing.T, python, client string, version uint) {
	h := newHarness(t, client, version)
	edge := "gw-" + client + fmt.Sprint(version)
	prefix := "spb/plant/" + edge + "/"

	// A WebSocket subscriber sees the device's values as they change.
	wsURL := "ws" + strings.TrimPrefix(h.http.URL, "http") + "/api/v1/ws?filter=" + strings.ReplaceAll(prefix, "/", "%2F") + "press%2F%23"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": {"Bearer " + key}})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	var wsMu sync.Mutex
	wsSeen := map[string][]string{}
	wsOffline := map[string]bool{}
	go func() {
		for {
			var m struct {
				Type string
				Data struct {
					Topic   string
					Payload envelope
				}
			}
			if ws.ReadJSON(&m) != nil {
				return
			}
			if m.Type == "message" {
				wsMu.Lock()
				wsSeen[m.Data.Topic] = append(wsSeen[m.Data.Topic], string(m.Data.Payload.Value))
				if !m.Data.Payload.Online {
					wsOffline[m.Data.Topic] = true
				}
				wsMu.Unlock()
			}
		}
	}()

	born := h.startEdge(python, edge)
	t.Logf("edge node: %v", born)

	// 1. Every type pysparkplug encodes decodes to the expected JSON.
	h.until("node birth translated", func() bool { _, ok := h.value(prefix + "Arrays/DateTime"); return ok })
	for name, want := range nodeTypes {
		v, ok := h.value(prefix + name)
		if !ok {
			t.Errorf("%s: not translated", name)
			continue
		}
		if v.Type != want.typ || compact(t, v.Value) != compact(t, json.RawMessage(want.value)) || !v.Online {
			t.Errorf("%s: got %s %s online=%v, want %s %s", name, v.Type, v.Value, v.Online, want.typ, want.value)
		}
	}
	h.until("device birth translated", func() bool { _, ok := h.value(prefix + "press/Running"); return ok })
	if v, ok := h.value(prefix + "press/Setpoint"); !ok || v.Type != "Float" || string(v.Value) != "50" {
		t.Fatalf("device birth: %+v", v)
	}
	if _, ok := h.svc.Store().Get("spBv1.0/plant/NBIRTH/" + edge); ok {
		t.Error("raw message stored")
	}

	// 2. Data messages update values (and reach the WebSocket subscriber).
	h.send(map[string]any{"cmd": "update", "metric": "Temperature", "value": 21.75})
	h.until("device data", func() bool { v, _ := h.value(prefix + "press/Temperature"); return string(v.Value) == "21.75" })
	h.send(map[string]any{"cmd": "update", "metric": "Types/Int32", "value": -7})
	h.until("node data", func() bool { v, _ := h.value(prefix + "Types/Int32"); return string(v.Value) == "-7" })
	h.until("websocket delivery", func() bool {
		wsMu.Lock()
		defer wsMu.Unlock()
		vals := wsSeen[prefix+"press/Temperature"]
		return len(vals) > 0 && vals[len(vals)-1] == "21.75"
	})

	// 3. Writes become DCMDs that pysparkplug decodes with the right type,
	// and the device's confirmation updates the value.
	writes := []struct {
		metric, payload string
		datatype        float64
		want            any
	}{
		{"Setpoint", "61.5", 9, 61.5},
		{"Offset", "-7", 3, -7.0},
		{"Mode", "manual", 12, "manual"},
		{"Running", "true", 11, true},
	}
	for _, w := range writes {
		if code, out := h.post("/api/v1/publish/"+prefix+"press/"+w.metric, w.payload); code != 200 {
			t.Fatalf("write %s: %d %s", w.metric, code, out)
		}
		ev := h.event("cmd")
		ms := ev["metrics"].([]any)
		m := ms[0].(map[string]any)
		if !strings.HasSuffix(ev["topic"].(string), "/DCMD/"+edge+"/press") || len(ms) != 1 || m["name"] != w.metric ||
			m["datatype"] != w.datatype || m["value"] != w.want {
			t.Fatalf("DCMD for %s as decoded by pysparkplug: %v", w.metric, ev)
		}
	}
	h.until("write confirmed", func() bool { v, _ := h.value(prefix + "press/Running"); return string(v.Value) == "true" })

	// publish-and-wait returns the device's confirmation on the same topic.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := h.svc.PublishAndWait(ctx, mqttc.Message{Topic: prefix + "press/Setpoint", Payload: []byte("70.25")}, prefix+"press/Setpoint", 10*time.Second)
	if err != nil || resp == nil || !strings.Contains(string(resp.Payload), `"value":70.25`) {
		t.Fatalf("publish and wait: %v %v", resp, err)
	}
	h.event("cmd")

	// 4. A rebirth request is answered with new births.
	if code, out := h.post("/api/v1/sparkplug/nodes/plant/"+edge+"/rebirth", ""); code != 200 {
		t.Fatalf("rebirth: %d %s", code, out)
	}
	if ev := h.event("ncmd"); ev["metrics"].([]any)[0].(map[string]any)["name"] != "Node Control/Rebirth" {
		t.Fatalf("NCMD: %v", ev)
	}
	h.event("rebirth")
	h.until("rebirth processed", func() bool {
		code, b := h.get("/api/v1/sparkplug/nodes")
		return code == 200 && strings.Contains(string(b), `"edge_node":"`+edge+`"`) && strings.Contains(string(b), `"online":true`)
	})
	_, b := h.get("/api/v1/sparkplug")
	var view core.SparkplugView
	_ = json.Unmarshal(b, &view)
	if view.Stats == nil || view.Stats.SeqErrors != 0 || view.Stats.Malformed != 0 || view.Stats.Unknown != 0 {
		t.Fatalf("translator counters: %s", b)
	}

	// 5. Abrupt loss of the edge node: the broker publishes its will, the
	// NDEATH, and the values go offline (keeping the last value).
	h.send(map[string]any{"cmd": "kill"})
	h.until("node death", func() bool { v, ok := h.value(prefix + "press/Temperature"); return ok && !v.Online })
	if v, _ := h.value(prefix + "press/Temperature"); string(v.Value) != "21.75" {
		t.Fatalf("offline value: %+v", v)
	}
	if v, ok := h.value(prefix + "Types/Int8"); !ok || v.Online {
		t.Fatal("node metric still online after NDEATH")
	}
	h.until("websocket saw the death", func() bool {
		wsMu.Lock()
		defer wsMu.Unlock()
		return wsOffline[prefix+"press/Temperature"]
	})

	// 6. The node comes back (new session, new bdSeq) and then disconnects
	// cleanly. pysparkplug's clean NDEATH carries the next session's bdSeq;
	// it must still take the node offline.
	h.startEdge(python, edge)
	h.until("back online", func() bool { v, ok := h.value(prefix + "press/Temperature"); return ok && v.Online })
	h.send(map[string]any{"cmd": "disconnect"})
	h.event("disconnected")
	h.until("offline after a clean disconnect", func() bool { v, ok := h.value(prefix + "press/Temperature"); return ok && !v.Online })
}
