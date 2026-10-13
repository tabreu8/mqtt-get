package mcp

import (
	"strings"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/sparkplug"
	"github.com/tabreu8/mqtt-get/internal/store"
)

func TestSparkplugTools(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	a := newStdio(t, s, admin)
	a.initialize("2025-06-18")
	r := newStdio(t, s, reader)
	r.initialize("2025-06-18")

	tl := toolNames(t, r)
	if _, ok := tl["list_sparkplug_nodes"]; !ok {
		t.Fatal("list_sparkplug_nodes hidden from readers")
	}
	for _, name := range []string{"configure_sparkplug", "request_sparkplug_rebirth"} {
		if _, ok := tl[name]; ok {
			t.Fatalf("%s visible to readers", name)
		}
	}
	if _, isErr, text := r.tool("list_sparkplug_nodes", map[string]any{}); !isErr || !strings.Contains(text, "configure_sparkplug") {
		t.Fatalf("disabled: %s", text)
	}

	out := a.mustTool("configure_sparkplug", map[string]any{"enabled": true, "prefix": "uns/spb"})
	if cfg := out["config"].(map[string]any); cfg["enabled"] != true || cfg["prefix"] != "uns/spb" || cfg["rebirth_interval_sec"].(float64) != 30 {
		t.Fatalf("configure: %v", out)
	}
	// Only the given fields change.
	out = a.mustTool("configure_sparkplug", map[string]any{"commands": true})
	if cfg := out["config"].(map[string]any); cfg["prefix"] != "uns/spb" || cfg["commands"] != true {
		t.Fatalf("partial update: %v", out)
	}

	birth := sparkplug.Encode(&sparkplug.Payload{Seq: 0, HasSeq: true, Metrics: []sparkplug.Metric{
		{Name: "Level", Datatype: sparkplug.TypeDouble, Value: sparkplug.Value{Kind: sparkplug.KindDouble, F: 3.5}},
	}})
	svc.Ingest(store.Entry{Topic: "spBv1.0/site/DBIRTH/edge/tank", Payload: birth, Time: time.Now().UnixNano()})

	nodes := r.mustTool("list_sparkplug_nodes", map[string]any{"metrics": true})
	if nodes["count"].(float64) != 1 || nodes["prefix"] != "uns/spb" {
		t.Fatalf("nodes %v", nodes)
	}
	dev := nodes["nodes"].([]any)[0].(map[string]any)["devices"].([]any)[0].(map[string]any)
	if dev["id"] != "tank" || dev["metrics"].([]any)[0].(map[string]any)["topic"] != "uns/spb/site/edge/tank/Level" {
		t.Fatalf("device %v", dev)
	}
	v := r.mustTool("get_value", map[string]any{"topic": "uns/spb/site/edge/tank/Level"})
	if p := v["payload"].(map[string]any); p["value"] != 3.5 || p["type"] != "Double" {
		t.Fatalf("value %v", v)
	}
	// Not connected to a broker: rebirths and writes fail cleanly.
	if _, isErr, text := a.tool("request_sparkplug_rebirth", map[string]any{"group": "site", "edge_node": "edge"}); !isErr || !strings.Contains(text, "not connected") {
		t.Fatalf("rebirth offline: %s", text)
	}
	if _, isErr, text := a.tool("publish", map[string]any{"topic": "uns/spb/site/edge/tank/Level", "payload": "abc"}); !isErr || !strings.Contains(text, "not a number") {
		t.Fatalf("bad write: %s", text)
	}
}
