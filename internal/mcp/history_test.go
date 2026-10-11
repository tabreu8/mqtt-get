package mcp

import (
	"strings"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/store"
)

func TestHistoryTools(t *testing.T) {
	svc, _ := newService(t, false)
	s := New(svc, quiet(), Options{})
	defer s.Close()
	a := newStdio(t, s, admin)
	a.initialize("2025-06-18")
	r := newStdio(t, s, reader)
	r.initialize("2025-06-18")

	// Readers see the read tools only.
	tl := toolNames(t, r)
	if _, ok := tl["get_history"]; !ok {
		t.Fatal("get_history hidden from readers")
	}
	if _, ok := tl["create_history_rule"]; ok {
		t.Fatal("create_history_rule visible to readers")
	}
	if out := r.mustTool("list_history_topics", map[string]any{}); out["hint"] == nil {
		t.Fatalf("no hint without rules: %v", out)
	}
	if _, isErr, text := r.tool("get_history", map[string]any{"topic": "plant/temp"}); !isErr || !strings.Contains(text, "create_history_rule") {
		t.Fatalf("no rule: %v %s", isErr, text)
	}

	rule := a.mustTool("create_history_rule", map[string]any{"id": "plant", "topics": []string{"plant/#"}, "max_points": 500})
	if rule["id"] != "plant" || rule["max_points"].(float64) != 500 {
		t.Fatalf("rule %v", rule)
	}
	if _, isErr, text := r.tool("get_history", map[string]any{"topic": "plant/temp"}); !isErr || !strings.Contains(text, "no history recorded yet") {
		t.Fatalf("rule without data: %v %s", isErr, text)
	}

	now := time.Now()
	for i, v := range []string{`{"c":20}`, `{"c":22}`, `{"c":25}`, `{"c":21}`} {
		svc.Ingest(store.Entry{Topic: "plant/temp", Payload: []byte(v), Time: now.Add(time.Duration(i-4) * time.Minute).UnixNano()})
	}
	for i, v := range []string{"running", "running", "stopped", "running"} {
		svc.Ingest(store.Entry{Topic: "plant/state", Payload: []byte(v), Time: now.Add(time.Duration(i-4) * time.Minute).UnixNano()})
	}
	ingest(svc, "other/x", "1")

	out := r.mustTool("get_history", map[string]any{"topic": "plant/temp", "field": "c"})
	sum := out["summary"].(map[string]any)
	num := sum["numeric"].(map[string]any)
	if sum["count"].(float64) != 4 || num["min"].(float64) != 20 || num["max"].(float64) != 25 || num["avg"].(float64) != 22 || num["change"].(float64) != 1 {
		t.Fatalf("numeric summary %v", sum)
	}
	if b := sum["buckets"].([]any); len(b) != 4 || sum["bucket_seconds"].(float64) != 60 {
		t.Fatalf("buckets: %v", sum)
	}
	if out["rule"] != "plant" || out["recorded"].(map[string]any)["points"].(float64) != 4 {
		t.Fatalf("meta %v", out)
	}
	out = r.mustTool("get_history", map[string]any{"topic": "plant/temp", "field": "c", "since": "150s", "max_buckets": 0})
	if sum := out["summary"].(map[string]any); sum["count"].(float64) != 2 || sum["buckets"] != nil {
		t.Fatalf("since 150s: %v", sum)
	}

	out = r.mustTool("get_history", map[string]any{"topic": "plant/state", "since": "all"})
	sum = out["summary"].(map[string]any)
	if sum["numeric"] != nil || len(sum["changes"].([]any)) != 3 || sum["distinct"].([]any)[0].(map[string]any)["value"] != "running" {
		t.Fatalf("status summary %v", sum)
	}

	out = r.mustTool("get_history", map[string]any{"topic": "plant/temp", "mode": "points", "limit": 2})
	pts := out["points"].([]any)
	if out["total"].(float64) != 4 || len(pts) != 2 || pts[1].(map[string]any)["payload"].(map[string]any)["c"].(float64) != 21 {
		t.Fatalf("points %v", out)
	}

	lst := r.mustTool("list_history_topics", map[string]any{"filter": "plant/#"})
	if lst["total"].(float64) != 2 || lst["topics"].([]any)[0].(map[string]any)["topic"] != "plant/state" {
		t.Fatalf("list %v", lst)
	}

	if _, isErr, text := r.tool("get_history", map[string]any{"topic": "plant/+"}); !isErr || !strings.Contains(text, "wildcards") {
		t.Fatalf("wildcard: %s", text)
	}
	if _, isErr, _ := r.tool("get_history", map[string]any{"topic": "plant/temp", "since": "yesterday"}); !isErr {
		t.Fatal("bad since accepted")
	}

	// Lowering max_points trims; deleting the rule forgets the history.
	a.mustTool("update_history_rule", map[string]any{"id": "plant", "max_points": 1})
	if out := r.mustTool("get_history", map[string]any{"topic": "plant/temp", "mode": "points", "since": "all"}); out["total"].(float64) != 1 {
		t.Fatalf("after update %v", out)
	}
	rules := a.mustTool("list_history_rules", map[string]any{})
	if st := rules["rules"].([]any)[0].(map[string]any)["stats"].(map[string]any); st["topics"].(float64) != 2 || st["points"].(float64) != 2 {
		t.Fatalf("rule stats %v", rules)
	}
	a.mustTool("clear_history", map[string]any{"topic": "plant/state"})
	a.mustTool("delete_history_rule", map[string]any{"id": "plant"})
	if st := svc.HistoryStats(); st.Topics != 0 || st.Rules != 0 {
		t.Fatalf("after delete %+v", st)
	}
}
