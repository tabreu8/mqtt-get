package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
)

func TestHistoryEndToEnd(t *testing.T) {
	e := setup(t, "secret")
	e.waitConnected()

	// Rules are admin-only; reads need the read scope.
	if code, _, _ := e.do("POST", "/api/v1/history-rules", "read-key", `{"topics":["#"]}`); code != 403 {
		t.Fatalf("read key creating a rule: %d", code)
	}
	if code, out, _ := e.do("POST", "/api/v1/history-rules", e.admin, `{"topics":["bad/#/x"]}`); code != 400 {
		t.Fatalf("invalid filter: %d %s", code, out)
	}
	code, out, _ := e.do("POST", "/api/v1/history-rules", e.admin, `{"id":"sensors","topics":["sensors/#"],"max_points":3}`)
	if code != 201 || !strings.Contains(out, `"max_points":3`) {
		t.Fatalf("create: %d %s", code, out)
	}
	if code, _, _ := e.do("POST", "/api/v1/history-rules", e.admin, `{"id":"sensors","topics":["x"]}`); code != 409 {
		t.Fatalf("duplicate id: %d", code)
	}
	if code, out, _ := e.do("GET", "/api/v1/history/sensors/t", "read-key", ""); code != 404 || !strings.Contains(out, "no history recorded yet") {
		t.Fatalf("before data: %d %s", code, out)
	}
	if code, out, _ := e.do("GET", "/api/v1/history/other", "read-key", ""); code != 404 || !strings.Contains(out, "no history rule matches") {
		t.Fatalf("no rule: %d %s", code, out)
	}

	// Messages published through the broker are recorded.
	for _, v := range []string{"20.5", "21", "22", "23.5"} {
		if code, out, _ := e.do("POST", "/api/v1/publish/sensors/t?qos=1", e.admin, v); code != 200 {
			t.Fatalf("publish: %d %s", code, out)
		}
	}
	eventually(t, func() bool { return e.svc.HistoryStats().Points == 3 })

	code, out, _ = e.do("GET", "/api/v1/history/sensors/t?since=1h", "read-key", "")
	var pts struct {
		Topic, Rule  string
		Total, Count int
		Points       []struct {
			Payload   any
			Encoding  string
			Timestamp time.Time
		}
	}
	if code != 200 || json.Unmarshal([]byte(out), &pts) != nil {
		t.Fatalf("points: %d %s", code, out)
	}
	if pts.Rule != "sensors" || pts.Total != 3 || pts.Points[0].Payload != 21.0 || pts.Points[2].Payload != 23.5 || pts.Points[0].Encoding != "json" {
		t.Fatalf("points: %s", out)
	}
	if _, out, _ := e.do("GET", "/api/v1/history?topic=sensors/t&limit=1", "read-key", ""); !strings.Contains(out, `"total":3,"count":1`) {
		t.Fatalf("limit: %s", out)
	}

	code, out, _ = e.do("GET", "/api/v1/history/sensors/t?summary=true", "read-key", "")
	if code != 200 || !strings.Contains(out, `"min":21`) || !strings.Contains(out, `"max":23.5`) || !strings.Contains(out, `"rule":"sensors"`) {
		t.Fatalf("summary: %d %s", code, out)
	}
	code, out, _ = e.do("GET", "/api/v1/history/sensors/t?bucket=1h&format=csv", "read-key", "")
	if code != 200 || !strings.HasPrefix(out, "start,count,min,max,avg,first,last\n") || !strings.Contains(out, ",3,21,23.5,") {
		t.Fatalf("csv buckets: %d %s", code, out)
	}
	if _, out, _ := e.do("GET", "/api/v1/history/sensors/t?format=csv", "read-key", ""); strings.Count(out, "\n") != 4 {
		t.Fatalf("csv points: %s", out)
	}
	for _, bad := range []string{"since=yesterday", "bucket=-5m", "since=1h&until=2h"} {
		if code, out, _ := e.do("GET", "/api/v1/history/sensors/t?"+bad, "read-key", ""); code != 400 {
			t.Fatalf("%s: %d %s", bad, code, out)
		}
	}

	if _, out, _ := e.do("GET", "/api/v1/history?filter=sensors/%23", "read-key", ""); !strings.Contains(out, `"topic":"sensors/t"`) || !strings.Contains(out, `"points":3`) {
		t.Fatalf("list: %s", out)
	}
	if _, out, _ := e.do("GET", "/api/v1/status", "read-key", ""); !strings.Contains(out, `"history":{"rules":1,"topics":1,"points":3`) {
		t.Fatalf("status: %s", out)
	}
	if _, out, _ := e.do("GET", "/metrics", "read-key", ""); !strings.Contains(out, "mqttget_history_points 3") {
		t.Fatalf("metrics: %s", out)
	}

	// Update (partial updates go through PUT with the full rule), then delete.
	if code, out, _ := e.do("PUT", "/api/v1/history-rules/sensors", e.admin, `{"topics":["sensors/#"],"max_points":1}`); code != 200 {
		t.Fatalf("update: %d %s", code, out)
	}
	if e.svc.HistoryStats().Points != 1 {
		t.Fatal("update did not trim")
	}
	if code, _, _ := e.do("DELETE", "/api/v1/history/sensors/t", "read-key", ""); code != 403 {
		t.Fatal("read key deleting history")
	}
	if code, _, _ := e.do("DELETE", "/api/v1/history/sensors/t", e.admin, ""); code != 204 {
		t.Fatal("delete history")
	}
	if code, _, _ := e.do("DELETE", "/api/v1/history-rules/sensors", e.admin, ""); code != 204 {
		t.Fatal("delete rule")
	}
	if code, _, _ := e.do("GET", "/api/v1/history-rules/sensors", e.admin, ""); code != 404 {
		t.Fatal("rule still there")
	}
	if len(e.st.HistoryRules()) != 0 {
		t.Fatal("rule still persisted")
	}
}

func TestHistorySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Server{DataDir: dir, HistoryPersist: true, HistorySnapshotSec: 300, MaxTopics: 100}
	start := func() *core.Service {
		st, err := state.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		svc := core.New(cfg, log, st)
		svc.SetEnvHistoryRule(config.HistoryRule{ID: "env", Topics: []string{"env/#"}, MaxPoints: 10, Env: true})
		if err := svc.Start(config.Broker{}, false); err != nil {
			t.Fatal(err)
		}
		return svc
	}
	svc := start()
	if _, err := svc.PutHistoryRule(config.HistoryRule{ID: "r", Topics: []string{"a/#"}}, true); err != nil {
		t.Fatal(err)
	}
	// The environment rule comes first and is read-only.
	if rules := svc.HistoryRules(); len(rules) != 2 || rules[0].ID != "env" || !rules[0].Env {
		t.Fatalf("rules %+v", rules)
	}
	if _, err := svc.PutHistoryRule(config.HistoryRule{ID: "env", Topics: []string{"#"}}, false); core.KindOf(err) != core.Conflict {
		t.Fatalf("env rule changed: %v", err)
	}
	if err := svc.DeleteHistoryRule("env"); core.KindOf(err) != core.Conflict {
		t.Fatalf("env rule deleted: %v", err)
	}
	now := time.Now().UnixNano()
	for i := 0; i < 5; i++ {
		svc.Ingest(store.Entry{Topic: "a/1", Payload: []byte{'0' + byte(i)}, Time: now + int64(i)})
		svc.Ingest(store.Entry{Topic: "env/x", Payload: []byte("e"), Time: now + int64(i)})
	}
	svc.Close() // saves the snapshot

	svc = start()
	defer svc.Close()
	res, err := svc.History("a/1", core.HistoryQuery{})
	if err != nil || res.Total != 5 || string(res.Points[4].Payload) != "4" {
		t.Fatalf("after restart: %v %+v", err, res)
	}
	if res, err := svc.History("env/x", core.HistoryQuery{}); err != nil || res.Total != 5 {
		t.Fatalf("env topic after restart: %v", err)
	}
}

func BenchmarkIngestWithHistory(b *testing.B) {
	st, _ := state.Open("")
	svc := core.New(config.Server{}, slog.New(slog.NewTextHandler(io.Discard, nil)), st)
	if _, err := svc.PutHistoryRule(config.HistoryRule{ID: "h", Topics: []string{"devices/#"}, MaxPoints: 100}, true); err != nil {
		b.Fatal(err)
	}
	topics := make([]string, 10000)
	for i := range topics {
		topics[i] = "devices/" + strconv.Itoa(i) + "/state"
	}
	p := []byte(`{"on":true}`)
	for i := 0; i < 100*len(topics); i++ {
		svc.Ingest(store.Entry{Topic: topics[i%len(topics)], Payload: p, Time: int64(i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		svc.Ingest(store.Entry{Topic: topics[i%len(topics)], Payload: p, Time: int64(i)})
	}
}
