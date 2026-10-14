package history

import (
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

const sec = int64(time.Second)

type clock struct{ t int64 }

func (c *clock) now() time.Time { return time.Unix(0, c.t) }

func newTest(maxBytes int64, rules ...config.HistoryRule) (*History, *clock) {
	c := &clock{t: 1000 * sec}
	h := New(Options{MaxBytes: maxBytes, Now: c.now})
	h.SetRules(rules)
	return h, c
}

func add(h *History, topic, payload string, t int64) {
	h.Add(&store.Entry{Topic: topic, Payload: []byte(payload), Time: t})
}

func payloads(pts []Point) []string {
	out := make([]string, len(pts))
	for i, p := range pts {
		out[i] = string(p.Payload)
	}
	return out
}

func eq(t *testing.T, got []string, want ...string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// checkAccounting verifies the global and per-rule counters against the
// actual contents.
func checkAccounting(t *testing.T, h *History) {
	t.Helper()
	var topics, points, bytes int64
	for i := range h.shards {
		sh := &h.shards[i]
		sh.mu.RLock()
		for name, s := range sh.m {
			s.mu.Lock()
			topics++
			points += int64(s.n)
			var b int64
			for k := 0; k < s.n; k++ {
				b += int64(s.at(k).n) + int64(len(name)) + pointOverhead
			}
			if b != s.bytes {
				t.Fatalf("%s: series bytes %d, actual %d", name, s.bytes, b)
			}
			bytes += b + seriesOverhead + int64(len(name))
			s.mu.Unlock()
		}
		sh.mu.RUnlock()
	}
	st := h.Stats()
	if st.Topics != topics || st.Points != points || st.Bytes != bytes {
		t.Fatalf("stats %+v, actual topics=%d points=%d bytes=%d", st, topics, points, bytes)
	}
	var rt, rp, rb int64
	for _, r := range h.Rules() {
		rt, rp, rb = rt+r.Topics, rp+r.Points, rb+r.Bytes
	}
	if rt != topics || rp != points || rb != bytes {
		t.Fatalf("rule totals topics=%d points=%d bytes=%d, actual %d %d %d", rt, rp, rb, topics, points, bytes)
	}
}

func TestNoRulesRecordsNothing(t *testing.T) {
	h, _ := newTest(0)
	add(h, "a", "1", 1)
	if st := h.Stats(); st.Topics != 0 || st.Points != 0 {
		t.Fatalf("%+v", st)
	}
	if _, _, _, ok := h.Get("a", 0, 0, 0); ok {
		t.Fatal("unexpected history")
	}
}

func TestRingKeepsNewestPoints(t *testing.T) {
	h, _ := newTest(0, config.HistoryRule{ID: "r", Topics: []string{"s/#"}, MaxPoints: 3})
	for i := 1; i <= 20; i++ {
		add(h, "s/a", strconv.Itoa(i), int64(i)*sec)
	}
	add(h, "other", "x", 1)
	pts, total, info, ok := h.Get("s/a", 0, 0, 0)
	if !ok || total != 3 || info.Rule != "r" || info.Points != 3 {
		t.Fatalf("ok=%v total=%d info=%+v", ok, total, info)
	}
	eq(t, payloads(pts), "18", "19", "20")
	if _, _, _, ok := h.Get("other", 0, 0, 0); ok {
		t.Fatal("unmatched topic recorded")
	}
	checkAccounting(t, h)
}

func TestRangeAndLimit(t *testing.T) {
	h, _ := newTest(0, config.HistoryRule{ID: "r", Topics: []string{"#"}, MaxPoints: 100})
	for i := 1; i <= 10; i++ {
		add(h, "t", strconv.Itoa(i), int64(i)*sec)
	}
	pts, total, _, _ := h.Get("t", 3*sec, 7*sec, 0)
	if total != 5 {
		t.Fatalf("total %d", total)
	}
	eq(t, payloads(pts), "3", "4", "5", "6", "7")
	pts, total, _, _ = h.Get("t", 3*sec, 7*sec, 2) // newest 2 of the range
	if total != 5 {
		t.Fatalf("total %d", total)
	}
	eq(t, payloads(pts), "6", "7")
	pts, total, _, _ = h.Get("t", 20*sec, 0, 0)
	if total != 0 || len(pts) != 0 || pts == nil {
		t.Fatalf("empty range: %v %d", pts, total)
	}
}

func TestMaxAge(t *testing.T) {
	h, c := newTest(0, config.HistoryRule{ID: "r", Topics: []string{"#"}, MaxAgeSec: 10})
	for i := 0; i < 20; i++ {
		add(h, "t", strconv.Itoa(i), c.t+int64(i)*sec)
	}
	// Appending trims to the window relative to the newest message.
	if pts, _, _, _ := h.Get("t", 0, 0, 0); len(pts) != 11 {
		t.Fatalf("after append: %v", payloads(pts))
	}
	c.t += 25 * sec // reads apply the window relative to now
	pts, _, _, _ := h.Get("t", 0, 0, 0)
	eq(t, payloads(pts), "15", "16", "17", "18", "19")
	if l, _ := h.List("", 0); len(l) != 1 || l[0].Points != 5 {
		t.Fatalf("list %+v", l)
	}
	c.t += 100 * sec
	h.Sweep()
	if st := h.Stats(); st.Topics != 0 || st.Points != 0 || st.Bytes != 0 {
		t.Fatalf("after sweep: %+v", st)
	}
	checkAccounting(t, h)
}

func TestMinIntervalAndOnChange(t *testing.T) {
	h, _ := newTest(0,
		config.HistoryRule{ID: "slow", Topics: []string{"fast"}, MinIntervalMs: 1000},
		config.HistoryRule{ID: "chg", Topics: []string{"state"}, OnChange: true})
	for i := 0; i < 50; i++ { // 10 Hz for 5 s
		add(h, "fast", strconv.Itoa(i), int64(i)*sec/10)
	}
	pts, _, _, _ := h.Get("fast", 0, 0, 0)
	eq(t, payloads(pts), "0", "10", "20", "30", "40")
	for i, v := range []string{"on", "on", "off", "off", "off", "on"} {
		add(h, "state", v, int64(i)*sec)
	}
	pts, _, _, _ = h.Get("state", 0, 0, 0)
	eq(t, payloads(pts), "on", "off", "on")
}

func TestFirstMatchingRuleWins(t *testing.T) {
	h, _ := newTest(0,
		config.HistoryRule{ID: "specific", Topics: []string{"a/b"}, MaxPoints: 2},
		config.HistoryRule{ID: "broad", Topics: []string{"a/#"}, MaxPoints: 10})
	if h.RuleFor("a/b") != "specific" || h.RuleFor("a/c") != "broad" || h.RuleFor("z") != "" {
		t.Fatal("RuleFor")
	}
	// Order, not specificity, decides.
	h.SetRules([]config.HistoryRule{
		{ID: "broad", Topics: []string{"a/#"}, MaxPoints: 10},
		{ID: "specific", Topics: []string{"a/b"}, MaxPoints: 2}})
	if h.RuleFor("a/b") != "broad" {
		t.Fatal("order")
	}
	disabled := false
	h.SetRules([]config.HistoryRule{{ID: "off", Topics: []string{"#"}, Enabled: &disabled}})
	if h.RuleFor("a") != "" || h.Stats().Rules != 0 {
		t.Fatal("disabled rule applies")
	}
}

func TestSetRulesMovesAndForgets(t *testing.T) {
	h, _ := newTest(0, config.HistoryRule{ID: "a", Topics: []string{"x/#"}, MaxPoints: 100})
	for i := 0; i < 50; i++ {
		add(h, "x/1", strconv.Itoa(i), int64(i))
		add(h, "x/2", strconv.Itoa(i), int64(i))
	}
	// x/1 moves to a smaller rule and is trimmed; x/2 no longer matches.
	h.SetRules([]config.HistoryRule{{ID: "b", Topics: []string{"x/1"}, MaxPoints: 5}})
	pts, _, info, ok := h.Get("x/1", 0, 0, 0)
	if !ok || info.Rule != "b" {
		t.Fatalf("x/1: %v %+v", ok, info)
	}
	eq(t, payloads(pts), "45", "46", "47", "48", "49")
	if _, _, _, ok := h.Get("x/2", 0, 0, 0); ok {
		t.Fatal("x/2 kept")
	}
	add(h, "x/1", "50", 50) // the trimmed ring keeps working
	pts, _, _, _ = h.Get("x/1", 0, 0, 0)
	eq(t, payloads(pts), "46", "47", "48", "49", "50")
	checkAccounting(t, h)
	if r := h.Rules()["b"]; r.Topics != 1 || r.Points != 5 {
		t.Fatalf("rule stats %+v", r)
	}
}

func TestGlobalByteBudget(t *testing.T) {
	payload := string(make([]byte, 100))
	h, _ := newTest(10_000, config.HistoryRule{ID: "r", Topics: []string{"#"}, MaxPoints: 1000})
	for i := 0; i < 2000; i++ {
		add(h, "noisy", payload, int64(i))
	}
	st := h.Stats()
	if st.Bytes > 10_000 {
		t.Fatalf("over budget: %+v", st)
	}
	n := st.Points
	if n < 50 {
		t.Fatalf("budget not used: %+v", st)
	}
	// A new topic can't start while the budget is exhausted, and the noisy
	// topic stays bounded by evicting its own points.
	add(h, "quiet", "1", 1)
	for i := 0; i < 100; i++ {
		add(h, "noisy", payload, int64(3000+i))
	}
	if st := h.Stats(); st.Bytes > 10_000 || st.Points != n || st.Dropped == 0 {
		t.Fatalf("after: %+v (points before %d)", st, n)
	}
	pts, _, _, _ := h.Get("noisy", 0, 0, 1)
	if pts[0].Time != 3099 {
		t.Fatalf("newest point not kept: %d", pts[0].Time)
	}
	checkAccounting(t, h)
}

func TestRuleByteBudget(t *testing.T) {
	payload := string(make([]byte, 100))
	h, _ := newTest(0,
		config.HistoryRule{ID: "small", Topics: []string{"s/#"}, MaxBytes: 5000},
		config.HistoryRule{ID: "big", Topics: []string{"b/#"}})
	for i := 0; i < 500; i++ {
		add(h, "s/"+strconv.Itoa(i%5), payload, int64(i))
		add(h, "b/x", payload, int64(i))
	}
	r := h.Rules()
	if r["small"].Bytes > 5000 || r["small"].Points == 0 || r["small"].Topics != 5 {
		t.Fatalf("small: %+v", r["small"])
	}
	if r["big"].Points != 500 {
		t.Fatalf("big: %+v", r["big"])
	}
	checkAccounting(t, h)
}

func TestDeleteAndClear(t *testing.T) {
	h, _ := newTest(0, config.HistoryRule{ID: "r", Topics: []string{"#"}})
	add(h, "a", "1", 1)
	add(h, "b", "1", 1)
	if !h.Delete("a") || h.Delete("a") {
		t.Fatal("delete")
	}
	h.Clear()
	if st := h.Stats(); st.Topics != 0 || st.Bytes != 0 {
		t.Fatalf("%+v", st)
	}
	add(h, "a", "2", 2) // recording resumes
	if l, total := h.List("#", 0); total != 1 || l[0].Topic != "a" {
		t.Fatalf("%+v", l)
	}
}

func TestList(t *testing.T) {
	h, _ := newTest(0, config.HistoryRule{ID: "r", Topics: []string{"#"}})
	for _, name := range []string{"home/b/t", "home/a/t", "car/x"} {
		add(h, name, "1", 1)
		add(h, name, "2", 2)
	}
	l, total := h.List("home/+/t", 1)
	if total != 2 || len(l) != 1 || l[0].Topic != "home/a/t" || l[0].Points != 2 || l[0].Oldest != 1 || l[0].Newest != 2 {
		t.Fatalf("%d %+v", total, l)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.bin")
	h, _ := newTest(0, config.HistoryRule{ID: "r", Topics: []string{"#"}, MaxPoints: 50})
	bin := string([]byte{0, 1, 2, 255})
	for i := 0; i < 100; i++ {
		h.Add(&store.Entry{Topic: "t/" + strconv.Itoa(i%3), Payload: []byte(strconv.Itoa(i)), Time: int64(i) * sec, QoS: 1, Retained: i%2 == 0})
	}
	add(h, "bin", bin, 5)
	if err := h.Save(path); err != nil {
		t.Fatal(err)
	}

	// Restored with a rule that keeps fewer points and only some topics.
	h2, _ := newTest(0, config.HistoryRule{ID: "r2", Topics: []string{"t/1", "bin"}, MaxPoints: 3})
	if err := h2.Load(path); err != nil {
		t.Fatal(err)
	}
	pts, _, info, ok := h2.Get("t/1", 0, 0, 0)
	if !ok || info.Rule != "r2" {
		t.Fatal("t/1 not restored")
	}
	eq(t, payloads(pts), "91", "94", "97")
	if pts[0].QoS != 1 || pts[0].Retained != false || pts[1].Retained != true || pts[2].Time != 97*sec {
		t.Fatalf("metadata: %+v", pts)
	}
	if pts, _, _, _ := h2.Get("bin", 0, 0, 0); string(pts[0].Payload) != bin {
		t.Fatal("binary payload")
	}
	if _, _, _, ok := h2.Get("t/0", 0, 0, 0); ok {
		t.Fatal("unmatched topic restored")
	}
	checkAccounting(t, h2)

	if err := New(Options{}).Load(filepath.Join(t.TempDir(), "missing")); err != nil {
		t.Fatal("missing file must not be an error")
	}
}

func TestConcurrentAddAndRuleChanges(t *testing.T) {
	h, _ := newTest(50_000, config.HistoryRule{ID: "r", Topics: []string{"#"}, MaxPoints: 20})
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				add(h, "t/"+strconv.Itoa(i%50), "payload", int64(i))
				if i%100 == 0 {
					h.Get("t/1", 0, 0, 5)
					h.List("#", 10)
				}
			}
		}(g)
	}
	for i := 0; i < 50; i++ {
		h.SetRules([]config.HistoryRule{{ID: strconv.Itoa(i), Topics: []string{"t/" + strconv.Itoa(i%3) + "#", "t/+"}, MaxPoints: 5 + i%10}})
		if i%10 == 0 {
			h.Sweep()
			h.Delete("t/7")
		}
	}
	close(stop)
	wg.Wait()
	checkAccounting(t, h)
	if st := h.Stats(); st.Bytes > 50_000 {
		t.Fatalf("over budget: %+v", st)
	}
}

func BenchmarkAdd(b *testing.B) {
	h := New(Options{MaxBytes: 1 << 30})
	h.SetRules([]config.HistoryRule{{ID: "r", Topics: []string{"devices/#"}, MaxPoints: 100}})
	topics := make([]string, 10000)
	for i := range topics {
		topics[i] = "devices/" + strconv.Itoa(i) + "/state"
	}
	p := []byte(`{"on":true}`)
	for i := 0; i < 100*len(topics); i++ { // fill the rings
		h.Add(&store.Entry{Topic: topics[i%len(topics)], Payload: p, Time: int64(i)})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Add(&store.Entry{Topic: topics[i%len(topics)], Payload: p, Time: int64(i)})
	}
}

func BenchmarkAddUnmatched(b *testing.B) {
	h := New(Options{})
	h.SetRules([]config.HistoryRule{{ID: "r", Topics: []string{"other/#"}}})
	p := []byte(`{"on":true}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Add(&store.Entry{Topic: "devices/1/state", Payload: p, Time: int64(i)})
	}
}

func TestAddRef(t *testing.T) {
	h, _ := newTest(0, config.HistoryRule{ID: "a", Topics: []string{"x/#"}, MaxPoints: 10})
	var ref Ref
	addRef := func(v string, ts int64) {
		h.AddRef(&store.Entry{Topic: "x/1", Payload: []byte(v), Time: ts}, &ref)
	}
	get := func() []string {
		pts, _, _, _ := h.Get("x/1", 0, 0, 0)
		return payloads(pts)
	}
	addRef("1", 1*sec)
	addRef("2", 2*sec)
	eq(t, get(), "1", "2")
	if ref.s.Load() == nil {
		t.Fatal("series not cached")
	}
	// Removed (delete, rule change): looked up again.
	h.Delete("x/1")
	addRef("3", 3*sec)
	eq(t, get(), "3")
	h.SetRules([]config.HistoryRule{{ID: "b", Topics: []string{"y/#"}}})
	addRef("4", 4*sec)
	if _, _, _, ok := h.Get("x/1", 0, 0, 0); ok {
		t.Fatal("recorded without a matching rule")
	}
	h.SetRules([]config.HistoryRule{{ID: "c", Topics: []string{"x/1"}, MaxPoints: 3}})
	addRef("5", 5*sec)
	addRef("6", 6*sec)
	eq(t, get(), "5", "6")
	if st := h.Stats(); st.Points != 2 || st.Topics != 1 {
		t.Fatalf("stats %+v", st)
	}
}
