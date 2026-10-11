package history

import (
	"testing"
	"time"
)

func pts(t0 int64, step time.Duration, payloads ...string) []Point {
	out := make([]Point, len(payloads))
	for i, p := range payloads {
		out[i] = Point{Payload: []byte(p), Time: t0 + int64(i)*int64(step)}
	}
	return out
}

func TestNumber(t *testing.T) {
	cases := []struct {
		payload, field string
		want           float64
		ok             bool
	}{
		{"23.5", "", 23.5, true},
		{" 7 ", "", 7, true},
		{`"12"`, "", 12, true},
		{"true", "", 1, true},
		{"ON", "", 0, false},
		{`{"celsius":21.4}`, "celsius", 21.4, true},
		{`{"data":{"values":[1,2,3]}}`, "data.values.2", 3, true},
		{`{"data":{"values":[1,2,3]}}`, "data.values.9", 0, false},
		{`{"celsius":"19.5"}`, "celsius", 19.5, true},
		{`{"celsius":21.4}`, "missing", 0, false},
		{`{"state":"running"}`, "state", 0, false},
		{"NaN", "", 0, false},
	}
	for _, c := range cases {
		got, ok := Number([]byte(c.payload), c.field)
		if ok != c.ok || got != c.want {
			t.Errorf("Number(%q, %q) = %v, %v; want %v, %v", c.payload, c.field, got, ok, c.want, c.ok)
		}
	}
}

func TestSummarizeNumeric(t *testing.T) {
	t0 := int64(1_699_999_980) * int64(time.Second) // a multiple of 1m
	p := pts(t0, 20*time.Second, `{"t":10}`, `{"t":14}`, `{"t":12}`, `{"t":20}`, `{"t":8}`, `{"h":1}`)
	s := Summarize(p, "t", time.Minute)
	n := s.Numeric
	if s.Count != 6 || n == nil || n.Count != 5 || n.Min != 8 || n.Max != 20 || n.Avg != 12.8 || n.First != 10 || n.Last != 8 || n.Change != -2 {
		t.Fatalf("%+v %+v", s, n)
	}
	if !n.MaxAt.Equal(time.Unix(0, t0+60*int64(time.Second))) || s.Missing != 1 || s.NonNumeric != 0 {
		t.Fatalf("max_at %v missing %d non-numeric %d", n.MaxAt, s.Missing, s.NonNumeric)
	}
	if len(s.Buckets) != 2 || s.BucketSec != 60 {
		t.Fatalf("buckets %+v", s.Buckets)
	}
	b0, b1 := s.Buckets[0], s.Buckets[1]
	if b0.Count != 3 || b0.Min != 10 || b0.Max != 14 || b0.Avg != 12 || b0.First != 10 || b0.Last != 12 {
		t.Fatalf("bucket 0 %+v", b0)
	}
	if b1.Count != 2 || b1.Avg != 14 || !b1.Start.Equal(time.Unix(0, t0+60*int64(time.Second))) {
		t.Fatalf("bucket 1 %+v", b1)
	}
	if s.Distinct != nil || s.Changes != nil {
		t.Fatal("numeric summary lists distinct values")
	}
}

func TestSummarizeStatus(t *testing.T) {
	s := Summarize(pts(0, time.Second, "running", "running", "stopped", "running", "fault", "fault"), "", 0)
	if s.Numeric != nil {
		t.Fatal("numeric")
	}
	if len(s.Distinct) != 3 || s.Distinct[0] != (Distinct{"running", 3}) || s.Distinct[1] != (Distinct{"fault", 2}) {
		t.Fatalf("distinct %+v", s.Distinct)
	}
	var vals []string
	for _, c := range s.Changes {
		vals = append(vals, c.Value)
	}
	eq(t, vals, "running", "stopped", "running", "fault")

	// A JSON field holding text, and the change list is capped.
	var many []string
	for i := 0; i < 100; i++ {
		many = append(many, map[bool]string{true: `{"s":"a"}`, false: `{"s":"b"}`}[i%2 == 0])
	}
	s = Summarize(pts(0, time.Second, many...), "s", 0)
	if len(s.Changes) != MaxChanges || s.Changes[len(s.Changes)-1].Value != "b" || len(s.Distinct) != 2 {
		t.Fatalf("%+v", s)
	}
}

func TestSummarizeEmpty(t *testing.T) {
	if s := Summarize(nil, "", time.Minute); s.Count != 0 || s.First != nil || s.Numeric != nil {
		t.Fatalf("%+v", s)
	}
}

func TestAutoBucket(t *testing.T) {
	for _, c := range []struct {
		span time.Duration
		want time.Duration
	}{
		{time.Minute, time.Second},
		{time.Hour, time.Minute},
		{24 * time.Hour, 30 * time.Minute},
		{1000 * 24 * time.Hour, 7 * 24 * time.Hour},
	} {
		if got := AutoBucket(c.span, 60); got != c.want {
			t.Errorf("AutoBucket(%v) = %v, want %v", c.span, got, c.want)
		}
	}
}
