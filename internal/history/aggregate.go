package history

import (
	"bytes"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Value extracts the value at field from a payload. field is a dot-separated
// path into a JSON payload ("celsius", "data.temp", "values.0"); "" means
// the whole payload. ok is false when the path does not exist.
func Value(payload []byte, field string) (v any, ok bool) {
	if field == "" {
		if json.Valid(payload) {
			if err := json.Unmarshal(payload, &v); err == nil {
				return v, true
			}
		}
		return string(payload), true
	}
	if err := json.Unmarshal(payload, &v); err != nil {
		return nil, false
	}
	for _, key := range strings.Split(field, ".") {
		switch c := v.(type) {
		case map[string]any:
			if v, ok = c[key]; !ok {
				return nil, false
			}
		case []any:
			i, err := strconv.Atoi(key)
			if err != nil || i < 0 || i >= len(c) {
				return nil, false
			}
			v = c[i]
		default:
			return nil, false
		}
	}
	return v, true
}

// Number returns the numeric value at field: a JSON number, a JSON boolean
// (1 or 0), or text holding a number (a bare "23.5" payload or a string
// value).
func Number(payload []byte, field string) (float64, bool) {
	if field == "" {
		// Fast path for the common bare-number payload.
		if f, err := strconv.ParseFloat(string(bytes.TrimSpace(payload)), 64); err == nil {
			if !finite(f) {
				return 0, false
			}
			return f, true
		}
	}
	v, ok := Value(payload, field)
	if !ok {
		return 0, false
	}
	return toNumber(v)
}

func toNumber(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil && finite(f)
	}
	return 0, false
}

func finite(f float64) bool { return !math.IsInf(f, 0) && !math.IsNaN(f) }

// text renders the value at field compactly, for distinct values and changes.
func text(payload []byte, field string) (string, bool) {
	var s string
	if field == "" {
		s = string(payload)
	} else {
		v, ok := Value(payload, field)
		if !ok {
			return "", false
		}
		if str, isStr := v.(string); isStr {
			s = str
		} else {
			b, _ := json.Marshal(v)
			s = string(b)
		}
	}
	if !utf8.ValidString(s) {
		return "(binary, " + strconv.Itoa(len(s)) + " bytes)", true
	}
	const maxText = 200
	if len(s) > maxText {
		s = s[:maxText]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
		s += "…"
	}
	return s, true
}

// NumStats summarize the numeric values in a range.
type NumStats struct {
	Count int       `json:"count"`
	Min   float64   `json:"min"`
	MinAt time.Time `json:"min_at"`
	Max   float64   `json:"max"`
	MaxAt time.Time `json:"max_at"`
	Avg   float64   `json:"avg"`
	First float64   `json:"first"`
	Last  float64   `json:"last"`
	// Change is Last - First.
	Change float64 `json:"change"`
}

// Bucket aggregates the numeric values received in [Start, Start+bucket).
type Bucket struct {
	Start time.Time `json:"start"`
	Count int       `json:"count"`
	Min   float64   `json:"min"`
	Max   float64   `json:"max"`
	Avg   float64   `json:"avg"`
	First float64   `json:"first"`
	Last  float64   `json:"last"`
}

// Distinct is one distinct (non-numeric) value and how often it occurred.
type Distinct struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// Change is a point whose value differs from the previous one.
type Change struct {
	Timestamp time.Time `json:"timestamp"`
	Value     string    `json:"value"`
}

// Summary describes the points of a range compactly: numeric statistics and
// time buckets when the values are numbers, otherwise the distinct values
// and the most recent changes.
type Summary struct {
	Count      int        `json:"count"`
	First      *time.Time `json:"first_timestamp,omitempty"`
	Last       *time.Time `json:"last_timestamp,omitempty"`
	Field      string     `json:"field,omitempty"`
	Missing    int        `json:"missing_field,omitempty"` // points without field
	NonNumeric int        `json:"non_numeric,omitempty"`   // points that aren't numbers (when some are)
	Numeric    *NumStats  `json:"numeric,omitempty"`
	BucketSec  float64    `json:"bucket_seconds,omitempty"`
	Buckets    []Bucket   `json:"buckets,omitempty"`
	Distinct   []Distinct `json:"distinct,omitempty"`
	Changes    []Change   `json:"changes,omitempty"`
}

// Summary limits.
const (
	MaxDistinct = 20
	MaxChanges  = 20
	MaxBuckets  = 10000
)

// Summarize aggregates pts (oldest first). bucket > 0 adds time buckets for
// numeric values, aligned to multiples of bucket since the Unix epoch.
func Summarize(pts []Point, field string, bucket time.Duration) Summary {
	sum := Summary{Count: len(pts), Field: field}
	if len(pts) == 0 {
		return sum
	}
	first, last := time.Unix(0, pts[0].Time).UTC(), time.Unix(0, pts[len(pts)-1].Time).UTC()
	sum.First, sum.Last = &first, &last

	var ns NumStats
	var total float64
	var buckets []Bucket
	bidx := map[int64]int{}
	for _, p := range pts {
		f, ok := Number(p.Payload, field)
		if !ok {
			continue
		}
		t := time.Unix(0, p.Time).UTC()
		if ns.Count == 0 {
			ns.Min, ns.Max, ns.First, ns.MinAt, ns.MaxAt = f, f, f, t, t
		}
		if f < ns.Min {
			ns.Min, ns.MinAt = f, t
		}
		if f > ns.Max {
			ns.Max, ns.MaxAt = f, t
		}
		ns.Last = f
		ns.Count++
		total += f
		if bucket > 0 {
			start := p.Time - mod(p.Time, int64(bucket))
			i, ok := bidx[start]
			if !ok {
				if len(buckets) >= MaxBuckets {
					continue
				}
				i = len(buckets)
				bidx[start] = i
				buckets = append(buckets, Bucket{Start: time.Unix(0, start).UTC(), Min: f, Max: f, First: f})
			}
			b := &buckets[i]
			b.Count++
			b.Min, b.Max, b.Last = math.Min(b.Min, f), math.Max(b.Max, f), f
			b.Avg += f // sum for now
		}
	}
	if ns.Count > 0 {
		ns.Avg = round(total / float64(ns.Count))
		ns.Change = round(ns.Last - ns.First)
		sum.Numeric = &ns
		sum.NonNumeric = len(pts) - ns.Count
		if bucket > 0 {
			sort.Slice(buckets, func(i, j int) bool { return buckets[i].Start.Before(buckets[j].Start) })
			for i := range buckets {
				buckets[i].Avg = round(buckets[i].Avg / float64(buckets[i].Count))
			}
			sum.BucketSec = bucket.Seconds()
			sum.Buckets = buckets
		}
	}

	// Distinct values and changes, for status-like (non-numeric) topics.
	if ns.Count == 0 {
		counts := map[string]int{}
		prev, havePrev := "", false
		for _, p := range pts {
			v, ok := text(p.Payload, field)
			if !ok {
				sum.Missing++
				continue
			}
			counts[v]++
			if !havePrev || v != prev {
				sum.Changes = append(sum.Changes, Change{Timestamp: time.Unix(0, p.Time).UTC(), Value: v})
				if len(sum.Changes) > 2*MaxChanges {
					sum.Changes = append(sum.Changes[:0], sum.Changes[len(sum.Changes)-MaxChanges:]...)
				}
			}
			prev, havePrev = v, true
		}
		if len(sum.Changes) > MaxChanges {
			sum.Changes = sum.Changes[len(sum.Changes)-MaxChanges:]
		}
		for v, c := range counts {
			sum.Distinct = append(sum.Distinct, Distinct{v, c})
		}
		sort.Slice(sum.Distinct, func(i, j int) bool {
			a, b := sum.Distinct[i], sum.Distinct[j]
			return a.Count > b.Count || (a.Count == b.Count && a.Value < b.Value)
		})
		if len(sum.Distinct) > MaxDistinct {
			sum.Distinct = sum.Distinct[:MaxDistinct]
		}
	} else if field != "" {
		for _, p := range pts {
			if _, ok := Value(p.Payload, field); !ok {
				sum.Missing++
			}
		}
		sum.NonNumeric -= sum.Missing
	}
	return sum
}

func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// round keeps averages readable (agents and JSON don't need 17 digits).
func round(f float64) float64 {
	if f == 0 || math.Abs(f) >= 1e15 {
		return f
	}
	p := math.Pow(10, 6-math.Ceil(math.Log10(math.Abs(f))))
	if p < 1 {
		return math.Round(f)
	}
	return math.Round(f*p) / p
}

// niceBuckets are the bucket sizes AutoBucket picks from.
var niceBuckets = []time.Duration{
	time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second,
	time.Minute, 5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 30 * time.Minute,
	time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour, 24 * time.Hour, 7 * 24 * time.Hour,
}

// AutoBucket picks the smallest round bucket size giving at most n buckets
// over span.
func AutoBucket(span time.Duration, n int) time.Duration {
	for _, b := range niceBuckets {
		if span/b <= time.Duration(n) {
			return b
		}
	}
	return niceBuckets[len(niceBuckets)-1]
}
