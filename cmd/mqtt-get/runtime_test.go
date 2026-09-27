package main

import "testing"

func TestParseMemoryLimit(t *testing.T) {
	cases := map[string]int64{"max\n": 0, "536870912\n": 536870912, "9223372036854771712": 0, "": 0, "junk": 0}
	for in, want := range cases {
		got, ok := parseMemoryLimit(in)
		if got != want || ok != (want > 0) {
			t.Errorf("parseMemoryLimit(%q) = %d, %v", in, got, ok)
		}
	}
}

func TestGCPercentFor(t *testing.T) {
	const mb = 1 << 20
	cases := map[uint64]int{0: 400, 10 * mb: 400, 64 * mb: 400, 128 * mb: 200, 256 * mb: 100, 2048 * mb: 100}
	for live, want := range cases {
		if got := gcPercentFor(live); got != want {
			t.Errorf("gcPercentFor(%d MiB) = %d, want %d", live/mb, got, want)
		}
	}
}
