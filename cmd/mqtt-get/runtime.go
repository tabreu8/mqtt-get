package main

import (
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
	"strings"
	"time"
)

// Garbage-collector tuning.
//
// Ingest benchmarks showed ~22% more throughput with GOGC=400 than with the
// default 100, because the GC runs less often. But GOGC=400 lets the heap
// grow to 5x the live data: with 1M topics that was 1.5 GB RSS instead of
// 480 MB. So instead of a fixed value, the tuner allows a fixed amount of
// garbage headroom (256 MiB), or 100% of the live heap if that is larger:
// small deployments run at GOGC=400, large ones converge to GOGC=100.
const (
	gcHeadroom   = 256 << 20
	gcPercentMax = 400
	gcPercentMin = 100
	gcTuneEvery  = 5 * time.Second
)

// tuneRuntime applies the GC policy unless the standard GOGC / GOMEMLIMIT
// variables are set, and sets a soft memory limit at 90% of the container
// (cgroup) memory limit when there is one.
func tuneRuntime(log *slog.Logger) {
	gc := "adaptive (100-400)"
	if os.Getenv("GOGC") != "" {
		gc = "GOGC env"
	} else {
		debug.SetGCPercent(gcPercentMax)
		go gcTuner()
	}
	mem := "none"
	if os.Getenv("GOMEMLIMIT") != "" {
		mem = "GOMEMLIMIT env"
	} else if limit, ok := cgroupMemoryLimit(); ok {
		soft := int64(float64(limit) * 0.9)
		debug.SetMemoryLimit(soft)
		mem = strconv.FormatInt(soft>>20, 10) + " MiB (90% of container limit)"
	}
	log.Info("runtime: garbage collector", "gc_percent", gc, "memory_limit", mem)
}

// gcPercentFor returns the GOGC value giving max(gcHeadroom, live) of
// garbage headroom above a live heap of live bytes.
func gcPercentFor(live uint64) int {
	if live == 0 {
		return gcPercentMax
	}
	p := int(float64(gcHeadroom) / float64(live) * 100)
	return min(max(p, gcPercentMin), gcPercentMax)
}

func gcTuner() {
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	current := gcPercentMax
	for range time.Tick(gcTuneEvery) {
		metrics.Read(sample)
		if sample[0].Value.Kind() != metrics.KindUint64 {
			return
		}
		want := gcPercentFor(sample[0].Value.Uint64())
		// Avoid churn: only react to meaningful changes.
		if want*10 < current*9 || want*10 > current*11 {
			debug.SetGCPercent(want)
			current = want
		}
	}
}

// cgroupMemoryLimit returns the memory limit of the current cgroup (v2 or v1).
func cgroupMemoryLimit() (int64, bool) {
	for _, f := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		b, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		if v, ok := parseMemoryLimit(string(b)); ok {
			return v, true
		}
	}
	return 0, false
}

func parseMemoryLimit(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	// cgroup v1 reports "unlimited" as a huge number (page-aligned MaxInt64).
	if err != nil || v <= 0 || v >= math.MaxInt64/2 {
		return 0, false
	}
	return v, true
}
