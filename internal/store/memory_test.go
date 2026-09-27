package store

import (
	"fmt"
	"os"
	"runtime"
	"testing"
)

// TestMemoryPerTopic reports the heap cost of storing 1M topics (run with
// MEMTEST=1; it is slow). Payloads and topic names are allocated separately,
// as the MQTT client does.
func TestMemoryPerTopic(t *testing.T) {
	if os.Getenv("MEMTEST") == "" {
		t.Skip("set MEMTEST=1")
	}
	const n = 1_000_000
	topics := make([]string, n)
	for i := range topics {
		topics[i] = fmt.Sprintf("site/%03d/line/%02d/sensor/%d", i%1000, i%50, i)
	}
	payload := func() []byte { return []byte(`{"v":21.53,"ok":true}`) } // 21 B
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	s := New(0)
	for round := 0; round < 3; round++ { // updates must not grow memory
		for i := 0; i < n; i++ {
			name := string([]byte(topics[i])) // fresh copy, like a decoded packet
			s.Set(&Entry{Topic: name, Payload: payload(), Time: int64(round*n + i)})
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	topicBytes, payloadBytes := 0, 0
	for _, tp := range topics {
		topicBytes += len(tp)
	}
	payloadBytes = n * len(payload())
	used := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("1M topics: heap %.1f MB = %d B/topic (topic name avg %d B + payload %d B + %d B overhead)",
		float64(used)/1e6, used/n, topicBytes/n, payloadBytes/n, (used-int64(topicBytes)-int64(payloadBytes))/n)
	runtime.KeepAlive(s)
}
