package sparkplug

import (
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
	spb "github.com/tabreu8/mqtt-get/sparkplug"
)

// BenchmarkIngest measures mqtt-get's own cost of ingesting a message (no
// broker, no network): plain messages, and Sparkplug DDATA messages with 1,
// 10 and 50 metrics (sent by alias, as edge nodes do) from 500 edge nodes
// whose births were seen. ns/op is per message; values/s counts translated
// metric values.
//
//	go test -run '^$' -bench Ingest -benchmem ./test/sparkplug
func BenchmarkIngest(b *testing.B) {
	b.Run("plain", func(b *testing.B) {
		svc := benchService(b)
		msgs := make([]store.Entry, 0, 50_000)
		for i := 0; i < cap(msgs); i++ {
			msgs = append(msgs, store.Entry{Topic: "plant/line" + strconv.Itoa(i%50) + "/m" + strconv.Itoa(i), Payload: []byte(`{"value":21.5}`)})
		}
		runIngest(b, msgs, 1, func(e store.Entry) { svc.Ingest(e) })
	})
	for _, k := range []int{1, 10, 50} {
		b.Run("sparkplug-"+strconv.Itoa(k)+"-metrics", func(b *testing.B) {
			svc := benchService(b)
			runIngest(b, sparkplugStream(svc.Ingest, 500, k), k, func(e store.Entry) { svc.Ingest(e) })
		})
	}
	// As messages arrive from the broker: one goroutine (a connection's
	// reader) hands them to the translation workers, one per CPU.
	for _, k := range []int{1, 10, 50} {
		b.Run("sparkplug-"+strconv.Itoa(k)+"-metrics-workers", func(b *testing.B) {
			svc := benchService(b)
			if err := svc.Start(config.Broker{}, false); err != nil {
				b.Fatal(err)
			}
			msgs := sparkplugStream(svc.Ingest, 500, k)
			base := svc.Sparkplug().Stats.Messages
			// A socket read holds about 16 KB of messages.
			r, per, n := svc.NewReceiver(), max(1, 16<<10/len(msgs[0].Payload)), 0
			runIngest(b, msgs, k, func(e store.Entry) {
				r.Receive(e)
				if n++; n%per == 0 {
					r.Flush()
				}
			}, func() {
				r.Flush()
				for svc.Sparkplug().Stats.Messages < base+uint64(b.N) {
					time.Sleep(50 * time.Microsecond)
				}
			})
		})
	}
}

func benchService(b *testing.B) *core.Service {
	st, _ := state.Open("")
	svc := core.New(config.Server{MaxTopics: 10_000_000}, slog.New(slog.NewTextHandler(io.Discard, nil)), st)
	b.Cleanup(svc.Close)
	sp := config.Sparkplug{Enabled: true}
	sp.Normalize()
	if _, err := svc.SetSparkplug(sp); err != nil {
		b.Fatal(err)
	}
	return svc
}

// sparkplugStream gives birth to nodes edge nodes (each with one device of
// max(50, k) metrics) through ingest, and returns their DDATA messages, k
// metrics each, interleaved across nodes. Each node has 256 messages with
// consecutive sequence numbers following its births, so the stream can be
// replayed in a loop.
func sparkplugStream(ingest func(store.Entry), nodes, k int) []store.Entry {
	metrics := max(50, k)
	now := time.Now().UnixNano()
	perNode := make([][]store.Entry, nodes)
	for i := range perNode {
		id := "edge-" + strconv.Itoa(i)
		n := &loadNode{id: id, last: make([]float64, metrics)}
		ingest(store.Entry{Topic: "spBv1.0/" + loadGroup + "/NBIRTH/" + id, Time: now, Payload: spb.Encode(&spb.Payload{Timestamp: 1, HasTimestamp: true,
			Seq: n.next(), HasSeq: true, Metrics: []spb.Metric{{Name: "bdSeq", Datatype: spb.TypeInt64, Value: spb.Value{Kind: spb.KindLong}}}})})
		birth := &spb.Payload{Timestamp: 1, HasTimestamp: true, Seq: n.next(), HasSeq: true}
		for m := 0; m < metrics; m++ {
			birth.Metrics = append(birth.Metrics, spb.Metric{Name: "Sensors/S" + strconv.Itoa(m), Alias: uint64(10 + m), HasAlias: true,
				Datatype: spb.TypeDouble, Value: spb.Value{Kind: spb.KindDouble},
				Properties: &spb.PropertySet{Keys: []string{"engUnit"}, Values: []spb.PropertyValue{{Type: spb.TypeString, Value: spb.Value{Kind: spb.KindString, Str: "°C"}}}}})
		}
		ingest(store.Entry{Topic: "spBv1.0/" + loadGroup + "/DBIRTH/" + id + "/dev", Time: now, Payload: spb.Encode(birth)})
		for s := 0; s < 256; s++ {
			p := &spb.Payload{Timestamp: uint64(1760000000000 + s), HasTimestamp: true, Seq: n.next(), HasSeq: true}
			for j := 0; j < k; j++ {
				p.Metrics = append(p.Metrics, spb.Metric{Alias: uint64(10 + (s*k+j)%metrics), HasAlias: true,
					Value: spb.Value{Kind: spb.KindDouble, Float: float64(s) + float64(j)/8}})
			}
			perNode[i] = append(perNode[i], store.Entry{Topic: "spBv1.0/" + loadGroup + "/DDATA/" + id + "/dev", Payload: spb.Encode(p)})
		}
	}
	out := make([]store.Entry, 0, nodes*256)
	for s := 0; s < 256; s++ {
		for i := range perNode {
			out = append(out, perNode[i][s])
		}
	}
	return out
}

func runIngest(b *testing.B, msgs []store.Entry, values int, ingest func(store.Entry), wait ...func()) {
	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		e := msgs[i%len(msgs)]
		e.Time = start.UnixNano() + int64(i)
		ingest(e)
	}
	for _, w := range wait {
		w()
	}
	d := time.Since(start).Seconds()
	b.ReportMetric(float64(b.N)/d, "msg/s")
	b.ReportMetric(float64(b.N*values)/d, "values/s")
}
