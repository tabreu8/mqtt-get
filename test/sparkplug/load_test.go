package sparkplug

// Load test of mqtt-get as a Sparkplug primary host: thousands of simulated
// edge nodes, each with a device, publish births and then data through a
// real MQTT broker (embedded) to mqtt-get. It checks that every message is
// translated (no gaps, no unknown aliases, no rebirth requests, every final
// value right) and reports throughput, latency, memory and how long births,
// a rebirth storm and saving the definitions take.
//
// It is skipped unless SPARKPLUG_LOAD is set. Sizes (defaults in brackets):
//
//	SPARKPLUG_LOAD_NODES    edge nodes, one device each [2000]
//	SPARKPLUG_LOAD_METRICS  metrics per device [50]
//	SPARKPLUG_LOAD_CHANGES  metrics changed per data message [10]
//	SPARKPLUG_LOAD_RATE     data messages per second, all nodes [20000]
//	SPARKPLUG_LOAD_SECONDS  duration of the data phase [20]
//
//	SPARKPLUG_LOAD=1 go test -v -run Load -timeout 30m ./test/sparkplug

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/sparkplug"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
	spb "github.com/tabreu8/mqtt-get/sparkplug"
)

func envInt(t *testing.T, name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q: want a positive integer", name, v)
	}
	return n
}

// loadNode is one simulated edge node with one device.
type loadNode struct {
	id    string
	seq   uint64
	bdSeq uint64
	last  []float64 // last value sent for each device metric
}

const loadGroup = "load"

func (n *loadNode) next() uint64 {
	s := n.seq % 256
	n.seq++
	return s
}

func (n *loadNode) births(b *mochi.Server, metrics int) error {
	n.seq = 0
	node := &spb.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Seq: n.next(), HasSeq: true, Metrics: []spb.Metric{
		{Name: "bdSeq", Datatype: spb.TypeInt64, Value: spb.Value{Kind: spb.KindLong, Uint: n.bdSeq}},
		{Name: sparkplug.RebirthMetric, Alias: 0, HasAlias: true, Datatype: spb.TypeBoolean, Value: spb.Value{Kind: spb.KindBool}},
		{Name: "Uptime", Alias: 1, HasAlias: true, Datatype: spb.TypeInt64, Value: spb.Value{Kind: spb.KindLong, Uint: 1}},
	}}
	if err := b.Publish("spBv1.0/"+loadGroup+"/NBIRTH/"+n.id, spb.Encode(node), false, 0); err != nil {
		return err
	}
	dev := &spb.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Seq: n.next(), HasSeq: true}
	for i := 0; i < metrics; i++ {
		dev.Metrics = append(dev.Metrics, spb.Metric{Name: "Sensors/S" + strconv.Itoa(i), Alias: uint64(10 + i), HasAlias: true,
			Datatype: spb.TypeDouble, Value: spb.Value{Kind: spb.KindDouble, Float: n.last[i]},
			Properties: &spb.PropertySet{Keys: []string{"engUnit"}, Values: []spb.PropertyValue{{Type: spb.TypeString, Value: spb.Value{Kind: spb.KindString, Str: "°C"}}}}})
	}
	return b.Publish("spBv1.0/"+loadGroup+"/DBIRTH/"+n.id+"/dev", spb.Encode(dev), false, 0)
}

// data changes `changes` device metrics, sent by alias only.
func (n *loadNode) data(b *mochi.Server, changes int, step int) error {
	p := &spb.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Seq: n.next(), HasSeq: true}
	for j := 0; j < changes; j++ {
		i := (step*changes + j) % len(n.last)
		n.last[i] = float64(step) + float64(j)/100
		p.Metrics = append(p.Metrics, spb.Metric{Alias: uint64(10 + i), HasAlias: true, Value: spb.Value{Kind: spb.KindDouble, Float: n.last[i]}})
	}
	return b.Publish("spBv1.0/"+loadGroup+"/DDATA/"+n.id+"/dev", spb.Encode(p), false, 0)
}

func (n *loadNode) death(b *mochi.Server) error {
	p := &spb.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Metrics: []spb.Metric{
		{Name: "bdSeq", Datatype: spb.TypeInt64, Value: spb.Value{Kind: spb.KindLong, Uint: n.bdSeq}}}}
	n.bdSeq++
	return b.Publish("spBv1.0/"+loadGroup+"/NDEATH/"+n.id, spb.Encode(p), false, 0)
}

func heapMB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1 << 20)
}

func TestLoadPrimaryHost(t *testing.T) {
	if os.Getenv("SPARKPLUG_LOAD") == "" {
		t.Skip("set SPARKPLUG_LOAD=1 to run the Sparkplug load test")
	}
	nodes := envInt(t, "SPARKPLUG_LOAD_NODES", 2000)
	metrics := envInt(t, "SPARKPLUG_LOAD_METRICS", 50)
	changes := min(envInt(t, "SPARKPLUG_LOAD_CHANGES", 10), metrics)
	rate := envInt(t, "SPARKPLUG_LOAD_RATE", 20000)
	seconds := envInt(t, "SPARKPLUG_LOAD_SECONDS", 20)
	t.Logf("%d edge nodes × 1 device × %d metrics (%d metrics), %d data msg/s × %d changes for %ds, GOMAXPROCS %d",
		nodes, metrics, nodes*(metrics+3), rate, changes, seconds, runtime.GOMAXPROCS(0))

	// Broker: edge nodes publish through its inline client, mqtt-get
	// connects over TCP.
	// The edge nodes' messages queue up for mqtt-get's connection: let
	// the queue hold a burst of births instead of dropping them.
	caps := mochi.NewDefaultServerCapabilities()
	caps.MaximumClientWritesPending = 1 << 20
	b := mochi.New(&mochi.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Capabilities: caps})
	_ = b.AddHook(new(auth.AllowHook), nil)
	addr := freeAddr(t)
	if err := b.AddListener(listeners.NewTCP(listeners.Config{ID: "tcp", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go func() { _ = b.Serve() }()

	dir := t.TempDir()
	st, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := core.New(config.Server{DataDir: dir, MaxTopics: 10_000_000, PublishTimeout: 5 * time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)), st)
	sp := config.Sparkplug{Enabled: true, HostID: "load", Rebirth: true}
	sp.Normalize()
	svc.SetEnvSparkplug(sp)
	baseHeap := heapMB()
	if err := svc.Start(config.Broker{URLs: []string{"tcp://" + addr}, ClientID: "mqtt-get-load",
		Subscriptions: []config.Subscription{{Filter: "nothing/else", QoS: 0}}}, true); err != nil {
		t.Fatal(err)
	}
	closed := false
	shutdown := func() {
		if closed {
			return
		}
		closed = true
		svc.Close()
		for deadline := time.Now().Add(5 * time.Second); b.Clients.Len() > 1 && time.Now().Before(deadline); {
			time.Sleep(10 * time.Millisecond)
		}
		done := make(chan struct{})
		go func() { _ = b.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
	defer shutdown()
	for deadline := time.Now().Add(10 * time.Second); svc.Sparkplug().HostOnline == nil || !*svc.Sparkplug().HostOnline; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("primary host not online: %+v", svc.Sparkplug())
		}
	}

	stats := func() sparkplug.Stats { return *svc.Sparkplug().Stats }
	waitMessages := func(want uint64, what string) time.Duration {
		start := time.Now()
		for deadline := start.Add(5 * time.Minute); stats().Messages < want; time.Sleep(5 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: %d of %d messages translated: %+v", what, stats().Messages, want, stats())
			}
		}
		return time.Since(start)
	}

	ns := make([]*loadNode, nodes)
	for i := range ns {
		ns[i] = &loadNode{id: "edge-" + strconv.Itoa(i), last: make([]float64, metrics)}
	}
	// publishAll runs f for every node on all cores.
	publishAll := func(f func(n *loadNode) error) {
		var wg sync.WaitGroup
		workers := runtime.GOMAXPROCS(0)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := w; i < len(ns); i += workers {
					if err := f(ns[i]); err != nil {
						t.Error(err)
						return
					}
				}
			}(w)
		}
		wg.Wait()
	}

	// 1. Every node and device is born at once.
	start := time.Now()
	publishAll(func(n *loadNode) error { return n.births(b, metrics) })
	expected := uint64(2 * nodes)
	waitMessages(expected, "births")
	birthTime := time.Since(start)
	afterBirths := heapMB()
	t.Logf("births: %d births with %d metrics translated in %v (%.0f metrics/s); heap %.0f MB (%.0f bytes per metric)",
		2*nodes, nodes*(metrics+3), birthTime.Round(time.Millisecond), float64(nodes*(metrics+3))/birthTime.Seconds(),
		afterBirths, (afterBirths-baseHeap)*(1<<20)/float64(nodes*(metrics+3)))

	// 2. Data at a steady rate, with a probe measuring the latency from
	// the edge node's publish to the value being readable.
	var lat []time.Duration
	var latMu sync.Mutex
	// Node 0's messages: the first metric of each carries the step number.
	probeTopic := "spb/" + loadGroup + "/" + ns[0].id + "/dev/#"
	var probeSent sync.Map // value -> send time
	cancel, err := svc.Watch(probeTopic, func(e *store.Entry) {
		if i := strings.Index(string(e.Payload), `"value":`); i >= 0 {
			v := string(e.Payload[i+8:])
			v = v[:strings.IndexAny(v, ",}")]
			if sent, ok := probeSent.Load(v); ok {
				latMu.Lock()
				lat = append(lat, time.Since(sent.(time.Time)))
				latMu.Unlock()
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	tick := time.Duration(float64(time.Second) * float64(nodes) / float64(rate)) // each node sends once per tick
	steps := int(time.Duration(seconds) * time.Second / tick)
	before := stats()
	start = time.Now()
	var sent atomic.Uint64
	for step := 1; step <= steps; step++ {
		due := start.Add(time.Duration(step) * tick)
		publishAll(func(n *loadNode) error {
			if n == ns[0] {
				probeSent.Store(strconv.Itoa(step), time.Now())
			}
			sent.Add(1)
			return n.data(b, changes, step)
		})
		time.Sleep(time.Until(due))
	}
	pubTime := time.Since(start)
	expected += sent.Load()
	drain := waitMessages(expected, "data")
	dataTime := time.Since(start)
	after := stats()
	msgs := after.Messages - before.Messages
	vals := after.Metrics - before.Metrics
	latMu.Lock()
	slices.Sort(lat)
	q := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(p*float64(len(lat))))].Round(10 * time.Microsecond)
	}
	t.Logf("data: %d messages (%d values) published in %v at %.0f msg/s, all translated %v after the last; %.0f msg/s, %.0f values/s overall",
		msgs, vals, pubTime.Round(time.Millisecond), float64(msgs)/pubTime.Seconds(), drain.Round(time.Millisecond),
		float64(msgs)/dataTime.Seconds(), float64(vals)/dataTime.Seconds())
	t.Logf("latency publish → readable (%d samples): p50 %v, p99 %v, max %v", len(lat), q(0.5), q(0.99), q(1))
	latMu.Unlock()

	// Every value is the last one sent; no gaps, no unknown aliases, no
	// problems, no rebirth requests.
	for _, n := range ns {
		for i, want := range n.last {
			e, ok := svc.Store().Get("spb/" + loadGroup + "/" + n.id + "/dev/Sensors/S" + strconv.Itoa(i))
			if !ok || !strings.Contains(string(e.Payload), `"value":`+strconv.FormatFloat(want, 'g', -1, 64)+`,`) {
				t.Fatalf("%s S%d: want %v, have %s", n.id, i, want, e.Payload)
			}
		}
	}
	if after.SeqErrors != 0 || after.Unknown != 0 || after.Problems != 0 || after.Malformed != 0 || after.Dropped != 0 ||
		after.RebirthRequests != 0 || after.NodesOnline != nodes {
		t.Fatalf("stats after the data phase: %+v", after)
	}

	// 3. Rebirth storm: every node dies and is born again at once.
	start = time.Now()
	publishAll(func(n *loadNode) error {
		if err := n.death(b); err != nil {
			return err
		}
		return n.births(b, metrics)
	})
	expected += uint64(3 * nodes)
	waitMessages(expected, "rebirth storm")
	t.Logf("rebirth storm: %d deaths and %d births translated in %v", nodes, 2*nodes, time.Since(start).Round(time.Millisecond))
	if st := stats(); st.NodesOnline != nodes || st.Problems != 0 || st.SeqErrors != 0 {
		t.Fatalf("stats after the rebirth storm: %+v", st)
	}

	// 4. Saving the definitions (on shutdown) and the file's size.
	start = time.Now()
	shutdown()
	fi, err := os.Stat(filepath.Join(dir, "sparkplug.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("shutdown, including saving the definitions: %v; sparkplug.json %.1f MB", time.Since(start).Round(time.Millisecond), float64(fi.Size())/(1<<20))
}
