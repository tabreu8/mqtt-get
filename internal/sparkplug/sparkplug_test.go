package sparkplug

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// Golden payloads encoded by the reference protobuf implementation (Python
// google.protobuf with sparkplug_b.proto; see the test data generator in
// the commit history). birthHex is an NBIRTH with one metric of every
// supported type; dataHex an NDATA using aliases (one of them unknown).
const (
	birthHex = "08fb80b3c19c3312120a05626453657118fb80b3c19c3320085807121c0a144e6f646520436f6e74726f6c2f526562697274681001200b700012470a0b54656d7065726174757265100218e480b3c19c3320094a280a07656e67556e69740a07656e67486967681207080c4203c2b043120b080a310000000000c06240650000ac4112110a05436f756e741003200350fbffffff0f12110a05536d616c6c1004200150feffffff0f12140a03426967100520045880ccbbbcdeffffffff0112140a035536341006200858ffffffffffffffffff0112170a0850726573737572651007200a696666666666d66140120f0a0752756e6e696e671008200b700112170a044d6f64651009200c7a0b6175746f2022666173742212140a0753746172746564100a200d588080b3c19c3312100a04426c6f62100b20118201030001ff120f0a074e6f7468696e67100c2009380112190a04496e7473100d201882010cffffffff02000000e093040012170a06466c6f617473100e201e8201080000c03f000010c012130a05426f6f6c73100f202082010503000000a012130a054e616d6573101020218201056162006300122e0a055461626c65101120108a012008021202696412056c6162656c1803180c220d0a0608ffffffff0f0a0332017812340a054d6f746f7210122013920126120a0a0372706d200750dc0b1a0d0a066d617852706d100718b81722094d6f746f7254797065121d0a11466f6c6465722f446565702f56616c7565101320075080d0acf30e1800"
	dataHex  = "08e887b3c19c33120e100218e887b3c19c33650000b2411208100350faffffff0f1204106350011801"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type fakeBroker struct {
	mu   sync.Mutex
	sent []struct {
		topic   string
		payload []byte
	}
}

func (f *fakeBroker) publish(topic string, payload []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, struct {
		topic   string
		payload []byte
	}{topic, payload})
}

func (f *fakeBroker) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

func newT(cfg config.Sparkplug) (*Translator, *fakeBroker, *time.Time) {
	cfg.Enabled = true
	cfg.Normalize()
	fb := &fakeBroker{}
	tr := New(cfg, fb.publish)
	now := time.Unix(1760000000, 0)
	tr.now = func() time.Time { return now }
	return tr, fb, &now
}

func entry(topic string, payload []byte) store.Entry {
	return store.Entry{Topic: topic, Payload: payload, Time: time.Unix(1760000005, 0).UnixNano()}
}

// values maps translated topics to their decoded JSON envelopes.
func values(t *testing.T, out []store.Entry) map[string]map[string]any {
	t.Helper()
	m := map[string]map[string]any{}
	for _, e := range out {
		var v map[string]any
		if err := json.Unmarshal(e.Payload, &v); err != nil {
			t.Fatalf("%s: invalid JSON %s: %v", e.Topic, e.Payload, err)
		}
		m[e.Topic] = v
	}
	return m
}

func jsonOf(v any) string { b, _ := json.Marshal(v); return string(b) }

func TestDecodeGoldenBirth(t *testing.T) {
	p, err := Decode(mustHex(t, birthHex))
	if err != nil {
		t.Fatal(err)
	}
	if !p.HasSeq || p.Seq != 0 || p.Timestamp != 1760000000123 || len(p.Metrics) != 20 {
		t.Fatalf("payload header %+v (%d metrics)", p, len(p.Metrics))
	}
	temp := p.Metrics[2]
	if temp.Name != "Temperature" || temp.Alias != 2 || temp.Datatype != TypeFloat || temp.Value.F != 21.5 {
		t.Fatalf("temperature %+v", temp)
	}
	if u, ok := temp.Properties.Property("engunit"); !ok || u != "°C" {
		t.Fatalf("engUnit %q", u)
	}
	// Re-encoding and decoding again is lossless.
	p2, err := Decode(Encode(p))
	if err != nil || !reflect.DeepEqual(p, p2) {
		t.Fatalf("round trip: %v\n%+v\n%+v", err, p, p2)
	}
}

func TestTranslateBirthAndData(t *testing.T) {
	tr, fb, _ := newT(config.Sparkplug{})
	out, handled := tr.Process(entry("spBv1.0/plant/NBIRTH/press-gw", mustHex(t, birthHex)))
	if !handled {
		t.Fatal("not handled")
	}
	v := values(t, out)
	want := map[string]string{
		"Temperature":       `21.5`,
		"Count":             `-5`,
		"Small":             `-2`,
		"Big":               `-9000000000`,
		"Pressure":          `142.7`,
		"Running":           `true`,
		"Mode":              `"auto \"fast\""`,
		"Started":           `"2025-10-09T08:53:20Z"`,
		"Blob":              `"AAH/"`,
		"Nothing":           `null`,
		"Ints":              `[-1,2,300000]`,
		"Floats":            `[1.5,-2.25]`,
		"Bools":             `[true,false,true]`,
		"Names":             `["ab","c"]`,
		"Table":             `{"columns":["id","label"],"rows":[[-1,"x"]],"types":["Int32","String"]}`,
		"Motor":             `{"metrics":{"rpm":1500},"parameters":{"maxRpm":3000},"template_ref":"MotorType"}`,
		"Folder/Deep/Value": `4000000000`,
	}
	for name, w := range want {
		got, ok := v["spb/plant/press-gw/"+name]
		if !ok {
			t.Errorf("%s not translated", name)
			continue
		}
		if g := jsonOf(got["value"]); g != w {
			t.Errorf("%s = %s, want %s", name, g, w)
		}
		if got["online"] != true {
			t.Errorf("%s not online", name)
		}
	}
	// U64 above 2^53 must keep every digit (JSON number text, not float).
	for _, e := range out {
		if e.Topic == "spb/plant/press-gw/U64" && !strings.Contains(string(e.Payload), "18446744073709551615") {
			t.Fatalf("U64 lost precision: %s", e.Payload)
		}
	}
	temp := v["spb/plant/press-gw/Temperature"]
	if temp["type"] != "Float" || temp["unit"] != "°C" || temp["timestamp"] != "2025-10-09T08:53:20.1Z" ||
		jsonOf(temp["properties"]) != `{"engHigh":150,"engUnit":"°C"}` {
		t.Fatalf("temperature envelope %v", temp)
	}

	out, _ = tr.Process(entry("spBv1.0/plant/NDATA/press-gw", mustHex(t, dataHex)))
	v = values(t, out)
	if len(out) != 2 || jsonOf(v["spb/plant/press-gw/Temperature"]["value"]) != "22.25" || v["spb/plant/press-gw/Count"]["value"] != -6.0 {
		t.Fatalf("data %v", v)
	}
	if v["spb/plant/press-gw/Temperature"]["unit"] != "°C" {
		t.Fatal("unit from the birth must carry over to data values")
	}
	st := tr.Stats()
	if st.Unknown != 1 || st.SeqErrors != 0 || st.Nodes != 1 || st.NodesOnline != 1 {
		t.Fatalf("stats %+v", st)
	}
	if fb.count() != 0 {
		t.Fatal("rebirth requested although disabled")
	}
}

func birthPayload(seq uint64, bdSeq uint64, metrics ...Metric) []byte {
	p := &Payload{Timestamp: 1, HasTimestamp: true, Seq: seq, HasSeq: true}
	p.Metrics = append([]Metric{{Name: "bdSeq", Datatype: TypeUInt64, Value: Value{Kind: KindLong, U: bdSeq}}}, metrics...)
	return Encode(p)
}

func devBirthPayload(seq uint64, metrics ...Metric) []byte {
	return Encode(&Payload{Timestamp: 1, HasTimestamp: true, Seq: seq, HasSeq: true, Metrics: metrics})
}

func dataPayload(seq uint64, metrics ...Metric) []byte {
	return Encode(&Payload{Timestamp: 2, HasTimestamp: true, Seq: seq, HasSeq: true, Metrics: metrics})
}

func dbl(name string, alias uint64, f float64) Metric {
	return Metric{Name: name, Alias: alias, HasAlias: true, Datatype: TypeDouble, Value: Value{Kind: KindDouble, F: f}}
}

func byAlias(alias uint64, f float64) Metric {
	return Metric{Alias: alias, HasAlias: true, Value: Value{Kind: KindDouble, F: f}}
}

func TestDeviceLifecycleAndDeath(t *testing.T) {
	tr, _, _ := newT(config.Sparkplug{})
	tr.Process(entry("spBv1.0/g/NBIRTH/e", birthPayload(0, 3, dbl("Uptime", 1, 5))))
	out, _ := tr.Process(entry("spBv1.0/g/DBIRTH/e/pump", devBirthPayload(1, dbl("Flow", 10, 1.5))))
	if v := values(t, out); v["spb/g/e/pump/Flow"]["value"] != 1.5 {
		t.Fatalf("device birth %v", v)
	}
	out, _ = tr.Process(entry("spBv1.0/g/DDATA/e/pump", dataPayload(2, byAlias(10, 2.5))))
	if v := values(t, out); v["spb/g/e/pump/Flow"]["value"] != 2.5 {
		t.Fatalf("device data %v", v)
	}
	// An alias of the node is not a metric of the device.
	if out, _ = tr.Process(entry("spBv1.0/g/DDATA/e/pump", dataPayload(3, byAlias(1, 9)))); len(out) != 0 {
		t.Fatalf("cross-scope alias resolved: %v", out)
	}

	// DDEATH marks the device's metrics offline, keeping value and time.
	out, _ = tr.Process(entry("spBv1.0/g/DDEATH/e/pump", dataPayload(4)))
	if v := values(t, out); len(out) != 1 || v["spb/g/e/pump/Flow"]["online"] != false || v["spb/g/e/pump/Flow"]["value"] != 2.5 {
		t.Fatalf("device death %v", v)
	}
	if out[0].Time != time.Unix(1760000005, 0).UnixNano() {
		t.Fatal("offline value must keep the time of the last update")
	}

	// A death from an older session (other bdSeq) is ignored.
	if out, _ = tr.Process(entry("spBv1.0/g/NDEATH/e", Encode(&Payload{Metrics: []Metric{{Name: "bdSeq", Value: Value{Kind: KindLong, U: 2}}}}))); len(out) != 0 {
		t.Fatalf("stale death applied: %v", out)
	}
	if !tr.Nodes("", false)[0].Online {
		t.Fatal("stale death took the node offline")
	}
	out, _ = tr.Process(entry("spBv1.0/g/NDEATH/e", Encode(&Payload{Metrics: []Metric{{Name: "bdSeq", Value: Value{Kind: KindLong, U: 3}}}})))
	if v := values(t, out); v["spb/g/e/Uptime"]["online"] != false {
		t.Fatalf("node death %v", v)
	}
	nodes := tr.Nodes("", true)
	if len(nodes) != 1 || nodes[0].Online || nodes[0].Devices[0].Online || nodes[0].Metrics[0].Name != "Uptime" || *nodes[0].BdSeq != 3 {
		t.Fatalf("nodes %+v", nodes)
	}

	// A new birth drops the old devices (they must be born again).
	tr.Process(entry("spBv1.0/g/NBIRTH/e", birthPayload(0, 4, dbl("Uptime", 1, 0))))
	if n := tr.Nodes("", false)[0]; !n.Online || len(n.Devices) != 0 {
		t.Fatalf("after rebirth %+v", n)
	}
	if tr.IsMetricTopic("spb/g/e/pump/Flow") {
		t.Fatal("old device metric still writable")
	}
}

func TestRebirthRequests(t *testing.T) {
	tr, fb, now := newT(config.Sparkplug{Rebirth: true, RebirthIntervalSec: 30})
	// Data from a node whose birth we missed: named metrics are still
	// translated (typed by value) and a rebirth is requested, once per interval.
	out, _ := tr.Process(entry("spBv1.0/g/NDATA/e", dataPayload(5, Metric{Name: "Speed", Value: Value{Kind: KindFloat, F: 3}}, byAlias(7, 1))))
	if v := values(t, out); v["spb/g/e/Speed"]["value"] != 3.0 || v["spb/g/e/Speed"]["type"] != "Float" {
		t.Fatalf("named metric without birth: %v", v)
	}
	tr.Process(entry("spBv1.0/g/NDATA/e", dataPayload(6, byAlias(7, 1))))
	if fb.count() != 1 {
		t.Fatalf("rebirth requests: %d, want 1 (rate limited)", fb.count())
	}
	sent := fb.sent[0]
	p, err := Decode(sent.payload)
	if err != nil || sent.topic != "spBv1.0/g/NCMD/e" || len(p.Metrics) != 1 || p.Metrics[0].Name != RebirthMetric ||
		p.Metrics[0].Datatype != TypeBoolean || p.Metrics[0].Value.U != 1 {
		t.Fatalf("rebirth request %s %+v %v", sent.topic, p, err)
	}
	*now = now.Add(31 * time.Second)
	tr.Process(entry("spBv1.0/g/NDATA/e", dataPayload(7, byAlias(7, 1))))
	if fb.count() != 2 {
		t.Fatalf("after the interval: %d requests", fb.count())
	}

	// After a birth, a sequence gap triggers a request too.
	*now = now.Add(31 * time.Second)
	tr.Process(entry("spBv1.0/g/NBIRTH/e", birthPayload(0, 1, dbl("Speed", 7, 0))))
	tr.Process(entry("spBv1.0/g/NDATA/e", dataPayload(1, byAlias(7, 1))))
	if fb.count() != 2 {
		t.Fatal("request without reason")
	}
	tr.Process(entry("spBv1.0/g/NDATA/e", dataPayload(5, byAlias(7, 1)))) // 2, 3, 4 lost
	if fb.count() != 3 || tr.Stats().SeqErrors != 1 {
		t.Fatalf("seq gap: %d requests, %+v", fb.count(), tr.Stats())
	}
	// Sequence numbers wrap at 256.
	tr.Process(entry("spBv1.0/g/NDATA/e", dataPayload(255)))
	tr.Process(entry("spBv1.0/g/NDATA/e", dataPayload(0)))
	if tr.Stats().SeqErrors != 2 { // 5 -> 255 is a gap, 255 -> 0 is not
		t.Fatalf("wrap: %+v", tr.Stats())
	}

	// Manual requests ignore the setting but stay rate limited.
	tr2, fb2, _ := newT(config.Sparkplug{})
	tr2.Process(entry("spBv1.0/g/NBIRTH/e", birthPayload(0, 1)))
	if ok, err := tr2.RequestRebirth("g", "e"); !ok || err != nil || fb2.count() != 1 {
		t.Fatalf("manual rebirth %v %v", ok, err)
	}
	if ok, _ := tr2.RequestRebirth("g", "e"); ok {
		t.Fatal("manual rebirth not rate limited")
	}
	if _, err := tr2.RequestRebirth("g", "nope"); err != ErrUnknownNode {
		t.Fatalf("unknown node: %v", err)
	}
}

func TestCommands(t *testing.T) {
	tr, _, _ := newT(config.Sparkplug{Commands: true})
	tr.Process(entry("spBv1.0/g/NBIRTH/e", birthPayload(0, 1,
		Metric{Name: "Setpoint", Alias: 1, HasAlias: true, Datatype: TypeFloat, Value: Value{Kind: KindFloat, F: 20}},
		Metric{Name: "Mode", Datatype: TypeString, Value: Value{Kind: KindString, S: "auto"}})))
	tr.Process(entry("spBv1.0/g/DBIRTH/e/valve", devBirthPayload(1,
		Metric{Name: "Open", Datatype: TypeBoolean, Value: Value{Kind: KindBool}},
		Metric{Name: "Offset", Datatype: TypeInt16, Value: Value{Kind: KindInt}})))

	cases := []struct {
		topic, payload, cmdTopic string
		want                     Value
	}{
		{"spb/g/e/Setpoint", "21.5", "spBv1.0/g/NCMD/e", Value{Kind: KindFloat, F: 21.5}},
		{"spb/g/e/Setpoint", `{"value":"22"}`, "spBv1.0/g/NCMD/e", Value{Kind: KindFloat, F: 22}},
		{"spb/g/e/Mode", "manual", "spBv1.0/g/NCMD/e", Value{Kind: KindString, S: "manual"}},
		{"spb/g/e/valve/Open", "true", "spBv1.0/g/DCMD/e/valve", Value{Kind: KindBool, U: 1}},
		{"spb/g/e/valve/Open", "OFF", "spBv1.0/g/DCMD/e/valve", Value{Kind: KindBool, U: 0}},
		{"spb/g/e/valve/Offset", "-3", "spBv1.0/g/DCMD/e/valve", Value{Kind: KindInt, U: uint64(uint32(0xfffffffd))}},
	}
	for _, c := range cases {
		ct, cmd, err := tr.Command(c.topic, []byte(c.payload))
		if err != nil {
			t.Fatalf("%s %s: %v", c.topic, c.payload, err)
		}
		p, err := Decode(cmd)
		if err != nil || ct != c.cmdTopic || len(p.Metrics) != 1 || !p.HasTimestamp {
			t.Fatalf("%s: %s %+v %v", c.topic, ct, p, err)
		}
		m := p.Metrics[0]
		if !strings.HasSuffix(c.topic, "/"+m.Name) || !reflect.DeepEqual(m.Value, c.want) || m.Datatype == 0 {
			t.Fatalf("%s %s: metric %+v, want value %+v", c.topic, c.payload, m, c.want)
		}
	}
	for _, bad := range []struct{ topic, payload, err string }{
		{"spb/g/e/valve/Offset", "40000", "out of range"},
		{"spb/g/e/Setpoint", "warm", "not a number"},
		{"spb/g/e/valve/Open", "maybe", "true or false"},
		{"spb/g/e/Unknown", "1", "no birth certificate"},
	} {
		if _, _, err := tr.Command(bad.topic, []byte(bad.payload)); err == nil || !strings.Contains(err.Error(), bad.err) {
			t.Errorf("%s %s: %v, want %q", bad.topic, bad.payload, err, bad.err)
		}
	}
	off, _, _ := newT(config.Sparkplug{})
	off.Process(entry("spBv1.0/g/NBIRTH/e", birthPayload(0, 1, dbl("X", 1, 0))))
	if _, _, err := off.Command("spb/g/e/X", []byte("1")); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("commands disabled: %v", err)
	}
}

func TestTopicHandling(t *testing.T) {
	tr, _, _ := newT(config.Sparkplug{Prefix: "/uns/spb/"})
	out, _ := tr.Process(entry("spBv1.0/g/NBIRTH/e", birthPayload(0, 1, dbl("a+b#c", 1, 1))))
	if v := values(t, out); v["uns/spb/g/e/a_b_c"] == nil {
		t.Fatalf("prefix and sanitizing: %v", v)
	}
	for _, name := range []string{"spBv1.0/STATE/host", "spBv1.0/g/NBIRTH", "spBv1.0/g/DDATA/e", "spBv1.0/g/NDATA/e/extra", "spBv1.0//NDATA/e", "other/x"} {
		if _, handled := tr.Process(entry(name, nil)); handled {
			t.Errorf("%s handled", name)
		}
	}
	if _, handled := tr.Process(entry("spBv1.0/g/DCMD/e/d", []byte{1, 2})); !handled {
		t.Error("commands from other hosts must be consumed, not stored")
	}
	if _, handled := tr.Process(entry("spBv1.0/g/NDATA/e", []byte{0xff, 0xff})); !handled || tr.Stats().Malformed != 1 {
		t.Error("malformed payload")
	}
	if !IsSparkplug("spBv1.0/g/NDATA/e") || IsSparkplug("spBv1.0") || IsSparkplug("spBv1.0x/a") {
		t.Error("IsSparkplug")
	}
}

func TestPackedTypesAndUnknownFields(t *testing.T) {
	// A DataSet with packed types and an unknown field (99) must decode.
	var w writer
	w.msg(2, func(w *writer) {
		w.str(1, "T")
		w.varint(4, TypeDataSet)
		w.msg(17, func(w *writer) {
			w.varint(1, 2)
			w.str(2, "a")
			w.str(2, "b")
			w.bytes(3, []byte{TypeInt32, TypeDouble}) // packed varints
			w.varint(99, 5)
		})
	})
	w.varint(99, 1)
	p, err := Decode(w.b)
	if err != nil || len(p.Metrics) != 1 || !reflect.DeepEqual(p.Metrics[0].Value.DataSet.Types, []uint32{TypeInt32, TypeDouble}) {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{{0x12, 0x05, 0x01}, {0x08}, {0x00}, {0x12, 0x80}} {
		if _, err := Decode(b); err == nil {
			t.Errorf("% x decoded", b)
		}
	}
	// Deep nesting is bounded.
	inner := []byte{}
	for i := 0; i < 100; i++ {
		var w writer
		w.msg(2, func(w *writer) { w.b = append(w.b, inner...) }) // Template.metrics
		var m writer
		m.bytes(18, w.b) // Metric.template_value
		inner = m.b
	}
	var top writer
	top.bytes(2, inner)
	if _, err := Decode(top.b); err == nil {
		t.Error("unbounded nesting accepted")
	}
}

func TestValueFromJSONFloatPrecision(t *testing.T) {
	v, err := ValueFromJSON(TypeFloat, []byte("0.1"))
	if err != nil || float32(v.F) != float32(0.1) {
		t.Fatalf("%v %v", v, err)
	}
	if _, err := ValueFromJSON(TypeFloat, []byte("1e300")); err == nil {
		t.Fatal("float overflow accepted")
	}
	if b := AppendValue(nil, TypeDouble, Value{Kind: KindDouble, F: math.NaN()}); string(b) != "null" {
		t.Fatalf("NaN rendered as %s", b)
	}
	v, err = ValueFromJSON(TypeDateTime, []byte(`"2026-01-02T03:04:05Z"`))
	if err != nil || v.U != uint64(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixMilli()) {
		t.Fatalf("datetime %v %v", v, err)
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(mustHexF(birthHex))
	f.Add(mustHexF(dataHex))
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := Decode(b)
		if err != nil {
			return
		}
		// Anything decoded renders as valid JSON and re-encodes losslessly.
		for _, m := range p.Metrics {
			if out := AppendValue(nil, m.Datatype, m.Value); !json.Valid(out) {
				t.Fatalf("invalid JSON %s for %+v", out, m)
			}
		}
		if _, err := Decode(Encode(p)); err != nil {
			t.Fatalf("re-encoded payload does not decode: %v", err)
		}
	})
}

func mustHexF(s string) []byte { b, _ := hex.DecodeString(s); return b }

// BenchmarkProcessDData measures decoding and translating a DDATA message
// with 10 metrics (aliases), the common steady-state message.
func BenchmarkProcessDData(b *testing.B) {
	tr := New(config.Sparkplug{Enabled: true, Prefix: "spb", RebirthIntervalSec: 30}, nil)
	var birth, data []Metric
	for i := uint64(0); i < 10; i++ {
		birth = append(birth, dbl("Metric"+strconv.Itoa(int(i)), i, 0))
		data = append(data, byAlias(i, float64(i)))
	}
	tr.Process(entry("spBv1.0/g/NBIRTH/e", birthPayload(0, 1)))
	tr.Process(entry("spBv1.0/g/DBIRTH/e/d", devBirthPayload(1, birth...)))
	var payloads [256][]byte
	for i := range payloads {
		payloads[i] = dataPayload(uint64(i), data...)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, _ := tr.Process(entry("spBv1.0/g/DDATA/e/d", payloads[(i+2)%256]))
		if len(out) != 10 {
			b.Fatal(len(out))
		}
	}
}
