package sparkplug

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
)

// Golden payloads encoded by the reference protobuf implementation with the
// official sparkplug_b.proto (regenerate with testdata/gen_golden.py):
// birthHex is an NBIRTH with one metric of every kind of value, dataHex an
// NDATA using aliases, metadataHex a File metric with metadata.
const (
	birthHex    = "08fb80b3c19c3312120a05626453657118fb80b3c19c3320085807121c0a144e6f646520436f6e74726f6c2f526562697274681001200b700012470a0b54656d7065726174757265100218e480b3c19c3320094a280a07656e67556e69740a07656e67486967681207080c4203c2b043120b080a310000000000c06240650000ac4112110a05436f756e741003200350fbffffff0f12110a05536d616c6c1004200150feffffff0f12140a03426967100520045880ccbbbcdeffffffff0112140a035536341006200858ffffffffffffffffff0112170a0850726573737572651007200a696666666666d66140120f0a0752756e6e696e671008200b700112170a044d6f64651009200c7a0b6175746f2022666173742212140a0753746172746564100a200d588080b3c19c3312100a04426c6f62100b20118201030001ff120f0a074e6f7468696e67100c2009380112190a04496e7473100d201882010cffffffff02000000e093040012170a06466c6f617473100e201e8201080000c03f000010c012130a05426f6f6c73100f202082010503000000a012130a054e616d6573101020218201056162006300122e0a055461626c65101120108a012008021202696412056c6162656c1803180c220d0a0608ffffffff0f0a0332017812340a054d6f746f7210122013920126120a0a0372706d200750dc0b1a0d0a066d617852706d100718b81722094d6f746f7254797065121d0a11466f6c6465722f446565702f56616c7565101320075080d0acf30e1800"
	dataHex     = "08e887b3c19c33120e100218e887b3c19c33650000b2411208100350faffffff0f1204106350011801"
	metadataHex = "08d08fb3c19c3312680a06436f6e6669671014201242530801120f6170706c69636174696f6e2f7a697018802020032a0a636f6e6669672e7a697032037a69703a2064343164386364393866303062323034653938303039393865636638343237654206706172742033820104504b03041802"
)

func mustHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDecodeGoldenBirth(t *testing.T) {
	p, err := Decode(mustHex(t, birthHex))
	if err != nil {
		t.Fatal(err)
	}
	if !p.HasSeq || p.Seq != 0 || !p.HasTimestamp || p.Timestamp != 1760000000123 || len(p.Metrics) != 20 {
		t.Fatalf("payload header %+v (%d metrics)", p, len(p.Metrics))
	}
	byName := map[string]Metric{}
	for _, m := range p.Metrics {
		byName[m.Name] = m
	}
	check := func(name string, dt DataType, want Value) {
		t.Helper()
		m, ok := byName[name]
		if !ok {
			t.Fatalf("%s missing", name)
		}
		if m.Datatype != dt || !reflect.DeepEqual(m.Value, want) {
			t.Errorf("%s: %v %+v, want %v %+v", name, m.Datatype, m.Value, dt, want)
		}
	}
	check("bdSeq", TypeUInt64, Value{Kind: KindLong, Uint: 7})
	check("Temperature", TypeFloat, Value{Kind: KindFloat, Float: 21.5})
	check("Count", TypeInt32, Value{Kind: KindInt, Uint: 0xfffffffb}) // -5, two's complement
	check("Big", TypeInt64, Value{Kind: KindLong, Uint: 18446744064709551616})
	check("U64", TypeUInt64, Value{Kind: KindLong, Uint: 18446744073709551615})
	check("Pressure", TypeDouble, Value{Kind: KindDouble, Float: 142.7})
	check("Running", TypeBoolean, Value{Kind: KindBool, Bool: true})
	check("Node Control/Rebirth", TypeBoolean, Value{Kind: KindBool})
	check("Mode", TypeString, Value{Kind: KindString, Str: `auto "fast"`})
	check("Blob", TypeBytes, Value{Kind: KindBytes, Bytes: []byte{0, 1, 0xff}})
	check("Folder/Deep/Value", TypeUInt32, Value{Kind: KindInt, Uint: 4000000000})
	if int32(byName["Count"].Value.Uint) != -5 || int8(byName["Small"].Value.Uint) != -2 || int64(byName["Big"].Value.Uint) != -9000000000 {
		t.Error("signed values")
	}
	if m := byName["Nothing"]; !m.IsNull || m.Value.Kind != KindNone {
		t.Errorf("null metric %+v", m)
	}
	temp := byName["Temperature"]
	if temp.Alias != 2 || !temp.HasAlias || temp.Timestamp != 1760000000100 || !reflect.DeepEqual(temp.Properties, &PropertySet{
		Keys:   []string{"engUnit", "engHigh"},
		Values: []PropertyValue{{Type: TypeString, Value: Value{Kind: KindString, Str: "°C"}}, {Type: TypeDouble, Value: Value{Kind: KindDouble, Float: 150}}},
	}) {
		t.Errorf("temperature %+v %+v", temp, temp.Properties)
	}
	if ds := byName["Table"].Value.DataSet; !reflect.DeepEqual(ds, &DataSet{NumColumns: 2, Columns: []string{"id", "label"},
		Types: []DataType{TypeInt32, TypeString}, Rows: [][]Value{{{Kind: KindInt, Uint: 0xffffffff}, {Kind: KindString, Str: "x"}}}}) {
		t.Errorf("data set %+v", ds)
	}
	if tm := byName["Motor"].Value.Template; !reflect.DeepEqual(tm, &Template{TemplateRef: "MotorType",
		Metrics:    []Metric{{Name: "rpm", Datatype: TypeUInt32, Value: Value{Kind: KindInt, Uint: 1500}}},
		Parameters: []Parameter{{Name: "maxRpm", Type: TypeUInt32, Value: Value{Kind: KindInt, Uint: 3000}}}}) {
		t.Errorf("template %+v", tm)
	}
}

func TestDecodeGoldenData(t *testing.T) {
	p, err := Decode(mustHex(t, dataHex))
	if err != nil {
		t.Fatal(err)
	}
	want := []Metric{
		{Alias: 2, HasAlias: true, Timestamp: 1760000001000, HasTimestamp: true, Value: Value{Kind: KindFloat, Float: 22.25}},
		{Alias: 3, HasAlias: true, Value: Value{Kind: KindInt, Uint: 0xfffffffa}},
		{Alias: 99, HasAlias: true, Value: Value{Kind: KindInt, Uint: 1}},
	}
	if p.Seq != 1 || !reflect.DeepEqual(p.Metrics, want) {
		t.Fatalf("%+v", p.Metrics)
	}
}

func TestMetadata(t *testing.T) {
	p, err := Decode(mustHex(t, metadataHex))
	if err != nil {
		t.Fatal(err)
	}
	m := p.Metrics[0]
	want := &MetaData{IsMultiPart: true, ContentType: "application/zip", Size: 4096, Seq: 3, FileName: "config.zip",
		FileType: "zip", MD5: "d41d8cd98f00b204e9800998ecf8427e", Description: "part 3"}
	if m.Datatype != TypeFile || !reflect.DeepEqual(m.Metadata, want) || !bytes.Equal(m.Value.Bytes, []byte("PK\x03\x04")) {
		t.Fatalf("%+v %+v", m, m.Metadata)
	}
}

// Encoding writes fields in field-number order like the reference
// implementation: re-encoding a decoded golden payload gives the same bytes.
func TestEncodeMatchesReference(t *testing.T) {
	for name, h := range map[string]string{"birth": birthHex, "data": dataHex, "metadata": metadataHex} {
		in := mustHex(t, h)
		p, err := Decode(in)
		if err != nil {
			t.Fatal(err)
		}
		if out := Encode(p); !bytes.Equal(out, in) {
			t.Errorf("%s: re-encoded payload differs\n got  %x\n want %x", name, out, in)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	p := &Payload{Timestamp: 1, HasTimestamp: true, Seq: 255, HasSeq: true, UUID: "u", Body: []byte{1, 2},
		Metrics: []Metric{
			{Name: "a", Alias: 0, HasAlias: true, Datatype: TypeInt16, Value: Value{Kind: KindInt, Uint: 0xffff8000}},
			{Name: "b", Datatype: TypeDouble, IsHistorical: true, IsTransient: true, Value: Value{Kind: KindDouble, Float: -0.5}},
			{Name: "c", Datatype: TypeString, IsNull: true},
			{Name: "d", Datatype: TypeBoolean, Value: Value{Kind: KindBool, Bool: true}},
			{Name: "e", Datatype: TypeTemplate, Value: Value{Kind: KindTemplate, Template: &Template{Version: "1", IsDefinition: true,
				Metrics: []Metric{{Name: "x", Datatype: TypeBoolean, Value: Value{Kind: KindBool}}}}}},
			{Name: "f", Properties: &PropertySet{Keys: []string{"nested", "list", "null"}, Values: []PropertyValue{
				{Type: TypePropertySet, Value: Value{Kind: KindPropertySet, Properties: &PropertySet{Keys: []string{"k"},
					Values: []PropertyValue{{Type: TypeInt64, Value: Value{Kind: KindLong, Uint: 9}}}}}},
				{Type: TypePropertySetList, Value: Value{Kind: KindPropertySetList, PropertiesList: []PropertySet{{Keys: []string{"a"},
					Values: []PropertyValue{{Type: TypeBoolean, Value: Value{Kind: KindBool, Bool: true}}}}}}},
				{Type: TypeString, IsNull: true},
			}}, Datatype: TypeFloat, Value: Value{Kind: KindFloat, Float: 1.5}},
		}}
	got, err := Decode(Encode(p))
	if err != nil || !reflect.DeepEqual(got, p) {
		t.Fatalf("round trip: %v\n got  %+v\n want %+v", err, got, p)
	}
	if len(Encode(nil)) != 0 || len(Encode(&Payload{})) != 0 {
		t.Fatal("empty payloads encode to no bytes")
	}
	if empty, err := Decode(nil); err != nil || !reflect.DeepEqual(empty, &Payload{}) {
		t.Fatalf("empty payload: %+v %v", empty, err)
	}
}

func TestPackedTypesAndUnknownFields(t *testing.T) {
	// A data set with packed column types and unknown fields must decode.
	var w writer
	w.msg(2, func(w *writer) {
		w.str(1, "T")
		w.varint(4, uint64(TypeDataSet))
		w.msg(17, func(w *writer) {
			w.varint(1, 2)
			w.str(2, "a")
			w.str(2, "b")
			w.bytes(3, []byte{byte(TypeInt32), byte(TypeDouble)}) // packed varints
			w.varint(99, 5)
		})
		w.varint(19, 1) // a metric extension
	})
	w.varint(99, 1)
	p, err := Decode(w.b)
	if err != nil || len(p.Metrics) != 1 || !reflect.DeepEqual(p.Metrics[0].Value.DataSet.Types, []DataType{TypeInt32, TypeDouble}) {
		t.Fatalf("%+v %v", p, err)
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	for _, b := range [][]byte{
		{0x12, 0x05, 0x01},       // metric longer than the input
		{0x08},                   // truncated varint
		{0x00},                   // field number 0
		{0x12, 0x80},             // truncated length
		{0x0b},                   // group wire type (not used by Sparkplug)
		{0x12, 0x02, 0x65, 0x00}, // float field cut short
	} {
		if _, err := Decode(b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%x: %v", b, err)
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
	if _, err := Decode(top.b); !errors.Is(err, ErrMalformed) {
		t.Errorf("unbounded nesting: %v", err)
	}
}

func TestDecodeDoesNotAlias(t *testing.T) {
	in := mustHex(t, birthHex)
	p, _ := Decode(in)
	for i := range in {
		in[i] = 0
	}
	for _, m := range p.Metrics {
		if m.Name == "Blob" && !bytes.Equal(m.Value.Bytes, []byte{0, 1, 0xff}) {
			t.Fatal("decoded payload references the input")
		}
	}
}

func TestNames(t *testing.T) {
	if TypeDoubleArray.String() != "DoubleArray" || TypeUnknown.String() != "Unknown" || DataType(99).String() != "DataType(99)" {
		t.Error("DataType.String")
	}
	if KindPropertySetList.String() != "PropertySetList" || Kind(42).String() != "Kind(42)" {
		t.Error("Kind.String")
	}
}

// FuzzDecode: malformed input must return an error, never panic, and
// anything decoded must survive an encode/decode round trip unchanged.
// Validate and UnpackArray must not panic on whatever decodes.
func FuzzDecode(f *testing.F) {
	for _, h := range []string{birthHex, dataHex, metadataHex} {
		f.Add(mustHex(f, h))
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		p, err := Decode(b)
		if err != nil {
			if !errors.Is(err, ErrMalformed) || p != nil {
				t.Fatalf("error %v with payload %v", err, p)
			}
			return
		}
		_ = p.Validate() // must not panic either
		for _, m := range p.Metrics {
			if m.Datatype.IsArray() {
				_, _ = UnpackArray(m.Datatype, m.Value.Bytes)
			}
		}
		enc := Encode(p)
		p2, err := Decode(enc)
		if err != nil {
			t.Fatalf("re-encoded payload doesn't decode: %v", err)
		}
		if !bytes.Equal(Encode(p2), enc) {
			t.Fatal("encoding is not stable")
		}
	})
}
