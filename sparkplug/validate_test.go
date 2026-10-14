package sparkplug

import (
	"strings"
	"testing"
)

func TestValidateGolden(t *testing.T) {
	for name, h := range map[string]string{"birth": birthHex, "data": dataHex, "metadata": metadataHex} {
		p, err := Decode(mustHex(t, h))
		if err != nil {
			t.Fatal(err)
		}
		if err := p.Validate(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestValidateRejects(t *testing.T) {
	m := func(name string, dt DataType, v Value) Metric { return Metric{Name: name, Datatype: dt, Value: v} }
	cases := []struct {
		p    Payload
		want string
	}{
		{Payload{Seq: 256, HasSeq: true}, "seq 256 is above 255"},
		{Payload{Metrics: []Metric{{Value: Value{Kind: KindBool}}}}, "no name and no alias"},
		{Payload{Metrics: []Metric{m("a", TypeInt8, Value{Kind: KindInt, Uint: 300})}}, "300 is out of range for Int8"},
		{Payload{Metrics: []Metric{m("a", TypeUInt16, Value{Kind: KindLong, Uint: 70000})}}, "out of range for UInt16"},
		{Payload{Metrics: []Metric{m("a", TypeInt32, Value{Kind: KindString, Str: "1"})}}, "Int32 value carried as String"},
		{Payload{Metrics: []Metric{m("a", TypeBoolean, Value{Kind: KindInt})}}, "Boolean value carried as Int"},
		{Payload{Metrics: []Metric{m("a", TypeDouble, Value{})}}, "carries no value and is not null"},
		{Payload{Metrics: []Metric{{Name: "a", Datatype: TypeDouble, IsNull: true, Value: Value{Kind: KindDouble}}}}, "null metric carries a Double value"},
		{Payload{Metrics: []Metric{m("a", DataType(99), Value{Kind: KindInt})}}, "DataType(99) is not a metric data type"},
		{Payload{Metrics: []Metric{m("a", TypePropertySet, Value{Kind: KindPropertySet})}}, "not a metric data type"},
		{Payload{Metrics: []Metric{m("a", TypeInt16Array, Value{Kind: KindBytes, Bytes: []byte{1}})}}, "Int16Array of 1 bytes"},
		{Payload{Metrics: []Metric{m("a", TypeDataSet, Value{Kind: KindDataSet, DataSet: &DataSet{NumColumns: 2, Columns: []string{"x"}, Types: []DataType{TypeInt32}}})}}, "2 columns but 1 names and 1 types"},
		{Payload{Metrics: []Metric{m("a", TypeDataSet, Value{Kind: KindDataSet, DataSet: &DataSet{NumColumns: 1, Columns: []string{"x"},
			Types: []DataType{TypeInt32}, Rows: [][]Value{{}, {{Kind: KindString}}}}})}}, "row 0 has 0 values"},
		{Payload{Metrics: []Metric{m("a", TypeDataSet, Value{Kind: KindDataSet, DataSet: &DataSet{NumColumns: 1, Columns: []string{"x"},
			Types: []DataType{TypeInt32}, Rows: [][]Value{{{Kind: KindString}}}}})}}, `row 0 column "x": Int32 value carried as String`},
		{Payload{Metrics: []Metric{m("a", TypeTemplate, Value{Kind: KindTemplate, Template: &Template{}})}}, "instance without template_ref"},
		{Payload{Metrics: []Metric{m("a", TypeTemplate, Value{Kind: KindTemplate, Template: &Template{TemplateRef: "T",
			Metrics: []Metric{m("x", TypeUInt8, Value{Kind: KindInt, Uint: 256})}}})}}, `member 0 ("x"): 256 is out of range for UInt8`},
		{Payload{Metrics: []Metric{{Name: "a", Datatype: TypeBoolean, Value: Value{Kind: KindBool}, Properties: &PropertySet{
			Keys: []string{"engUnit"}, Values: []PropertyValue{{Type: TypeString, Value: Value{Kind: KindInt}}}}}}}, `properties "engUnit": String value carried as Int`},
		{Payload{Metrics: []Metric{{Name: "a", Datatype: TypeBoolean, Value: Value{Kind: KindBool}, Properties: &PropertySet{
			Keys: []string{"a", "b"}, Values: []PropertyValue{{Type: TypeString, Value: Value{Kind: KindString}}}}}}}, "2 keys for 1 values"},
	}
	for _, c := range cases {
		err := c.p.Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%+v: %v, want %q", c.p.Metrics, err, c.want)
		}
	}
}

func TestValidateAccepts(t *testing.T) {
	// Encodings seen in the wild: unsigned or sign-extended small signed
	// integers, UInt32 in long_value, Float in double_value.
	p := Payload{Seq: 255, HasSeq: true, Metrics: []Metric{
		{Name: "a", Datatype: TypeInt8, Value: Value{Kind: KindInt, Uint: 248}},        // -8, not sign-extended
		{Name: "b", Datatype: TypeInt8, Value: Value{Kind: KindInt, Uint: 0xfffffff8}}, // -8, sign-extended
		{Name: "c", Datatype: TypeUInt32, Value: Value{Kind: KindLong, Uint: 4000000000}},
		{Name: "d", Datatype: TypeFloat, Value: Value{Kind: KindDouble, Float: 1}},
		{Alias: 7, HasAlias: true, Value: Value{Kind: KindDouble, Float: 1}}, // typed by its birth
		{Name: "n", Datatype: TypeString, IsNull: true},
		{Name: "ds", Datatype: TypeDataSet, Value: Value{Kind: KindDataSet, DataSet: &DataSet{NumColumns: 1, Columns: []string{"x"},
			Types: []DataType{TypeString}, Rows: [][]Value{{{}}, {{Kind: KindString, Str: "ok"}}}}}}, // a null cell
	}}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	// Many problems: the report is capped.
	var many Payload
	for i := 0; i < 50; i++ {
		many.Metrics = append(many.Metrics, Metric{Value: Value{Kind: KindBool}})
	}
	if n := strings.Count(many.Validate().Error(), "\n") + 1; n != 20 {
		t.Fatalf("%d problems reported, want 20", n)
	}
}

func BenchmarkValidateDData(b *testing.B) {
	p := &Payload{Seq: 1, HasSeq: true}
	for i := uint64(0); i < 10; i++ {
		p.Metrics = append(p.Metrics, Metric{Alias: i, HasAlias: true, Datatype: TypeDouble, Value: Value{Kind: KindDouble, Float: 1}})
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if p.Validate() != nil {
			b.Fatal()
		}
	}
}
