package sparkplug

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestArraysRoundTrip(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)
	cases := map[DataType]any{
		TypeInt8Array:     []int8{-128, 0, 127},
		TypeUInt8Array:    []uint8{0, 255},
		TypeInt16Array:    []int16{-32768, 1, 32767},
		TypeUInt16Array:   []uint16{0, 65535},
		TypeInt32Array:    []int32{-2147483648, 2147483647},
		TypeUInt32Array:   []uint32{4294967295},
		TypeFloatArray:    []float32{0.5, -1.25},
		TypeInt64Array:    []int64{-9223372036854775808, 5},
		TypeUInt64Array:   []uint64{18446744073709551615},
		TypeDoubleArray:   []float64{3.5, -0.001},
		TypeDateTimeArray: []time.Time{when},
		TypeBooleanArray:  []bool{true, false, true, true, false, false, false, false, true},
		TypeStringArray:   []string{"a", "", "c d"},
	}
	for typ, in := range cases {
		b, err := PackArray(typ, in)
		if err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		out, err := UnpackArray(typ, b)
		if err != nil || !reflect.DeepEqual(out, in) {
			t.Errorf("%s: %v %v, want %v", typ, out, err, in)
		}
		if !typ.IsArray() {
			t.Errorf("%s.IsArray", typ)
		}
	}
	// Empty arrays.
	for typ := range cases {
		b, err := PackArray(typ, reflect.MakeSlice(reflect.TypeOf(cases[typ]), 0, 0).Interface())
		if err != nil {
			t.Fatalf("%s empty: %v", typ, err)
		}
		if out, err := UnpackArray(typ, b); err != nil || reflect.ValueOf(out).Len() != 0 {
			t.Errorf("%s empty: %v %v", typ, out, err)
		}
	}
}

// The packed forms decoded from the golden birth (encoded by the reference
// implementation, laid out as the specification describes).
func TestArraysGolden(t *testing.T) {
	p, err := Decode(mustHex(t, birthHex))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"Ints":   []int32{-1, 2, 300000},
		"Floats": []float32{1.5, -2.25},
		"Bools":  []bool{true, false, true},
		"Names":  []string{"ab", "c"},
	}
	for _, m := range p.Metrics {
		w, ok := want[m.Name]
		if !ok {
			continue
		}
		got, err := UnpackArray(m.Datatype, m.Value.Bytes)
		if err != nil || !reflect.DeepEqual(got, w) {
			t.Errorf("%s: %v %v, want %v", m.Name, got, err, w)
		}
		// Packing gives back the reference bytes.
		if b, _ := PackArray(m.Datatype, w); !bytes.Equal(b, m.Value.Bytes) {
			t.Errorf("%s: packed %x, reference %x", m.Name, b, m.Value.Bytes)
		}
		delete(want, m.Name)
	}
	if len(want) != 0 {
		t.Fatalf("not found: %v", want)
	}
}

func TestArraysMalformed(t *testing.T) {
	for _, c := range []struct {
		t DataType
		b []byte
	}{
		{TypeInt16Array, []byte{1}},
		{TypeDoubleArray, make([]byte, 12)},
		{TypeBooleanArray, []byte{1, 0}},
		{TypeBooleanArray, []byte{9, 0, 0, 0, 0xff}}, // 9 booleans in 8 bits
		{TypeStringArray, []byte("no terminator")},
	} {
		if _, err := UnpackArray(c.t, c.b); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s %x: %v", c.t, c.b, err)
		}
	}
	if _, err := UnpackArray(TypeInt32, nil); err == nil || !strings.Contains(err.Error(), "not an array type") {
		t.Errorf("scalar type: %v", err)
	}
	if _, err := PackArray(TypeInt32Array, []int64{1}); err == nil || !strings.Contains(err.Error(), "needs []int32") {
		t.Errorf("wrong slice type: %v", err)
	}
	if _, err := PackArray(TypeStringArray, []string{"a\x00b"}); err == nil {
		t.Error("NUL in a string accepted")
	}
	if _, err := PackArray(TypeBoolean, []bool{true}); err == nil {
		t.Error("scalar type accepted")
	}
	// Padding after the packed booleans is tolerated.
	if out, err := UnpackArray(TypeBooleanArray, []byte{1, 0, 0, 0, 0x80, 0}); err != nil || !reflect.DeepEqual(out, []bool{true}) {
		t.Errorf("padding: %v %v", out, err)
	}
}
