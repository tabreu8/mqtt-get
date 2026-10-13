package sparkplug

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tabreu8/mqtt-get/internal/store"
)

// Sparkplug B data types.
const (
	TypeUnknown         = 0
	TypeInt8            = 1
	TypeInt16           = 2
	TypeInt32           = 3
	TypeInt64           = 4
	TypeUInt8           = 5
	TypeUInt16          = 6
	TypeUInt32          = 7
	TypeUInt64          = 8
	TypeFloat           = 9
	TypeDouble          = 10
	TypeBoolean         = 11
	TypeString          = 12
	TypeDateTime        = 13
	TypeText            = 14
	TypeUUID            = 15
	TypeDataSet         = 16
	TypeBytes           = 17
	TypeFile            = 18
	TypeTemplate        = 19
	TypePropertySet     = 20
	TypePropertySetList = 21
	TypeInt8Array       = 22
	TypeInt16Array      = 23
	TypeInt32Array      = 24
	TypeInt64Array      = 25
	TypeUInt8Array      = 26
	TypeUInt16Array     = 27
	TypeUInt32Array     = 28
	TypeUInt64Array     = 29
	TypeFloatArray      = 30
	TypeDoubleArray     = 31
	TypeBooleanArray    = 32
	TypeStringArray     = 33
	TypeDateTimeArray   = 34
)

var typeNames = []string{
	"Unknown", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
	"Float", "Double", "Boolean", "String", "DateTime", "Text", "UUID", "DataSet", "Bytes", "File",
	"Template", "PropertySet", "PropertySetList", "Int8Array", "Int16Array", "Int32Array", "Int64Array",
	"UInt8Array", "UInt16Array", "UInt32Array", "UInt64Array", "FloatArray", "DoubleArray",
	"BooleanArray", "StringArray", "DateTimeArray",
}

// TypeName returns the Sparkplug name of a data type.
func TypeName(t uint32) string {
	if int(t) < len(typeNames) {
		return typeNames[t]
	}
	return "Unknown"
}

// inferType guesses a data type from the value field that is set, for
// metrics whose type is unknown (no birth seen).
func inferType(v Value) uint32 {
	switch v.Kind {
	case KindInt:
		return TypeUInt32
	case KindLong:
		return TypeUInt64
	case KindFloat:
		return TypeFloat
	case KindDouble:
		return TypeDouble
	case KindBool:
		return TypeBoolean
	case KindString:
		return TypeString
	case KindBytes:
		return TypeBytes
	case KindDataSet:
		return TypeDataSet
	case KindTemplate:
		return TypeTemplate
	}
	return TypeUnknown
}

func appendFloat(buf []byte, f float64, bits int) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(buf, "null"...) // JSON has no NaN or infinity
	}
	return strconv.AppendFloat(buf, f, 'g', -1, bits)
}

func appendTime(buf []byte, ms int64) []byte {
	buf = append(buf, '"')
	buf = time.UnixMilli(ms).UTC().AppendFormat(buf, time.RFC3339Nano)
	return append(buf, '"')
}

// AppendValue appends v, interpreted as data type t, as JSON.
func AppendValue(buf []byte, t uint32, v Value) []byte {
	if t == TypeUnknown {
		t = inferType(v)
	}
	switch t {
	case TypeInt8:
		return strconv.AppendInt(buf, int64(int8(v.U)), 10)
	case TypeInt16:
		return strconv.AppendInt(buf, int64(int16(v.U)), 10)
	case TypeInt32:
		return strconv.AppendInt(buf, int64(int32(v.U)), 10)
	case TypeInt64:
		return strconv.AppendInt(buf, int64(v.U), 10)
	case TypeUInt8, TypeUInt16, TypeUInt32, TypeUInt64:
		return strconv.AppendUint(buf, v.U, 10)
	case TypeFloat:
		if v.Kind == KindDouble {
			return appendFloat(buf, v.F, 64)
		}
		return appendFloat(buf, v.F, 32)
	case TypeDouble:
		if v.Kind == KindFloat {
			return appendFloat(buf, v.F, 32)
		}
		return appendFloat(buf, v.F, 64)
	case TypeBoolean:
		return strconv.AppendBool(buf, v.U != 0)
	case TypeDateTime:
		return appendTime(buf, int64(v.U))
	case TypeString, TypeText, TypeUUID:
		return store.AppendString(buf, v.S)
	case TypeBytes, TypeFile:
		buf = append(buf, '"')
		buf = base64.StdEncoding.AppendEncode(buf, v.B)
		return append(buf, '"')
	case TypeDataSet:
		if v.DataSet != nil {
			return appendDataSet(buf, v.DataSet)
		}
	case TypeTemplate:
		if v.Template != nil {
			return appendTemplate(buf, v.Template)
		}
	}
	if t >= TypeInt8Array && t <= TypeDateTimeArray && v.Kind == KindBytes {
		if out, ok := appendArray(buf, t, v.B); ok {
			return out
		}
	}
	return appendKind(buf, v)
}

// appendKind renders a value by its wire kind (type unknown or mismatched).
func appendKind(buf []byte, v Value) []byte {
	switch v.Kind {
	case KindInt, KindLong:
		return strconv.AppendUint(buf, v.U, 10)
	case KindFloat:
		return appendFloat(buf, v.F, 32)
	case KindDouble:
		return appendFloat(buf, v.F, 64)
	case KindBool:
		return strconv.AppendBool(buf, v.U != 0)
	case KindString:
		return store.AppendString(buf, v.S)
	case KindBytes:
		buf = append(buf, '"')
		buf = base64.StdEncoding.AppendEncode(buf, v.B)
		return append(buf, '"')
	case KindDataSet:
		return appendDataSet(buf, v.DataSet)
	case KindTemplate:
		return appendTemplate(buf, v.Template)
	case KindPropertySet:
		return AppendProperties(buf, v.Set)
	case KindPropertySetList:
		buf = append(buf, '[')
		for i := range v.Sets {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = AppendProperties(buf, &v.Sets[i])
		}
		return append(buf, ']')
	}
	return append(buf, "null"...)
}

// appendArray decodes a packed array (Sparkplug 3.0: little-endian values
// in bytes_value). ok is false when the length doesn't fit the type.
func appendArray(buf []byte, t uint32, b []byte) ([]byte, bool) {
	size := map[uint32]int{
		TypeInt8Array: 1, TypeUInt8Array: 1, TypeInt16Array: 2, TypeUInt16Array: 2,
		TypeInt32Array: 4, TypeUInt32Array: 4, TypeFloatArray: 4,
		TypeInt64Array: 8, TypeUInt64Array: 8, TypeDoubleArray: 8, TypeDateTimeArray: 8,
	}[t]
	switch t {
	case TypeBooleanArray:
		// uint32 count, then the booleans packed MSB first.
		if len(b) < 4 {
			return buf, false
		}
		n := int(binary.LittleEndian.Uint32(b))
		bits := b[4:]
		if n > len(bits)*8 {
			return buf, false
		}
		buf = append(buf, '[')
		for i := 0; i < n; i++ {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = strconv.AppendBool(buf, bits[i/8]&(0x80>>(i%8)) != 0)
		}
		return append(buf, ']'), true
	case TypeStringArray:
		// NUL-terminated strings.
		if len(b) > 0 && b[len(b)-1] != 0 {
			return buf, false
		}
		buf = append(buf, '[')
		for i, s := range bytes.Split(b[:max(len(b)-1, 0)], []byte{0}) {
			if len(b) == 0 {
				break
			}
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = store.AppendString(buf, string(s))
		}
		return append(buf, ']'), true
	}
	if size == 0 || len(b)%size != 0 {
		return buf, false
	}
	le := binary.LittleEndian
	buf = append(buf, '[')
	for i := 0; i < len(b); i += size {
		if i > 0 {
			buf = append(buf, ',')
		}
		switch t {
		case TypeInt8Array:
			buf = strconv.AppendInt(buf, int64(int8(b[i])), 10)
		case TypeUInt8Array:
			buf = strconv.AppendUint(buf, uint64(b[i]), 10)
		case TypeInt16Array:
			buf = strconv.AppendInt(buf, int64(int16(le.Uint16(b[i:]))), 10)
		case TypeUInt16Array:
			buf = strconv.AppendUint(buf, uint64(le.Uint16(b[i:])), 10)
		case TypeInt32Array:
			buf = strconv.AppendInt(buf, int64(int32(le.Uint32(b[i:]))), 10)
		case TypeUInt32Array:
			buf = strconv.AppendUint(buf, uint64(le.Uint32(b[i:])), 10)
		case TypeFloatArray:
			buf = appendFloat(buf, float64(math.Float32frombits(le.Uint32(b[i:]))), 32)
		case TypeInt64Array:
			buf = strconv.AppendInt(buf, int64(le.Uint64(b[i:])), 10)
		case TypeUInt64Array:
			buf = strconv.AppendUint(buf, le.Uint64(b[i:]), 10)
		case TypeDoubleArray:
			buf = appendFloat(buf, math.Float64frombits(le.Uint64(b[i:])), 64)
		case TypeDateTimeArray:
			buf = appendTime(buf, int64(le.Uint64(b[i:])))
		}
	}
	return append(buf, ']'), true
}

// appendDataSet renders {"columns":[...],"types":[...],"rows":[[...],...]}.
func appendDataSet(buf []byte, ds *DataSet) []byte {
	if ds == nil {
		return append(buf, "null"...)
	}
	buf = append(buf, `{"columns":[`...)
	for i, c := range ds.Columns {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = store.AppendString(buf, c)
	}
	buf = append(buf, `],"types":[`...)
	for i, t := range ds.Types {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = store.AppendString(buf, TypeName(t))
	}
	buf = append(buf, `],"rows":[`...)
	for i, row := range ds.Rows {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '[')
		for j, v := range row {
			if j > 0 {
				buf = append(buf, ',')
			}
			var t uint32
			if j < len(ds.Types) {
				t = ds.Types[j]
			}
			buf = AppendValue(buf, t, v)
		}
		buf = append(buf, ']')
	}
	return append(buf, "]}"...)
}

// appendTemplate renders {"template_ref":...,"metrics":{name:value},"parameters":{name:value}}.
func appendTemplate(buf []byte, t *Template) []byte {
	if t == nil {
		return append(buf, "null"...)
	}
	buf = append(buf, '{')
	if t.TemplateRef != "" {
		buf = append(buf, `"template_ref":`...)
		buf = store.AppendString(buf, t.TemplateRef)
		buf = append(buf, ',')
	}
	if t.Version != "" {
		buf = append(buf, `"version":`...)
		buf = store.AppendString(buf, t.Version)
		buf = append(buf, ',')
	}
	if t.IsDefinition {
		buf = append(buf, `"is_definition":true,`...)
	}
	buf = append(buf, `"metrics":{`...)
	for i := range t.Metrics {
		m := &t.Metrics[i]
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = store.AppendString(buf, m.Name)
		buf = append(buf, ':')
		if m.IsNull {
			buf = append(buf, "null"...)
		} else {
			buf = AppendValue(buf, m.Datatype, m.Value)
		}
	}
	buf = append(buf, '}')
	if len(t.Parameters) > 0 {
		buf = append(buf, `,"parameters":{`...)
		for i, p := range t.Parameters {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = store.AppendString(buf, p.Name)
			buf = append(buf, ':')
			buf = AppendValue(buf, p.Type, p.Value)
		}
		buf = append(buf, '}')
	}
	return append(buf, '}')
}

// AppendProperties renders a property set as a JSON object.
func AppendProperties(buf []byte, ps *PropertySet) []byte {
	if ps == nil {
		return append(buf, "null"...)
	}
	buf = append(buf, '{')
	for i, k := range ps.Keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = store.AppendString(buf, k)
		buf = append(buf, ':')
		if i >= len(ps.Values) || ps.Values[i].IsNull {
			buf = append(buf, "null"...)
			continue
		}
		pv := ps.Values[i]
		switch pv.Value.Kind {
		case KindPropertySet, KindPropertySetList:
			buf = appendKind(buf, pv.Value)
		default:
			buf = AppendValue(buf, pv.Type, pv.Value)
		}
	}
	return append(buf, '}')
}

// Property returns the string value of property key (case-insensitive).
func (ps *PropertySet) Property(key string) (string, bool) {
	if ps == nil {
		return "", false
	}
	for i, k := range ps.Keys {
		if strings.EqualFold(k, key) && i < len(ps.Values) && ps.Values[i].Value.Kind == KindString {
			return ps.Values[i].Value.S, true
		}
	}
	return "", false
}

// --- values from JSON (commands) ---

// ValueFromJSON converts a command payload to a Sparkplug value of data
// type t. The payload is a JSON value (72.5, true, "text"), an object with
// a "value" member, or plain text.
func ValueFromJSON(t uint32, payload []byte) (Value, error) {
	var raw any = string(payload)
	if trimmed := bytes.TrimSpace(payload); json.Valid(trimmed) && len(trimmed) > 0 {
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err == nil {
			raw = v
			if obj, ok := v.(map[string]any); ok {
				if inner, ok := obj["value"]; ok {
					raw = inner
				}
			}
		}
	}
	switch t {
	case TypeInt8, TypeInt16, TypeInt32, TypeInt64:
		bits := map[uint32]int{TypeInt8: 8, TypeInt16: 16, TypeInt32: 32, TypeInt64: 64}[t]
		n, err := strconv.ParseInt(numText(raw), 10, bits)
		if err != nil {
			return Value{}, fmt.Errorf("%s value: %v", TypeName(t), errText(raw, err))
		}
		if t == TypeInt64 {
			return Value{Kind: KindLong, U: uint64(n)}, nil
		}
		return Value{Kind: KindInt, U: uint64(uint32(int32(n)))}, nil // two's complement
	case TypeUInt8, TypeUInt16, TypeUInt32, TypeUInt64:
		bits := map[uint32]int{TypeUInt8: 8, TypeUInt16: 16, TypeUInt32: 32, TypeUInt64: 64}[t]
		n, err := strconv.ParseUint(numText(raw), 10, bits)
		if err != nil {
			return Value{}, fmt.Errorf("%s value: %v", TypeName(t), errText(raw, err))
		}
		if t == TypeUInt64 {
			return Value{Kind: KindLong, U: n}, nil
		}
		return Value{Kind: KindInt, U: n}, nil
	case TypeFloat, TypeDouble:
		f, err := strconv.ParseFloat(numText(raw), 64)
		if err != nil {
			return Value{}, fmt.Errorf("%s value: %v", TypeName(t), errText(raw, err))
		}
		if t == TypeFloat {
			if math.Abs(f) > math.MaxFloat32 {
				return Value{}, fmt.Errorf("Float value %g out of range", f)
			}
			return Value{Kind: KindFloat, F: float64(float32(f))}, nil
		}
		return Value{Kind: KindDouble, F: f}, nil
	case TypeBoolean:
		switch x := raw.(type) {
		case bool:
			return Value{Kind: KindBool, U: b2u(x)}, nil
		default:
			s := strings.ToLower(strings.TrimSpace(numText(raw)))
			switch s {
			case "true", "1", "on":
				return Value{Kind: KindBool, U: 1}, nil
			case "false", "0", "off":
				return Value{Kind: KindBool, U: 0}, nil
			}
			return Value{}, fmt.Errorf("Boolean value: want true or false, got %s", textOf(raw))
		}
	case TypeString, TypeText, TypeUUID:
		if s, ok := raw.(string); ok {
			return Value{Kind: KindString, S: s}, nil
		}
		return Value{Kind: KindString, S: strings.TrimSpace(string(payload))}, nil
	case TypeDateTime:
		if s, ok := raw.(string); ok {
			if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return Value{Kind: KindLong, U: uint64(ts.UnixMilli())}, nil
			}
		}
		n, err := strconv.ParseInt(numText(raw), 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("DateTime value: want an RFC 3339 timestamp or milliseconds since the epoch, got %s", textOf(raw))
		}
		return Value{Kind: KindLong, U: uint64(n)}, nil
	case TypeBytes, TypeFile:
		s, ok := raw.(string)
		if !ok {
			return Value{}, fmt.Errorf("%s value: want a base64 string", TypeName(t))
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return Value{}, fmt.Errorf("%s value: invalid base64: %v", TypeName(t), err)
		}
		return Value{Kind: KindBytes, B: b}, nil
	}
	return Value{}, fmt.Errorf("writing %s metrics is not supported", TypeName(t))
}

// numText returns the text of a JSON number or string.
func numText(v any) string {
	switch x := v.(type) {
	case json.Number:
		return x.String()
	case string:
		return strings.TrimSpace(x)
	case bool:
		if x {
			return "1"
		}
		return "0"
	}
	return fmt.Sprint(v)
}

func textOf(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func errText(v any, err error) string {
	if ne, ok := err.(*strconv.NumError); ok {
		if ne.Err == strconv.ErrRange {
			return "out of range: " + textOf(v)
		}
		return "not a number: " + textOf(v)
	}
	return err.Error()
}
