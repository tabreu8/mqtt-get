package sparkplug

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tabreu8/mqtt-get/internal/store"
	spb "github.com/tabreu8/mqtt-get/sparkplug"
)

// inferType guesses a data type from the value field that is set, for
// metrics whose type is unknown (no birth seen).
func inferType(v spb.Value) spb.DataType {
	switch v.Kind {
	case spb.KindInt:
		return spb.TypeUInt32
	case spb.KindLong:
		return spb.TypeUInt64
	case spb.KindFloat:
		return spb.TypeFloat
	case spb.KindDouble:
		return spb.TypeDouble
	case spb.KindBool:
		return spb.TypeBoolean
	case spb.KindString:
		return spb.TypeString
	case spb.KindBytes:
		return spb.TypeBytes
	case spb.KindDataSet:
		return spb.TypeDataSet
	case spb.KindTemplate:
		return spb.TypeTemplate
	}
	return spb.TypeUnknown
}

func appendFloat(buf []byte, f float64, bits int) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(buf, "null"...) // JSON has no NaN or infinity
	}
	return strconv.AppendFloat(buf, f, 'g', -1, bits)
}

// appendTime writes ms (milliseconds since the Unix epoch) as a quoted
// RFC 3339 time in UTC, exactly as time.RFC3339Nano formats it
// ("2026-10-13T09:15:02.311Z", trailing zeros of the fraction dropped),
// several times faster than the time package.
func appendTime(buf []byte, ms int64) []byte {
	days, msOfDay := ms/86_400_000, ms%86_400_000
	if msOfDay < 0 {
		days, msOfDay = days-1, msOfDay+86_400_000
	}
	y, m, d := civil(days)
	if y < 0 || y > 9999 {
		buf = append(buf, '"')
		buf = time.UnixMilli(ms).UTC().AppendFormat(buf, time.RFC3339Nano)
		return append(buf, '"')
	}
	sec := msOfDay / 1000
	var b [26]byte
	b[0] = '"'
	put4(b[1:], y)
	b[5] = '-'
	put2(b[6:], m)
	b[8] = '-'
	put2(b[9:], d)
	b[11] = 'T'
	put2(b[12:], int(sec/3600))
	b[14] = ':'
	put2(b[15:], int(sec/60%60))
	b[17] = ':'
	put2(b[18:], int(sec%60))
	n := 20
	if frac := int(msOfDay % 1000); frac != 0 {
		b[20] = '.'
		b[21], b[22], b[23] = byte('0'+frac/100), byte('0'+frac/10%10), byte('0'+frac%10)
		n = 24
		for b[n-1] == '0' {
			n--
		}
	}
	b[n], b[n+1] = 'Z', '"'
	return append(buf, b[:n+2]...)
}

// civil converts days since 1970-01-01 to a proleptic Gregorian date
// (Howard Hinnant's algorithm).
func civil(days int64) (y, m, d int) {
	z := days + 719468
	era := z / 146097
	if z < 0 && z%146097 != 0 {
		era--
	}
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = int(doy - (153*mp+2)/5 + 1)
	m = int(mp + 3)
	if m > 12 {
		m -= 12
	}
	y = int(yoe + era*400)
	if m <= 2 {
		y++
	}
	return y, m, d
}

func put2(b []byte, v int) { b[0], b[1] = byte('0'+v/10), byte('0'+v%10) }

func put4(b []byte, v int) {
	b[0], b[1], b[2], b[3] = byte('0'+v/1000), byte('0'+v/100%10), byte('0'+v/10%10), byte('0'+v%10)
}

// AppendValue appends v, interpreted as data type t, as JSON.
func AppendValue(buf []byte, t spb.DataType, v spb.Value) []byte {
	if v.Kind == spb.KindExtension {
		// Application-defined: the encoded message, as is.
		buf = append(buf, `{"extension":"`...)
		buf = base64.StdEncoding.AppendEncode(buf, v.Bytes)
		return append(buf, `"}`...)
	}
	if t == spb.TypeUnknown {
		t = inferType(v)
	}
	switch t {
	case spb.TypeInt8:
		return strconv.AppendInt(buf, int64(int8(v.Uint)), 10)
	case spb.TypeInt16:
		return strconv.AppendInt(buf, int64(int16(v.Uint)), 10)
	case spb.TypeInt32:
		return strconv.AppendInt(buf, int64(int32(v.Uint)), 10)
	case spb.TypeInt64:
		return strconv.AppendInt(buf, int64(v.Uint), 10)
	case spb.TypeUInt8, spb.TypeUInt16, spb.TypeUInt32, spb.TypeUInt64:
		return strconv.AppendUint(buf, v.Uint, 10)
	case spb.TypeFloat:
		if v.Kind == spb.KindDouble {
			return appendFloat(buf, v.Float, 64)
		}
		return appendFloat(buf, v.Float, 32)
	case spb.TypeDouble:
		if v.Kind == spb.KindFloat {
			return appendFloat(buf, v.Float, 32)
		}
		return appendFloat(buf, v.Float, 64)
	case spb.TypeBoolean:
		return strconv.AppendBool(buf, v.Bool)
	case spb.TypeDateTime:
		return appendTime(buf, int64(v.Uint))
	case spb.TypeString, spb.TypeText, spb.TypeUUID:
		return store.AppendString(buf, v.Str)
	case spb.TypeBytes, spb.TypeFile:
		buf = append(buf, '"')
		buf = base64.StdEncoding.AppendEncode(buf, v.Bytes)
		return append(buf, '"')
	case spb.TypeDataSet:
		if v.DataSet != nil {
			return appendDataSet(buf, v.DataSet)
		}
	case spb.TypeTemplate:
		if v.Template != nil {
			return appendTemplate(buf, v.Template)
		}
	}
	if t >= spb.TypeInt8Array && t <= spb.TypeDateTimeArray && v.Kind == spb.KindBytes {
		if out, ok := appendArray(buf, t, v.Bytes); ok {
			return out
		}
	}
	return appendKind(buf, v)
}

// appendKind renders a value by its wire kind (type unknown or mismatched).
func appendKind(buf []byte, v spb.Value) []byte {
	switch v.Kind {
	case spb.KindInt, spb.KindLong:
		return strconv.AppendUint(buf, v.Uint, 10)
	case spb.KindFloat:
		return appendFloat(buf, v.Float, 32)
	case spb.KindDouble:
		return appendFloat(buf, v.Float, 64)
	case spb.KindBool:
		return strconv.AppendBool(buf, v.Bool)
	case spb.KindString:
		return store.AppendString(buf, v.Str)
	case spb.KindBytes:
		buf = append(buf, '"')
		buf = base64.StdEncoding.AppendEncode(buf, v.Bytes)
		return append(buf, '"')
	case spb.KindDataSet:
		return appendDataSet(buf, v.DataSet)
	case spb.KindTemplate:
		return appendTemplate(buf, v.Template)
	case spb.KindPropertySet:
		return AppendProperties(buf, v.Properties)
	case spb.KindPropertySetList:
		buf = append(buf, '[')
		for i := range v.PropertiesList {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = AppendProperties(buf, &v.PropertiesList[i])
		}
		return append(buf, ']')
	}
	return append(buf, "null"...)
}

// appendArray renders a packed array value as a JSON array. ok is false
// when the bytes don't fit the type (spb.UnpackArray fails).
func appendArray(buf []byte, t spb.DataType, b []byte) ([]byte, bool) {
	vals, err := spb.UnpackArray(t, b)
	if err != nil {
		return buf, false
	}
	buf = append(buf, '[')
	switch x := vals.(type) {
	case []int8:
		buf = appendEach(buf, x, func(b []byte, v int8) []byte { return strconv.AppendInt(b, int64(v), 10) })
	case []int16:
		buf = appendEach(buf, x, func(b []byte, v int16) []byte { return strconv.AppendInt(b, int64(v), 10) })
	case []int32:
		buf = appendEach(buf, x, func(b []byte, v int32) []byte { return strconv.AppendInt(b, int64(v), 10) })
	case []int64:
		buf = appendEach(buf, x, func(b []byte, v int64) []byte { return strconv.AppendInt(b, v, 10) })
	case []uint8:
		buf = appendEach(buf, x, func(b []byte, v uint8) []byte { return strconv.AppendUint(b, uint64(v), 10) })
	case []uint16:
		buf = appendEach(buf, x, func(b []byte, v uint16) []byte { return strconv.AppendUint(b, uint64(v), 10) })
	case []uint32:
		buf = appendEach(buf, x, func(b []byte, v uint32) []byte { return strconv.AppendUint(b, uint64(v), 10) })
	case []uint64:
		buf = appendEach(buf, x, func(b []byte, v uint64) []byte { return strconv.AppendUint(b, v, 10) })
	case []float32:
		buf = appendEach(buf, x, func(b []byte, v float32) []byte { return appendFloat(b, float64(v), 32) })
	case []float64:
		buf = appendEach(buf, x, func(b []byte, v float64) []byte { return appendFloat(b, v, 64) })
	case []bool:
		buf = appendEach(buf, x, strconv.AppendBool)
	case []string:
		buf = appendEach(buf, x, store.AppendString)
	case []time.Time:
		buf = appendEach(buf, x, func(b []byte, v time.Time) []byte { return appendTime(b, v.UnixMilli()) })
	}
	return append(buf, ']'), true
}

func appendEach[T any](buf []byte, vals []T, f func([]byte, T) []byte) []byte {
	for i, v := range vals {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = f(buf, v)
	}
	return buf
}

// appendDataSet renders {"columns":[...],"types":[...],"rows":[[...],...]}.
func appendDataSet(buf []byte, ds *spb.DataSet) []byte {
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
		buf = store.AppendString(buf, t.String())
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
			var t spb.DataType
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
func appendTemplate(buf []byte, t *spb.Template) []byte {
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
func AppendProperties(buf []byte, ps *spb.PropertySet) []byte {
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
		case spb.KindPropertySet, spb.KindPropertySetList:
			buf = appendKind(buf, pv.Value)
		default:
			buf = AppendValue(buf, pv.Type, pv.Value)
		}
	}
	return append(buf, '}')
}

// --- values from JSON (commands) ---

// ValueFromJSON converts a command payload to a Sparkplug value of data
// type t. The payload is a JSON value (72.5, true, "text"), an object with
// a "value" member, or plain text.
func ValueFromJSON(t spb.DataType, payload []byte) (spb.Value, error) {
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
	case spb.TypeInt8, spb.TypeInt16, spb.TypeInt32, spb.TypeInt64:
		bits := map[spb.DataType]int{spb.TypeInt8: 8, spb.TypeInt16: 16, spb.TypeInt32: 32, spb.TypeInt64: 64}[t]
		n, err := strconv.ParseInt(numText(raw), 10, bits)
		if err != nil {
			return spb.Value{}, fmt.Errorf("%s value: %v", t.String(), errText(raw, err))
		}
		if t == spb.TypeInt64 {
			return spb.Value{Kind: spb.KindLong, Uint: uint64(n)}, nil
		}
		return spb.Value{Kind: spb.KindInt, Uint: uint64(uint32(int32(n)))}, nil // two's complement
	case spb.TypeUInt8, spb.TypeUInt16, spb.TypeUInt32, spb.TypeUInt64:
		bits := map[spb.DataType]int{spb.TypeUInt8: 8, spb.TypeUInt16: 16, spb.TypeUInt32: 32, spb.TypeUInt64: 64}[t]
		n, err := strconv.ParseUint(numText(raw), 10, bits)
		if err != nil {
			return spb.Value{}, fmt.Errorf("%s value: %v", t.String(), errText(raw, err))
		}
		if t == spb.TypeUInt64 {
			return spb.Value{Kind: spb.KindLong, Uint: n}, nil
		}
		return spb.Value{Kind: spb.KindInt, Uint: n}, nil
	case spb.TypeFloat, spb.TypeDouble:
		f, err := strconv.ParseFloat(numText(raw), 64)
		if err != nil {
			return spb.Value{}, fmt.Errorf("%s value: %v", t.String(), errText(raw, err))
		}
		if t == spb.TypeFloat {
			if math.Abs(f) > math.MaxFloat32 {
				return spb.Value{}, fmt.Errorf("Float value %g out of range", f)
			}
			return spb.Value{Kind: spb.KindFloat, Float: float64(float32(f))}, nil
		}
		return spb.Value{Kind: spb.KindDouble, Float: f}, nil
	case spb.TypeBoolean:
		switch x := raw.(type) {
		case bool:
			return spb.Value{Kind: spb.KindBool, Bool: x}, nil
		default:
			s := strings.ToLower(strings.TrimSpace(numText(raw)))
			switch s {
			case "true", "1", "on":
				return spb.Value{Kind: spb.KindBool, Bool: true}, nil
			case "false", "0", "off":
				return spb.Value{Kind: spb.KindBool}, nil
			}
			return spb.Value{}, fmt.Errorf("Boolean value: want true or false, got %s", textOf(raw))
		}
	case spb.TypeString, spb.TypeText, spb.TypeUUID:
		if s, ok := raw.(string); ok {
			return spb.Value{Kind: spb.KindString, Str: s}, nil
		}
		return spb.Value{Kind: spb.KindString, Str: strings.TrimSpace(string(payload))}, nil
	case spb.TypeDateTime:
		if s, ok := raw.(string); ok {
			if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return spb.Value{Kind: spb.KindLong, Uint: uint64(ts.UnixMilli())}, nil
			}
		}
		n, err := strconv.ParseInt(numText(raw), 10, 64)
		if err != nil {
			return spb.Value{}, fmt.Errorf("DateTime value: want an RFC 3339 timestamp or milliseconds since the epoch, got %s", textOf(raw))
		}
		return spb.Value{Kind: spb.KindLong, Uint: uint64(n)}, nil
	case spb.TypeBytes, spb.TypeFile:
		s, ok := raw.(string)
		if !ok {
			return spb.Value{}, fmt.Errorf("%s value: want a base64 string", t.String())
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return spb.Value{}, fmt.Errorf("%s value: invalid base64: %v", t.String(), err)
		}
		return spb.Value{Kind: spb.KindBytes, Bytes: b}, nil
	}
	return spb.Value{}, fmt.Errorf("writing %s metrics is not supported", t.String())
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
