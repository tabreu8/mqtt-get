// Package sparkplug decodes and encodes Eclipse Sparkplug B payloads and
// translates Sparkplug traffic into plain MQTT topics with JSON values.
//
// The protobuf wire format of sparkplug_b.proto is implemented by hand
// (it is small and stable), so the package has no generated code and no
// protobuf runtime dependency.
package sparkplug

import (
	"encoding/binary"
	"errors"
	"math"
)

// Payload is a Sparkplug B payload.
type Payload struct {
	Timestamp    uint64 // ms since the Unix epoch
	HasTimestamp bool
	Metrics      []Metric
	Seq          uint64
	HasSeq       bool
	UUID         string
	Body         []byte
}

// Metric is one metric of a payload (or of a template).
type Metric struct {
	Name         string
	Alias        uint64
	HasAlias     bool
	Timestamp    uint64 // ms since the Unix epoch
	HasTimestamp bool
	Datatype     uint32
	IsHistorical bool
	IsTransient  bool
	IsNull       bool
	Properties   *PropertySet
	Value        Value
}

// Kind tells which field of a value oneof is set.
type Kind uint8

// Value kinds (the oneof fields of Metric, PropertyValue, DataSetValue and
// Template.Parameter).
const (
	KindNone     Kind = iota
	KindInt           // uint32 int_value
	KindLong          // uint64 long_value
	KindFloat         // float float_value
	KindDouble        // double double_value
	KindBool          // bool boolean_value
	KindString        // string string_value
	KindBytes         // bytes bytes_value (metrics only)
	KindDataSet       // dataset_value (metrics only)
	KindTemplate      // template_value (metrics only)
	KindPropertySet
	KindPropertySetList
)

// Value is a decoded oneof value.
type Value struct {
	Kind     Kind
	U        uint64  // KindInt, KindLong, KindBool (0/1)
	F        float64 // KindFloat, KindDouble
	S        string  // KindString
	B        []byte  // KindBytes
	DataSet  *DataSet
	Template *Template
	Set      *PropertySet
	Sets     []PropertySet
}

// PropertySet is a set of typed key/value properties (e.g. engUnit).
type PropertySet struct {
	Keys   []string
	Values []PropertyValue
}

// PropertyValue is one property.
type PropertyValue struct {
	Type   uint32
	IsNull bool
	Value  Value
}

// DataSet is a table.
type DataSet struct {
	NumColumns uint64
	Columns    []string
	Types      []uint32
	Rows       [][]Value
}

// Template is a template definition or instance.
type Template struct {
	Version      string
	Metrics      []Metric
	Parameters   []Parameter
	TemplateRef  string
	IsDefinition bool
}

// Parameter is a template parameter.
type Parameter struct {
	Name  string
	Type  uint32
	Value Value
}

// ErrMalformed is returned for payloads that are not valid protobuf.
var ErrMalformed = errors.New("malformed Sparkplug B payload")

// --- wire decoding ---

const (
	wireVarint = 0
	wire64     = 1
	wireBytes  = 2
	wire32     = 5
)

// maxDepth bounds nesting (templates contain metrics, property sets contain
// property sets) so hostile payloads can't exhaust the stack.
const maxDepth = 16

type reader struct {
	b   []byte
	err error
}

func (r *reader) fail() { r.err = ErrMalformed; r.b = nil }

func (r *reader) varint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.fail()
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) fixed32() uint32 {
	if len(r.b) < 4 {
		r.fail()
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}

func (r *reader) fixed64() uint64 {
	if len(r.b) < 8 {
		r.fail()
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *reader) bytes() []byte {
	n := r.varint()
	if r.err != nil || n > uint64(len(r.b)) {
		r.fail()
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

// next reads a field key. ok is false at the end (or on error).
func (r *reader) next() (field uint64, wt int, ok bool) {
	if r.err != nil || len(r.b) == 0 {
		return 0, 0, false
	}
	k := r.varint()
	if r.err != nil {
		return 0, 0, false
	}
	field, wt = k>>3, int(k&7)
	if field == 0 {
		r.fail()
		return 0, 0, false
	}
	return field, wt, true
}

// skip skips a field of wire type wt.
func (r *reader) skip(wt int) {
	switch wt {
	case wireVarint:
		r.varint()
	case wire64:
		r.fixed64()
	case wireBytes:
		r.bytes()
	case wire32:
		r.fixed32()
	default: // groups (3, 4) are not used by Sparkplug
		r.fail()
	}
}

// expect checks a field's wire type; a mismatch is skipped as unknown data.
func (r *reader) expect(wt, want int) bool {
	if wt != want {
		r.skip(wt)
		return false
	}
	return true
}

// Decode parses a Sparkplug B payload.
func Decode(b []byte) (*Payload, error) {
	p := &Payload{}
	if n := countMetrics(b); n > 0 {
		p.Metrics = make([]Metric, 0, n)
	}
	r := &reader{b: b}
	for {
		f, wt, ok := r.next()
		if !ok {
			break
		}
		switch f {
		case 1:
			if r.expect(wt, wireVarint) {
				p.Timestamp, p.HasTimestamp = r.varint(), true
			}
		case 2:
			if r.expect(wt, wireBytes) {
				var m Metric
				decodeMetric(r.bytes(), &m, r, 0)
				p.Metrics = append(p.Metrics, m)
			}
		case 3:
			if r.expect(wt, wireVarint) {
				p.Seq, p.HasSeq = r.varint(), true
			}
		case 4:
			if r.expect(wt, wireBytes) {
				p.UUID = string(r.bytes())
			}
		case 5:
			if r.expect(wt, wireBytes) {
				p.Body = append([]byte(nil), r.bytes()...)
			}
		default:
			r.skip(wt)
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	return p, nil
}

// countMetrics counts the top-level metrics fields, to allocate the
// metrics slice once (a Metric is large). Malformed input returns what was
// counted so far; Decode reports the error.
func countMetrics(b []byte) int {
	r := &reader{b: b}
	n := 0
	for {
		f, wt, ok := r.next()
		if !ok {
			return n
		}
		if f == 2 && wt == wireBytes {
			n++
		}
		r.skip(wt)
	}
}

// sub decodes a nested message; errors propagate to the parent reader.
func sub(b []byte, parent *reader, depth int, fn func(r *reader, depth int)) {
	if parent.err != nil {
		return
	}
	if depth > maxDepth {
		parent.fail()
		return
	}
	r := &reader{b: b}
	fn(r, depth+1)
	if r.err != nil {
		parent.fail()
	}
}

func decodeMetric(b []byte, m *Metric, parent *reader, depth int) {
	sub(b, parent, depth, func(r *reader, depth int) {
		for {
			f, wt, ok := r.next()
			if !ok {
				return
			}
			switch f {
			case 1:
				if r.expect(wt, wireBytes) {
					m.Name = string(r.bytes())
				}
			case 2:
				if r.expect(wt, wireVarint) {
					m.Alias, m.HasAlias = r.varint(), true
				}
			case 3:
				if r.expect(wt, wireVarint) {
					m.Timestamp, m.HasTimestamp = r.varint(), true
				}
			case 4:
				if r.expect(wt, wireVarint) {
					m.Datatype = uint32(r.varint())
				}
			case 5:
				if r.expect(wt, wireVarint) {
					m.IsHistorical = r.varint() != 0
				}
			case 6:
				if r.expect(wt, wireVarint) {
					m.IsTransient = r.varint() != 0
				}
			case 7:
				if r.expect(wt, wireVarint) {
					m.IsNull = r.varint() != 0
				}
			case 9:
				if r.expect(wt, wireBytes) {
					m.Properties = &PropertySet{}
					decodePropertySet(r.bytes(), m.Properties, r, depth)
				}
			case 10, 11, 12, 13, 14, 15:
				decodeScalar(r, f-10, wt, &m.Value)
			case 16:
				if r.expect(wt, wireBytes) {
					m.Value = Value{Kind: KindBytes, B: append([]byte(nil), r.bytes()...)}
				}
			case 17:
				if r.expect(wt, wireBytes) {
					ds := &DataSet{}
					decodeDataSet(r.bytes(), ds, r, depth)
					m.Value = Value{Kind: KindDataSet, DataSet: ds}
				}
			case 18:
				if r.expect(wt, wireBytes) {
					t := &Template{}
					decodeTemplate(r.bytes(), t, r, depth)
					m.Value = Value{Kind: KindTemplate, Template: t}
				}
			default: // 8 metadata, 19 extension, unknown fields
				r.skip(wt)
			}
		}
	})
}

// decodeScalar decodes the six scalar oneof fields shared by every value
// type, in their common order: int, long, float, double, bool, string.
// idx is the field number minus the number of the int field.
func decodeScalar(r *reader, idx uint64, wt int, v *Value) {
	switch idx {
	case 0:
		if r.expect(wt, wireVarint) {
			*v = Value{Kind: KindInt, U: uint64(uint32(r.varint()))}
		}
	case 1:
		if r.expect(wt, wireVarint) {
			*v = Value{Kind: KindLong, U: r.varint()}
		}
	case 2:
		if r.expect(wt, wire32) {
			*v = Value{Kind: KindFloat, F: float64(math.Float32frombits(r.fixed32()))}
		}
	case 3:
		if r.expect(wt, wire64) {
			*v = Value{Kind: KindDouble, F: math.Float64frombits(r.fixed64())}
		}
	case 4:
		if r.expect(wt, wireVarint) {
			*v = Value{Kind: KindBool, U: b2u(r.varint() != 0)}
		}
	case 5:
		if r.expect(wt, wireBytes) {
			*v = Value{Kind: KindString, S: string(r.bytes())}
		}
	}
}

func b2u(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

func decodePropertySet(b []byte, ps *PropertySet, parent *reader, depth int) {
	sub(b, parent, depth, func(r *reader, depth int) {
		for {
			f, wt, ok := r.next()
			if !ok {
				return
			}
			switch {
			case f == 1 && wt == wireBytes:
				ps.Keys = append(ps.Keys, string(r.bytes()))
			case f == 2 && wt == wireBytes:
				var pv PropertyValue
				decodePropertyValue(r.bytes(), &pv, r, depth)
				ps.Values = append(ps.Values, pv)
			default:
				r.skip(wt)
			}
		}
	})
}

func decodePropertyValue(b []byte, pv *PropertyValue, parent *reader, depth int) {
	sub(b, parent, depth, func(r *reader, depth int) {
		for {
			f, wt, ok := r.next()
			if !ok {
				return
			}
			switch f {
			case 1:
				if r.expect(wt, wireVarint) {
					pv.Type = uint32(r.varint())
				}
			case 2:
				if r.expect(wt, wireVarint) {
					pv.IsNull = r.varint() != 0
				}
			case 3, 4, 5, 6, 7, 8:
				decodeScalar(r, f-3, wt, &pv.Value)
			case 9:
				if r.expect(wt, wireBytes) {
					ps := &PropertySet{}
					decodePropertySet(r.bytes(), ps, r, depth)
					pv.Value = Value{Kind: KindPropertySet, Set: ps}
				}
			case 10:
				if r.expect(wt, wireBytes) {
					var list []PropertySet
					sub(r.bytes(), r, depth, func(lr *reader, depth int) {
						for {
							f, wt, ok := lr.next()
							if !ok {
								return
							}
							if f == 1 && wt == wireBytes {
								var ps PropertySet
								decodePropertySet(lr.bytes(), &ps, lr, depth)
								list = append(list, ps)
							} else {
								lr.skip(wt)
							}
						}
					})
					pv.Value = Value{Kind: KindPropertySetList, Sets: list}
				}
			default:
				r.skip(wt)
			}
		}
	})
}

func decodeDataSet(b []byte, ds *DataSet, parent *reader, depth int) {
	sub(b, parent, depth, func(r *reader, depth int) {
		for {
			f, wt, ok := r.next()
			if !ok {
				return
			}
			switch {
			case f == 1 && wt == wireVarint:
				ds.NumColumns = r.varint()
			case f == 2 && wt == wireBytes:
				ds.Columns = append(ds.Columns, string(r.bytes()))
			case f == 3 && wt == wireVarint:
				ds.Types = append(ds.Types, uint32(r.varint()))
			case f == 3 && wt == wireBytes: // packed
				pr := &reader{b: r.bytes()}
				for len(pr.b) > 0 && pr.err == nil {
					ds.Types = append(ds.Types, uint32(pr.varint()))
				}
				if pr.err != nil {
					r.fail()
				}
			case f == 4 && wt == wireBytes:
				var row []Value
				sub(r.bytes(), r, depth, func(rr *reader, depth int) {
					for {
						f, wt, ok := rr.next()
						if !ok {
							return
						}
						if f != 1 || wt != wireBytes {
							rr.skip(wt)
							continue
						}
						var v Value
						sub(rr.bytes(), rr, depth, func(er *reader, _ int) {
							for {
								f, wt, ok := er.next()
								if !ok {
									return
								}
								if f >= 1 && f <= 6 {
									decodeScalar(er, f-1, wt, &v)
								} else {
									er.skip(wt)
								}
							}
						})
						row = append(row, v)
					}
				})
				ds.Rows = append(ds.Rows, row)
			default:
				r.skip(wt)
			}
		}
	})
}

func decodeTemplate(b []byte, t *Template, parent *reader, depth int) {
	sub(b, parent, depth, func(r *reader, depth int) {
		for {
			f, wt, ok := r.next()
			if !ok {
				return
			}
			switch {
			case f == 1 && wt == wireBytes:
				t.Version = string(r.bytes())
			case f == 2 && wt == wireBytes:
				var m Metric
				decodeMetric(r.bytes(), &m, r, depth)
				t.Metrics = append(t.Metrics, m)
			case f == 3 && wt == wireBytes:
				var p Parameter
				sub(r.bytes(), r, depth, func(pr *reader, _ int) {
					for {
						f, wt, ok := pr.next()
						if !ok {
							return
						}
						switch f {
						case 1:
							if pr.expect(wt, wireBytes) {
								p.Name = string(pr.bytes())
							}
						case 2:
							if pr.expect(wt, wireVarint) {
								p.Type = uint32(pr.varint())
							}
						case 3, 4, 5, 6, 7, 8:
							decodeScalar(pr, f-3, wt, &p.Value)
						default:
							pr.skip(wt)
						}
					}
				})
				t.Parameters = append(t.Parameters, p)
			case f == 4 && wt == wireBytes:
				t.TemplateRef = string(r.bytes())
			case f == 5 && wt == wireVarint:
				t.IsDefinition = r.varint() != 0
			default:
				r.skip(wt)
			}
		}
	})
}

// --- wire encoding ---

type writer struct{ b []byte }

func (w *writer) key(field uint64, wt int) { w.b = binary.AppendUvarint(w.b, field<<3|uint64(wt)) }

func (w *writer) varint(field, v uint64) {
	w.key(field, wireVarint)
	w.b = binary.AppendUvarint(w.b, v)
}

func (w *writer) bool(field uint64, v bool) { w.varint(field, b2u(v)) }

func (w *writer) bytes(field uint64, v []byte) {
	w.key(field, wireBytes)
	w.b = binary.AppendUvarint(w.b, uint64(len(v)))
	w.b = append(w.b, v...)
}

func (w *writer) str(field uint64, v string) {
	w.key(field, wireBytes)
	w.b = binary.AppendUvarint(w.b, uint64(len(v)))
	w.b = append(w.b, v...)
}

// msg writes a nested message produced by fn.
func (w *writer) msg(field uint64, fn func(w *writer)) {
	var inner writer
	fn(&inner)
	w.bytes(field, inner.b)
}

// scalar writes a scalar value at base (the field number of int_value).
func (w *writer) scalar(base uint64, v Value) {
	switch v.Kind {
	case KindInt:
		w.varint(base, uint64(uint32(v.U)))
	case KindLong:
		w.varint(base+1, v.U)
	case KindFloat:
		w.key(base+2, wire32)
		w.b = binary.LittleEndian.AppendUint32(w.b, math.Float32bits(float32(v.F)))
	case KindDouble:
		w.key(base+3, wire64)
		w.b = binary.LittleEndian.AppendUint64(w.b, math.Float64bits(v.F))
	case KindBool:
		w.bool(base+4, v.U != 0)
	case KindString:
		w.str(base+5, v.S)
	}
}

// Encode serializes a payload.
func Encode(p *Payload) []byte {
	var w writer
	if p.HasTimestamp {
		w.varint(1, p.Timestamp)
	}
	for i := range p.Metrics {
		m := &p.Metrics[i]
		w.msg(2, func(w *writer) { encodeMetric(w, m) })
	}
	if p.HasSeq {
		w.varint(3, p.Seq)
	}
	if p.UUID != "" {
		w.str(4, p.UUID)
	}
	if p.Body != nil {
		w.bytes(5, p.Body)
	}
	return w.b
}

func encodeMetric(w *writer, m *Metric) {
	if m.Name != "" {
		w.str(1, m.Name)
	}
	if m.HasAlias {
		w.varint(2, m.Alias)
	}
	if m.HasTimestamp {
		w.varint(3, m.Timestamp)
	}
	if m.Datatype != 0 {
		w.varint(4, uint64(m.Datatype))
	}
	if m.IsHistorical {
		w.bool(5, true)
	}
	if m.IsTransient {
		w.bool(6, true)
	}
	if m.IsNull {
		w.bool(7, true)
	}
	if m.Properties != nil {
		w.msg(9, func(w *writer) { encodePropertySet(w, m.Properties) })
	}
	switch m.Value.Kind {
	case KindBytes:
		w.bytes(16, m.Value.B)
	case KindDataSet:
		w.msg(17, func(w *writer) { encodeDataSet(w, m.Value.DataSet) })
	case KindTemplate:
		w.msg(18, func(w *writer) { encodeTemplate(w, m.Value.Template) })
	default:
		w.scalar(10, m.Value)
	}
}

func encodePropertySet(w *writer, ps *PropertySet) {
	for _, k := range ps.Keys {
		w.str(1, k)
	}
	for i := range ps.Values {
		pv := &ps.Values[i]
		w.msg(2, func(w *writer) {
			if pv.Type != 0 {
				w.varint(1, uint64(pv.Type))
			}
			if pv.IsNull {
				w.bool(2, true)
			}
			switch pv.Value.Kind {
			case KindPropertySet:
				w.msg(9, func(w *writer) { encodePropertySet(w, pv.Value.Set) })
			case KindPropertySetList:
				w.msg(10, func(w *writer) {
					for j := range pv.Value.Sets {
						w.msg(1, func(w *writer) { encodePropertySet(w, &pv.Value.Sets[j]) })
					}
				})
			default:
				w.scalar(3, pv.Value)
			}
		})
	}
}

func encodeDataSet(w *writer, ds *DataSet) {
	w.varint(1, ds.NumColumns)
	for _, c := range ds.Columns {
		w.str(2, c)
	}
	for _, t := range ds.Types {
		w.varint(3, uint64(t))
	}
	for _, row := range ds.Rows {
		w.msg(4, func(w *writer) {
			for _, v := range row {
				w.msg(1, func(w *writer) { w.scalar(1, v) })
			}
		})
	}
}

func encodeTemplate(w *writer, t *Template) {
	if t.Version != "" {
		w.str(1, t.Version)
	}
	for i := range t.Metrics {
		m := &t.Metrics[i]
		w.msg(2, func(w *writer) { encodeMetric(w, m) })
	}
	for i := range t.Parameters {
		p := &t.Parameters[i]
		w.msg(3, func(w *writer) {
			if p.Name != "" {
				w.str(1, p.Name)
			}
			if p.Type != 0 {
				w.varint(2, uint64(p.Type))
			}
			w.scalar(3, p.Value)
		})
	}
	if t.TemplateRef != "" {
		w.str(4, t.TemplateRef)
	}
	if t.IsDefinition {
		w.bool(5, true)
	}
}
