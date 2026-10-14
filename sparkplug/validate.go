package sparkplug

import (
	"errors"
	"fmt"
	"strings"
)

// maxValidationErrors bounds the problems Validate reports.
const maxValidationErrors = 20

// Validate checks a payload against the value rules of the Sparkplug 3.0
// specification that don't depend on the message type:
//
//   - seq is at most 255;
//   - every metric has a name or an alias;
//   - data types are defined, and each value is carried in the field its
//     data type uses (integers and DateTime in int_value or long_value,
//     within the type's range; Float and Double in float_value or
//     double_value; Boolean in boolean_value; strings in string_value;
//     Bytes, File and arrays in bytes_value; DataSet and Template in their
//     messages);
//   - a null metric or property carries no value, and other metrics carry
//     one (when their data type is known);
//   - array values are well formed (see UnpackArray);
//   - data sets have one name and one type per column and one value per
//     column in every row, each matching its column type or null;
//   - template members and parameters follow the same rules, and
//     instances name their definition.
//
// Rules that depend on the message type (births must carry names, data
// types and a bdSeq, …) are left to the caller. Validate returns nil or
// an error describing up to 20 problems (errors.Join). Checking a valid
// payload doesn't allocate, so it is cheap enough for every message.
func (p *Payload) Validate() error {
	var v validator
	if p.HasSeq && p.Seq > 255 {
		v.add("seq %d is above 255", p.Seq)
	}
	for i := range p.Metrics {
		v.push("metric", i, p.Metrics[i].Name)
		v.metric(&p.Metrics[i], false)
		v.pop()
	}
	return errors.Join(v.errs...)
}

// validator collects problems. Locations are kept on a small stack and
// only formatted when a problem is reported, so checking a valid payload
// allocates nothing.
type validator struct {
	errs   []error
	frames [16]frame // nesting is bounded by Decode
	depth  int
}

type frame struct {
	what string
	i    int // -1: no index
	name string
}

func (v *validator) push(what string, i int, name string) {
	if v.depth < len(v.frames) {
		v.frames[v.depth] = frame{what, i, name}
	}
	v.depth++
}

func (v *validator) pop() { v.depth-- }

func (v *validator) add(format string, a ...any) {
	if len(v.errs) >= maxValidationErrors {
		return
	}
	var b strings.Builder
	for _, f := range v.frames[:min(v.depth, len(v.frames))] {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		switch {
		case f.i >= 0 && f.name != "": // metric 3 ("Temperature")
			fmt.Fprintf(&b, "%s %d (%q)", f.what, f.i, f.name)
		case f.i >= 0: // row 2
			fmt.Fprintf(&b, "%s %d", f.what, f.i)
		case f.what != "" && f.name != "": // column "x"
			fmt.Fprintf(&b, "%s %q", f.what, f.name)
		case f.what != "": // properties
			b.WriteString(f.what)
		default: // a property key
			fmt.Fprintf(&b, "%q", f.name)
		}
	}
	if b.Len() > 0 {
		b.WriteString(": ")
	}
	fmt.Fprintf(&b, format, a...)
	v.errs = append(v.errs, errors.New(b.String()))
}

func (v *validator) metric(m *Metric, inTemplate bool) {
	if m.Name == "" && !m.HasAlias && !inTemplate {
		v.add("no name and no alias")
	}
	if m.Datatype > TypeDateTimeArray || m.Datatype == TypePropertySet || m.Datatype == TypePropertySetList {
		v.add("%s is not a metric data type", m.Datatype)
		return
	}
	switch {
	case m.IsNull && m.Value.Kind != KindNone:
		v.add("null metric carries a %s value", m.Value.Kind)
	case m.IsNull:
	case m.Value.Kind == KindNone && m.Datatype != TypeUnknown:
		v.add("%s metric carries no value and is not null", m.Datatype)
	default:
		v.value(m.Datatype, m.Value)
	}
	if m.Properties != nil {
		v.push("properties", -1, "")
		v.properties(m.Properties)
		v.pop()
	}
}

// value checks that val is carried as data type t requires.
func (v *validator) value(t DataType, val Value) {
	want := func(k1, k2 Kind) bool {
		if val.Kind == k1 || val.Kind == k2 {
			return true
		}
		v.add("%s value carried as %s", t, val.Kind)
		return false
	}
	switch t {
	case TypeUnknown: // typed by its birth: nothing to check here
	case TypeInt8, TypeInt16, TypeInt32, TypeUInt8, TypeUInt16, TypeUInt32, TypeInt64, TypeUInt64, TypeDateTime:
		if want(KindInt, KindLong) && !intFits(t, val.Uint) {
			v.add("%d is out of range for %s", val.Uint, t)
		}
	case TypeFloat, TypeDouble:
		want(KindFloat, KindDouble)
	case TypeBoolean:
		want(KindBool, KindBool)
	case TypeString, TypeText, TypeUUID:
		want(KindString, KindString)
	case TypeBytes, TypeFile:
		want(KindBytes, KindBytes)
	case TypeDataSet:
		if want(KindDataSet, KindDataSet) {
			v.dataSet(val.DataSet)
		}
	case TypeTemplate:
		if want(KindTemplate, KindTemplate) {
			v.template(val.Template)
		}
	case TypePropertySet:
		want(KindPropertySet, KindPropertySet)
	case TypePropertySetList:
		want(KindPropertySetList, KindPropertySetList)
	default:
		if !t.IsArray() {
			v.add("%s is not a defined data type", t)
		} else if want(KindBytes, KindBytes) {
			if err := checkArray(t, val.Bytes); err != nil {
				v.add("%v", err)
			}
		}
	}
}

// intFits reports whether u, as carried on the wire, is a valid value of
// integer type t. Signed types may be sign-extended to 32 or 64 bits (two's
// complement) or not (as some encoders do).
func intFits(t DataType, u uint64) bool {
	const max32 = 1<<32 - 1
	switch t {
	case TypeInt8:
		return u <= 0xff || (u >= 0xffffff80 && u <= max32) || u >= 0xffffffffffffff80
	case TypeInt16:
		return u <= 0xffff || (u >= 0xffff8000 && u <= max32) || u >= 0xffffffffffff8000
	case TypeInt32:
		return u <= max32 || u >= 0xffffffff80000000
	case TypeUInt8:
		return u <= 0xff
	case TypeUInt16:
		return u <= 0xffff
	case TypeUInt32:
		return u <= max32
	}
	return true // Int64, UInt64, DateTime: any 64-bit value
}

func (v *validator) properties(ps *PropertySet) {
	if len(ps.Keys) != len(ps.Values) {
		v.add("%d keys for %d values", len(ps.Keys), len(ps.Values))
	}
	for i := range ps.Values {
		pv := &ps.Values[i]
		if i < len(ps.Keys) {
			v.push("", -1, ps.Keys[i])
		} else {
			v.push("value", i, "")
		}
		switch {
		case pv.IsNull && pv.Value.Kind != KindNone:
			v.add("null property carries a %s value", pv.Value.Kind)
		case pv.IsNull:
		case pv.Type == TypePropertySet && pv.Value.Kind == KindPropertySet && pv.Value.Properties != nil:
			v.properties(pv.Value.Properties)
		case pv.Type == TypePropertySetList && pv.Value.Kind == KindPropertySetList:
			for j := range pv.Value.PropertiesList {
				v.push("set", j, "")
				v.properties(&pv.Value.PropertiesList[j])
				v.pop()
			}
		case pv.Type.IsArray() || pv.Type == TypeDataSet || pv.Type == TypeTemplate || pv.Type == TypeBytes || pv.Type == TypeFile:
			v.add("%s is not a property data type", pv.Type)
		default:
			v.value(pv.Type, pv.Value)
		}
		v.pop()
	}
}

func (v *validator) dataSet(ds *DataSet) {
	if ds == nil {
		v.add("empty DataSet")
		return
	}
	n := ds.NumColumns
	if uint64(len(ds.Columns)) != n || uint64(len(ds.Types)) != n {
		v.add("DataSet has %d columns but %d names and %d types", n, len(ds.Columns), len(ds.Types))
		return
	}
	for r, row := range ds.Rows {
		if uint64(len(row)) != n {
			v.add("DataSet row %d has %d values for %d columns", r, len(row), n)
			continue
		}
		for c, cell := range row {
			t := ds.Types[c]
			if cell.Kind == KindNone {
				continue // null cell
			}
			switch {
			case t.IsArray() || t == TypeDataSet || t == TypeTemplate || t == TypeBytes || t == TypeFile ||
				t == TypePropertySet || t == TypePropertySetList:
				v.add("DataSet column %q has type %s, which a cell can't hold", ds.Columns[c], t)
			default:
				v.push("row", r, "")
				v.push("column", -1, ds.Columns[c])
				v.value(t, cell)
				v.pop()
				v.pop()
			}
		}
	}
}

func (v *validator) template(t *Template) {
	if t == nil {
		v.add("empty Template")
		return
	}
	if !t.IsDefinition && t.TemplateRef == "" {
		v.add("template instance without template_ref")
	}
	if t.IsDefinition && t.TemplateRef != "" {
		v.add("template definition with a template_ref")
	}
	for i := range t.Metrics {
		v.push("member", i, t.Metrics[i].Name)
		v.metric(&t.Metrics[i], true)
		v.pop()
	}
	for i, p := range t.Parameters {
		if p.Value.Kind != KindNone {
			v.push("parameter", i, p.Name)
			v.value(p.Type, p.Value)
			v.pop()
		}
	}
}
