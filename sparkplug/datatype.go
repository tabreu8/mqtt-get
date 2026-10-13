package sparkplug

import "strconv"

// DataType is a Sparkplug B data type (the datatype field of a metric, the
// type of a property, a data set column or a template parameter).
type DataType uint32

// Sparkplug B data types, numbered as in the specification.
const (
	TypeUnknown         DataType = 0
	TypeInt8            DataType = 1
	TypeInt16           DataType = 2
	TypeInt32           DataType = 3
	TypeInt64           DataType = 4
	TypeUInt8           DataType = 5
	TypeUInt16          DataType = 6
	TypeUInt32          DataType = 7
	TypeUInt64          DataType = 8
	TypeFloat           DataType = 9
	TypeDouble          DataType = 10
	TypeBoolean         DataType = 11
	TypeString          DataType = 12
	TypeDateTime        DataType = 13
	TypeText            DataType = 14
	TypeUUID            DataType = 15
	TypeDataSet         DataType = 16
	TypeBytes           DataType = 17
	TypeFile            DataType = 18
	TypeTemplate        DataType = 19
	TypePropertySet     DataType = 20
	TypePropertySetList DataType = 21
	TypeInt8Array       DataType = 22
	TypeInt16Array      DataType = 23
	TypeInt32Array      DataType = 24
	TypeInt64Array      DataType = 25
	TypeUInt8Array      DataType = 26
	TypeUInt16Array     DataType = 27
	TypeUInt32Array     DataType = 28
	TypeUInt64Array     DataType = 29
	TypeFloatArray      DataType = 30
	TypeDoubleArray     DataType = 31
	TypeBooleanArray    DataType = 32
	TypeStringArray     DataType = 33
	TypeDateTimeArray   DataType = 34
)

var typeNames = [...]string{
	"Unknown", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
	"Float", "Double", "Boolean", "String", "DateTime", "Text", "UUID", "DataSet", "Bytes", "File",
	"Template", "PropertySet", "PropertySetList", "Int8Array", "Int16Array", "Int32Array", "Int64Array",
	"UInt8Array", "UInt16Array", "UInt32Array", "UInt64Array", "FloatArray", "DoubleArray",
	"BooleanArray", "StringArray", "DateTimeArray",
}

// String returns the name the specification uses for t ("Int32",
// "DoubleArray", …), or "DataType(n)" for numbers it doesn't define.
func (t DataType) String() string {
	if int(t) < len(typeNames) {
		return typeNames[t]
	}
	return "DataType(" + strconv.FormatUint(uint64(t), 10) + ")"
}

// Kind tells which protobuf field of a value is set.
type Kind uint8

// Value kinds. Metrics use KindInt to KindTemplate; property values use
// KindInt to KindString, KindPropertySet and KindPropertySetList; data set
// cells and template parameters use KindInt to KindString.
const (
	KindNone            Kind = iota // no value field is set
	KindInt                         // uint32 int_value, in Value.Uint
	KindLong                        // uint64 long_value, in Value.Uint
	KindFloat                       // float float_value, in Value.Float
	KindDouble                      // double double_value, in Value.Float
	KindBool                        // bool boolean_value, in Value.Bool
	KindString                      // string string_value, in Value.Str
	KindBytes                       // bytes bytes_value, in Value.Bytes
	KindDataSet                     // dataset_value, in Value.DataSet
	KindTemplate                    // template_value, in Value.Template
	KindPropertySet                 // propertyset_value, in Value.Properties
	KindPropertySetList             // propertysets_value, in Value.PropertiesList
)

var kindNames = [...]string{"None", "Int", "Long", "Float", "Double", "Bool", "String", "Bytes",
	"DataSet", "Template", "PropertySet", "PropertySetList"}

// String returns the kind's name ("Int", "Double", …).
func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "Kind(" + strconv.Itoa(int(k)) + ")"
}
