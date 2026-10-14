package sparkplug

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// IsArray reports whether t is one of the array data types (Int8Array to
// DateTimeArray), whose values are packed into Value.Bytes.
func (t DataType) IsArray() bool { return t >= TypeInt8Array && t <= TypeDateTimeArray }

// elemSize is the packed size of one element of a fixed-size array type.
func elemSize(t DataType) int {
	switch t {
	case TypeInt8Array, TypeUInt8Array:
		return 1
	case TypeInt16Array, TypeUInt16Array:
		return 2
	case TypeInt32Array, TypeUInt32Array, TypeFloatArray:
		return 4
	case TypeInt64Array, TypeUInt64Array, TypeDoubleArray, TypeDateTimeArray:
		return 8
	}
	return 0
}

// checkArray reports whether b is a well-formed packed array of type t
// (the checks of UnpackArray, without building the slice).
func checkArray(t DataType, b []byte) error {
	if n := elemSize(t); n > 0 {
		if len(b)%n != 0 {
			return fmt.Errorf("%w: %s of %d bytes (not a multiple of %d)", ErrMalformed, t, len(b), n)
		}
		return nil
	}
	switch t {
	case TypeBooleanArray:
		if len(b) < 4 {
			return fmt.Errorf("%w: BooleanArray of %d bytes (no count)", ErrMalformed, len(b))
		}
		if n := uint64(binary.LittleEndian.Uint32(b)); n > uint64(len(b)-4)*8 {
			return fmt.Errorf("%w: BooleanArray counts %d values in %d bytes", ErrMalformed, n, len(b)-4)
		}
	case TypeStringArray:
		if len(b) > 0 && b[len(b)-1] != 0 {
			return fmt.Errorf("%w: StringArray without a final NUL", ErrMalformed)
		}
	}
	return nil
}

// UnpackArray decodes the packed bytes of an array value (Value.Bytes of a
// metric whose data type is an array type) into a Go slice:
//
//	Int8Array []int8       UInt8Array  []uint8      FloatArray    []float32
//	Int16Array []int16     UInt16Array []uint16     DoubleArray   []float64
//	Int32Array []int32     UInt32Array []uint32     BooleanArray  []bool
//	Int64Array []int64     UInt64Array []uint64     StringArray   []string
//	DateTimeArray []time.Time (UTC, millisecond precision)
//
// Bytes that don't fit the type (a length that isn't a multiple of the
// element size, a missing string terminator, fewer packed booleans than
// counted) return an error wrapping ErrMalformed.
func UnpackArray(t DataType, b []byte) (any, error) {
	le := binary.LittleEndian
	if n := elemSize(t); n > 0 && len(b)%n != 0 {
		return nil, fmt.Errorf("%w: %s of %d bytes (not a multiple of %d)", ErrMalformed, t, len(b), n)
	}
	switch t {
	case TypeInt8Array:
		out := make([]int8, len(b))
		for i := range out {
			out[i] = int8(b[i])
		}
		return out, nil
	case TypeUInt8Array:
		return append([]uint8{}, b...), nil
	case TypeInt16Array:
		out := make([]int16, len(b)/2)
		for i := range out {
			out[i] = int16(le.Uint16(b[2*i:]))
		}
		return out, nil
	case TypeUInt16Array:
		out := make([]uint16, len(b)/2)
		for i := range out {
			out[i] = le.Uint16(b[2*i:])
		}
		return out, nil
	case TypeInt32Array:
		out := make([]int32, len(b)/4)
		for i := range out {
			out[i] = int32(le.Uint32(b[4*i:]))
		}
		return out, nil
	case TypeUInt32Array:
		out := make([]uint32, len(b)/4)
		for i := range out {
			out[i] = le.Uint32(b[4*i:])
		}
		return out, nil
	case TypeFloatArray:
		out := make([]float32, len(b)/4)
		for i := range out {
			out[i] = math.Float32frombits(le.Uint32(b[4*i:]))
		}
		return out, nil
	case TypeInt64Array:
		out := make([]int64, len(b)/8)
		for i := range out {
			out[i] = int64(le.Uint64(b[8*i:]))
		}
		return out, nil
	case TypeUInt64Array:
		out := make([]uint64, len(b)/8)
		for i := range out {
			out[i] = le.Uint64(b[8*i:])
		}
		return out, nil
	case TypeDoubleArray:
		out := make([]float64, len(b)/8)
		for i := range out {
			out[i] = math.Float64frombits(le.Uint64(b[8*i:]))
		}
		return out, nil
	case TypeDateTimeArray:
		out := make([]time.Time, len(b)/8)
		for i := range out {
			out[i] = time.UnixMilli(int64(le.Uint64(b[8*i:]))).UTC()
		}
		return out, nil
	case TypeBooleanArray:
		// A uint32 count, then the booleans packed most significant bit first.
		if len(b) < 4 {
			return nil, fmt.Errorf("%w: BooleanArray of %d bytes (no count)", ErrMalformed, len(b))
		}
		n := uint64(le.Uint32(b))
		bits := b[4:]
		if n > uint64(len(bits))*8 { // trailing padding bytes are tolerated
			return nil, fmt.Errorf("%w: BooleanArray counts %d values in %d bytes", ErrMalformed, n, len(bits))
		}
		out := make([]bool, n)
		for i := range out {
			out[i] = bits[i/8]&(0x80>>(i%8)) != 0
		}
		return out, nil
	case TypeStringArray:
		// NUL-terminated strings.
		if len(b) == 0 {
			return []string{}, nil
		}
		if b[len(b)-1] != 0 {
			return nil, fmt.Errorf("%w: StringArray without a final NUL", ErrMalformed)
		}
		parts := bytes.Split(b[:len(b)-1], []byte{0})
		out := make([]string, len(parts))
		for i, p := range parts {
			out[i] = string(p)
		}
		return out, nil
	}
	return nil, fmt.Errorf("sparkplug: %s is not an array type", t)
}

// PackArray encodes values, a slice of the Go type UnpackArray returns for
// t, into the packed bytes of an array value (for Value.Bytes, with Kind
// KindBytes). Strings must not contain NUL bytes.
func PackArray(t DataType, values any) ([]byte, error) {
	le := binary.LittleEndian
	mismatch := func() ([]byte, error) {
		return nil, fmt.Errorf("sparkplug: %s needs %s, got %T", t, goType(t), values)
	}
	var out []byte
	switch t {
	case TypeInt8Array:
		v, ok := values.([]int8)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = append(out, byte(x))
		}
	case TypeUInt8Array:
		v, ok := values.([]uint8)
		if !ok {
			return mismatch()
		}
		out = append(out, v...)
	case TypeInt16Array:
		v, ok := values.([]int16)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint16(out, uint16(x))
		}
	case TypeUInt16Array:
		v, ok := values.([]uint16)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint16(out, x)
		}
	case TypeInt32Array:
		v, ok := values.([]int32)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint32(out, uint32(x))
		}
	case TypeUInt32Array:
		v, ok := values.([]uint32)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint32(out, x)
		}
	case TypeFloatArray:
		v, ok := values.([]float32)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint32(out, math.Float32bits(x))
		}
	case TypeInt64Array:
		v, ok := values.([]int64)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint64(out, uint64(x))
		}
	case TypeUInt64Array:
		v, ok := values.([]uint64)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint64(out, x)
		}
	case TypeDoubleArray:
		v, ok := values.([]float64)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint64(out, math.Float64bits(x))
		}
	case TypeDateTimeArray:
		v, ok := values.([]time.Time)
		if !ok {
			return mismatch()
		}
		for _, x := range v {
			out = le.AppendUint64(out, uint64(x.UnixMilli()))
		}
	case TypeBooleanArray:
		v, ok := values.([]bool)
		if !ok {
			return mismatch()
		}
		out = le.AppendUint32(out, uint32(len(v)))
		packed := make([]byte, (len(v)+7)/8)
		for i, x := range v {
			if x {
				packed[i/8] |= 0x80 >> (i % 8)
			}
		}
		out = append(out, packed...)
	case TypeStringArray:
		v, ok := values.([]string)
		if !ok {
			return mismatch()
		}
		for _, s := range v {
			if bytes.IndexByte([]byte(s), 0) >= 0 {
				return nil, fmt.Errorf("sparkplug: StringArray element %q contains a NUL byte", s)
			}
			out = append(append(out, s...), 0)
		}
	default:
		return nil, fmt.Errorf("sparkplug: %s is not an array type", t)
	}
	if out == nil {
		out = []byte{}
	}
	return out, nil
}

func goType(t DataType) string {
	return map[DataType]string{
		TypeInt8Array: "[]int8", TypeUInt8Array: "[]uint8", TypeInt16Array: "[]int16", TypeUInt16Array: "[]uint16",
		TypeInt32Array: "[]int32", TypeUInt32Array: "[]uint32", TypeFloatArray: "[]float32", TypeInt64Array: "[]int64",
		TypeUInt64Array: "[]uint64", TypeDoubleArray: "[]float64", TypeDateTimeArray: "[]time.Time",
		TypeBooleanArray: "[]bool", TypeStringArray: "[]string",
	}[t]
}
