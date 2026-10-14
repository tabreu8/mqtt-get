// Package sparkplug encodes and decodes Sparkplug B payloads.
//
// Sparkplug B is an Eclipse specification for industrial data over MQTT
// (https://sparkplug.eclipse.org). It defines a topic namespace
// (spBv1.0/<group>/<message type>/<edge node>[/<device>]), session rules
// (birth and death certificates, sequence numbers, rebirth requests) and a
// binary payload: a Protocol Buffers message carrying a list of metrics,
// each with a name or a numeric alias, a data type and a value.
//
// This package handles only that payload. [Decode] turns the bytes of an
// MQTT message into a [Payload]; [Encode] does the reverse. It covers every
// message of the sparkplug_b.proto schema used by Sparkplug 3.0: metrics,
// metric metadata, property sets, data sets and templates. Decoding is
// lossless: extension values are kept as [KindExtension] values and fields
// unknown to the schema in each message's Unknown field, and [Encode]
// writes both back.
// [Payload.Validate] checks a payload against the specification's value
// rules, and [UnpackArray] and [PackArray] convert array values to and from
// Go slices. The package is written by hand from the schema, has no
// dependencies outside the Go standard library, and needs no code
// generation.
//
// It is not an edge node, not a host application and not an MQTT client:
// it doesn't connect to brokers, build topics, track births and deaths,
// resolve aliases or check sequence numbers. Use it inside your own MQTT
// client code for that.
//
// # Values
//
// A metric's value is a [Value]: its [Kind] tells which protobuf field
// carried it, and the metric's [DataType] tells how to read it. The rules
// come from the Sparkplug 3.0 specification:
//
//   - Int8, Int16 and Int32 travel as uint32 in two's complement, and
//     Int64 as uint64: convert with int8(v.Uint), int16(v.Uint),
//     int32(v.Uint) or int64(v.Uint).
//   - UInt8, UInt16 and UInt32 travel as uint32, UInt64 as uint64, in
//     [Value.Uint].
//   - DateTime is milliseconds since the Unix epoch, in [Value.Uint].
//   - Float and Double are in [Value.Float]; Boolean in [Value.Bool];
//     String, Text and UUID in [Value.Str]; Bytes and File in [Value.Bytes].
//   - Array types (Int8Array to DateTimeArray) are packed into
//     [Value.Bytes]: little-endian values; BooleanArray is a little-endian
//     uint32 count followed by the booleans packed most significant bit
//     first; StringArray is NUL-terminated strings. [UnpackArray] and
//     [PackArray] convert them.
//
// A null value is a metric with IsNull set, not a value.
//
// Implementations differ on which field carries some types (for example
// UInt32 in int_value or long_value, Float in float_value or
// double_value). [Decode] accepts either, and so does [Payload.Validate];
// when writing to a device, use the field it used in its birth.
package sparkplug
