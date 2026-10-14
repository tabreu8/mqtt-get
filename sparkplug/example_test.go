package sparkplug_test

import (
	"encoding/hex"
	"fmt"
	"log"

	"github.com/tabreu8/mqtt-get/sparkplug"
)

// Decode the payload of a DDATA message received on
// spBv1.0/<group>/DDATA/<edge node>/<device>. Data messages often carry
// only aliases; their names and data types come from the device's DBIRTH.
func ExampleDecode() {
	// The bytes of the MQTT message (shown in hex).
	msg, _ := hex.DecodeString("088080b3c19c3312121001188080b3c19c336900000000008035401208100250fdffffff0f12180a0b4c696e652f537461747573200c7a0772756e6e696e671807")

	p, err := sparkplug.Decode(msg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("seq", p.Seq, "metrics", len(p.Metrics))
	for _, m := range p.Metrics {
		switch {
		case m.Name != "":
			fmt.Printf("%s (%s) = %q\n", m.Name, m.Datatype, m.Value.Str)
		case m.Value.Kind == sparkplug.KindDouble:
			fmt.Printf("alias %d = %g\n", m.Alias, m.Value.Float)
		case m.Value.Kind == sparkplug.KindInt:
			// Say the birth declared alias 2 as an Int16: signed values
			// travel in two's complement.
			fmt.Printf("alias %d = %d\n", m.Alias, int16(m.Value.Uint))
		}
	}
	// Output:
	// seq 7 metrics 3
	// alias 1 = 21.5
	// alias 2 = -3
	// Line/Status (String) = "running"
}

// Encode an NCMD asking an edge node to publish its birth certificates
// again. Publish it (QoS 0, not retained) on spBv1.0/<group>/NCMD/<edge node>.
func ExampleEncode() {
	const now = 1760000000000 // milliseconds since the Unix epoch
	payload := sparkplug.Encode(&sparkplug.Payload{
		Timestamp:    now,
		HasTimestamp: true,
		Metrics: []sparkplug.Metric{{
			Name:         "Node Control/Rebirth",
			Timestamp:    now,
			HasTimestamp: true,
			Datatype:     sparkplug.TypeBoolean,
			Value:        sparkplug.Value{Kind: sparkplug.KindBool, Bool: true},
		}},
	})
	fmt.Printf("%x\n", payload)
	// Output:
	// 088080b3c19c3312210a144e6f646520436f6e74726f6c2f52656269727468188080b3c19c33200b7001
}

// Read an Int32Array metric, then build a BooleanArray one.
func ExampleUnpackArray() {
	m := sparkplug.Metric{Name: "Setpoints", Datatype: sparkplug.TypeInt32Array,
		Value: sparkplug.Value{Kind: sparkplug.KindBytes, Bytes: []byte{0xff, 0xff, 0xff, 0xff, 2, 0, 0, 0}}}
	vals, err := sparkplug.UnpackArray(m.Datatype, m.Value.Bytes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(vals.([]int32))

	packed, _ := sparkplug.PackArray(sparkplug.TypeBooleanArray, []bool{true, false, true})
	fmt.Printf("%x\n", packed)
	// Output:
	// [-1 2]
	// 03000000a0
}

// Check a payload before publishing it (or after receiving it).
func ExamplePayload_Validate() {
	p := &sparkplug.Payload{Seq: 1, HasSeq: true, Metrics: []sparkplug.Metric{
		{Name: "Speed", Datatype: sparkplug.TypeUInt8, Value: sparkplug.Value{Kind: sparkplug.KindInt, Uint: 300}},
		{Name: "Mode", Datatype: sparkplug.TypeString, Value: sparkplug.Value{Kind: sparkplug.KindBool}},
	}}
	fmt.Println(p.Validate())
	// Output:
	// metric 0 ("Speed"): 300 is out of range for UInt8
	// metric 1 ("Mode"): String value carried as Bool
}
