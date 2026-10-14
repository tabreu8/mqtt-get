// Command floodbroker is a minimal fake MQTT broker for benchmarking MQTT
// subscribers such as mqtt-get. After a client subscribes it streams
// pre-encoded QoS 0 PUBLISH packets matching each subscribed filter as fast
// as the socket accepts them, so the measurement is limited by the
// subscriber, not by a real broker. Supports MQTT 3.1.1 and 5.
//
//	go run ./cmd/floodbroker -addr :1884 -topics 1000 -size 64
//
// With -sparkplug N it plays N Sparkplug B edge nodes instead, each with
// one device: their births first, then DDATA messages carrying -metrics
// metrics each (by alias, with consecutive sequence numbers).
//
//	go run ./cmd/floodbroker -addr :1884 -sparkplug 1000 -metrics 10
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tabreu8/mqtt-get/sparkplug"
)

var (
	addr    = flag.String("addr", "127.0.0.1:1884", "listen address")
	nTopics = flag.Int("topics", 1000, "distinct topics per subscribed filter")
	size    = flag.Int("size", 64, "payload bytes")
	qos     = flag.Int("qos", 0, "QoS of the flood (0 or 1; acknowledgements are read and ignored)")
	props   = flag.Bool("props", false, "MQTT 5: give every message a content type, a user property and an expiry")
	spNodes = flag.Int("sparkplug", 0, "play this many Sparkplug B edge nodes instead (one device each)")
	spK     = flag.Int("metrics", 10, "Sparkplug: metrics per DDATA message")
	sent    atomic.Uint64
)

func main() {
	flag.Parse()
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("floodbroker listening on %s (topics/filter=%d, payload=%dB)", l.Addr(), *nTopics, *size)
	go func() {
		var last uint64
		for range time.Tick(5 * time.Second) {
			n := sent.Load()
			log.Printf("sent %d msgs (%.0f msg/s)", n, float64(n-last)/5)
			last = n
		}
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go serve(c)
	}
}

func readPacket(r *bufio.Reader) (byte, []byte, error) {
	h, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := binary.ReadUvarint(r) // MQTT varint == LEB128 for lengths < 2^28
	if err != nil {
		return 0, nil, err
	}
	body := make([]byte, n)
	_, err = io.ReadFull(r, body)
	return h, body, err
}

func appendVarint(b []byte, n int) []byte { return binary.AppendUvarint(b, uint64(n)) }

func appendStr(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	h, body, err := readPacket(r)
	if err != nil || h>>4 != 1 {
		return
	}
	// CONNECT: protocol name (2+len), then level.
	nameLen := int(binary.BigEndian.Uint16(body))
	v5 := body[2+nameLen] == 5
	connack := []byte{0x20, 2, 0, 0}
	if v5 {
		connack = []byte{0x20, 3, 0, 0, 0}
	}
	if _, err := c.Write(connack); err != nil {
		return
	}
	var filters []string
	for len(filters) == 0 {
		h, body, err = readPacket(r)
		if err != nil {
			return
		}
		switch h >> 4 {
		case 12: // PINGREQ
			_, _ = c.Write([]byte{0xD0, 0})
		case 8: // SUBSCRIBE
			id := body[:2]
			p := body[2:]
			if v5 {
				pl, n := binary.Uvarint(p)
				p = p[n+int(pl):]
			}
			var codes []byte
			for len(p) > 0 {
				fl := int(binary.BigEndian.Uint16(p))
				filters = append(filters, string(p[2:2+fl]))
				p = p[2+fl+1:]
				codes = append(codes, 0)
			}
			suback := append([]byte{}, id...)
			if v5 {
				suback = append(suback, 0)
			}
			suback = append(suback, codes...)
			if _, err := c.Write(append(appendVarint([]byte{0x90}, len(suback)), suback...)); err != nil {
				return
			}
		}
	}
	// Answer pings and ignore everything else from now on.
	go func() {
		for {
			h, _, err := readPacket(r)
			if err != nil {
				return
			}
			if h>>4 == 12 {
				_, _ = c.Write([]byte{0xD0, 0})
			}
		}
	}()
	block := buildBlock(filters, v5)
	perBlock := uint64(len(filters) * *nTopics)
	if *spNodes > 0 {
		births, data, n := sparkplugBlocks(v5)
		if _, err := c.Write(births); err != nil {
			return
		}
		block, perBlock = data, uint64(n)
	}
	for {
		if _, err := c.Write(block); err != nil {
			return
		}
		sent.Add(perBlock)
	}
}

// buildBlock pre-encodes one PUBLISH per topic for every filter.
func buildBlock(filters []string, v5 bool) []byte {
	payload := []byte(strings.Repeat("x", *size))
	var block []byte
	for _, f := range filters {
		if rest, ok := strings.CutPrefix(f, "$share/"); ok {
			_, f, _ = strings.Cut(rest, "/") // drop the group name
		}
		base := strings.TrimSuffix(strings.ReplaceAll(f, "+", "p"), "#")
		base = strings.TrimSuffix(base, "/")
		for i := 0; i < *nTopics; i++ {
			topic := base + "/t/" + strconv.Itoa(i)
			var vh []byte
			vh = appendStr(vh, topic)
			if *qos > 0 {
				vh = binary.BigEndian.AppendUint16(vh, uint16(i%65535+1))
			}
			if v5 && *props {
				var pb []byte
				pb = appendStr(append(pb, 0x03), "application/json")          // content type
				pb = appendStr(appendStr(append(pb, 0x26), "site"), "lisbon") // user property
				pb = binary.BigEndian.AppendUint32(append(pb, 0x02), 3600)    // message expiry
				vh = append(appendVarint(vh, len(pb)), pb...)
			} else if v5 {
				vh = append(vh, 0) // no properties
			}
			block = appendVarint(append(block, 0x30|byte(*qos)<<1), len(vh)+len(payload))
			block = append(append(block, vh...), payload...)
		}
	}
	return block
}

// publish encodes a QoS 0 PUBLISH.
func publish(b []byte, topic string, payload []byte, v5 bool) []byte {
	vh := appendStr(nil, topic)
	if v5 {
		vh = append(vh, 0) // no properties
	}
	b = appendVarint(append(b, 0x30), len(vh)+len(payload))
	return append(append(b, vh...), payload...)
}

// sparkplugBlocks pre-encodes the births of the edge nodes, and 256 DDATA
// messages per node (interleaved across nodes) with consecutive sequence
// numbers that follow the births, so the data block can be sent in a loop.
func sparkplugBlocks(v5 bool) (births, data []byte, n int) {
	metrics := max(50, *spK)
	for i := 0; i < *spNodes; i++ {
		edge := "edge-" + strconv.Itoa(i)
		nb := &sparkplug.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Seq: 0, HasSeq: true, Metrics: []sparkplug.Metric{
			{Name: "bdSeq", Datatype: sparkplug.TypeInt64, Value: sparkplug.Value{Kind: sparkplug.KindLong}},
			{Name: "Node Control/Rebirth", Datatype: sparkplug.TypeBoolean, Value: sparkplug.Value{Kind: sparkplug.KindBool}}}}
		births = publish(births, "spBv1.0/flood/NBIRTH/"+edge, sparkplug.Encode(nb), v5)
		db := &sparkplug.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Seq: 1, HasSeq: true}
		for m := 0; m < metrics; m++ {
			db.Metrics = append(db.Metrics, sparkplug.Metric{Name: "Sensors/S" + strconv.Itoa(m), Alias: uint64(10 + m), HasAlias: true,
				Datatype: sparkplug.TypeDouble, Value: sparkplug.Value{Kind: sparkplug.KindDouble}})
		}
		births = publish(births, "spBv1.0/flood/DBIRTH/"+edge+"/dev", sparkplug.Encode(db), v5)
	}
	for s := 0; s < 256; s++ {
		for i := 0; i < *spNodes; i++ {
			p := &sparkplug.Payload{Timestamp: uint64(time.Now().UnixMilli()), HasTimestamp: true, Seq: uint64(s+2) % 256, HasSeq: true}
			for j := 0; j < *spK; j++ {
				p.Metrics = append(p.Metrics, sparkplug.Metric{Alias: uint64(10 + (s**spK+j)%metrics), HasAlias: true,
					Value: sparkplug.Value{Kind: sparkplug.KindDouble, Float: float64(s) + float64(j)/8}})
			}
			data = publish(data, "spBv1.0/flood/DDATA/edge-"+strconv.Itoa(i)+"/dev", sparkplug.Encode(p), v5)
			n++
		}
	}
	return births, data, n
}

func init() { log.SetFlags(log.Ltime) }
