// Command floodbroker is a minimal fake MQTT broker for benchmarking MQTT
// subscribers such as mqtt-get. After a client subscribes it streams
// pre-encoded QoS 0 PUBLISH packets matching each subscribed filter as fast
// as the socket accepts them, so the measurement is limited by the
// subscriber, not by a real broker. Supports MQTT 3.1.1 and 5.
//
//	go run ./cmd/floodbroker -addr :1884 -topics 1000 -size 64
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
)

var (
	addr    = flag.String("addr", "127.0.0.1:1884", "listen address")
	nTopics = flag.Int("topics", 1000, "distinct topics per subscribed filter")
	size    = flag.Int("size", 64, "payload bytes")
	qos     = flag.Int("qos", 0, "QoS of the flood (0 or 1; acknowledgements are read and ignored)")
	props   = flag.Bool("props", false, "MQTT 5: give every message a content type, a user property and an expiry")
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

func init() { log.SetFlags(log.Ltime) }
