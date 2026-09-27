package mqttc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

func newFakeBroker5(t *testing.T, connackProps []byte, script func(c net.Conn, r *bufio.Reader)) *fakeBroker {
	f := newFakeBroker(t, script)
	f.v5, f.connackProps = true, connackProps
	return f
}

func leanCfg5(url string) config.Broker {
	b := leanCfg(url)
	b.ProtocolVersion = 5
	return b
}

// publishPacket5 builds an MQTT 5 PUBLISH with raw properties.
func publishPacket5(topic string, qos byte, id uint16, props []byte, payload string) []byte {
	b := appendStr(nil, topic)
	if qos > 0 {
		b = binary.BigEndian.AppendUint16(b, id)
	}
	b = append(appendVarint(b, len(props)), props...)
	return packet(byte(pPublish<<4)|qos<<1, append(b, payload...))
}

func u16prop(id byte, v uint16) []byte { return binary.BigEndian.AppendUint16([]byte{id}, v) }
func u32prop(id byte, v uint32) []byte { return binary.BigEndian.AppendUint32([]byte{id}, v) }
func strprop(id byte, v string) []byte { return appendStr([]byte{id}, v) }

func cat(bs ...[]byte) []byte { return bytes.Join(bs, nil) }

func TestLeanV5PropertiesAndAliases(t *testing.T) {
	var conns atomic.Int32
	f := newFakeBroker5(t, nil, func(c net.Conn, _ *bufio.Reader) {
		if conns.Add(1) > 1 {
			return
		}
		props := cat(u16prop(propTopicAlias, 7), strprop(propContentType, "text/plain"),
			strprop(propCorrelationData, "c-1"), []byte{propPayloadFormat, 1}, u32prop(propMessageExpiry, 30),
			appendStr(strprop(propUserProperty, "k"), "v"), []byte{0x0B, 5}) // + a subscription identifier
		_, _ = c.Write(publishPacket5("sensors/1", 1, 1, props, "first"))
		_, _ = c.Write(publishPacket5("", 0, 0, u16prop(propTopicAlias, 7), "via alias"))
		_, _ = c.Write(publishPacket5("plain/topic", 0, 0, nil, "no props"))
		_, _ = c.Write(publishPacket5("", 0, 0, u16prop(propTopicAlias, 9), "unknown alias"))
	})
	col := &collector{}
	m := New(quietLogger(), col.handle)
	defer m.Close()
	if err := m.Apply(leanCfg5(f.url())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "3 messages", func() bool { return col.count() >= 3 })
	// The unknown alias is a protocol error: the client disconnects with
	// a reason code and reconnects.
	waitFor(t, "reconnect after unknown alias", func() bool { return conns.Load() >= 2 })
	col.mu.Lock()
	defer col.mu.Unlock()
	first, alias, plain := col.msgs[0], col.msgs[1], col.msgs[2]
	pr := first.Props
	if first.Topic != "sensors/1" || pr == nil || pr.ContentType != "text/plain" || string(pr.CorrelationData) != "c-1" ||
		!pr.PayloadUTF8 || pr.MessageExpiry != 30 || len(pr.UserProperties) != 1 || pr.UserProperties[0] != (store.UserProperty{Key: "k", Value: "v"}) ||
		string(first.Payload) != "first" {
		t.Fatalf("first message: %+v props %+v", first, pr)
	}
	if alias.Topic != "sensors/1" || string(alias.Payload) != "via alias" || alias.Props != nil {
		t.Fatalf("alias message: %+v", alias)
	}
	if plain.Topic != "plain/topic" || plain.Props != nil || string(plain.Payload) != "no props" {
		t.Fatalf("plain message: %+v", plain)
	}
	sawDisconnect := false
	for _, h := range f.received() {
		sawDisconnect = sawDisconnect || h>>4 == pDisconnect
	}
	if !sawDisconnect {
		t.Fatal("client did not send DISCONNECT with a reason code")
	}
}

// Receive Maximum from the broker bounds unacknowledged QoS 1 publishes.
func TestLeanV5ReceiveMaximum(t *testing.T) {
	var maxOutstanding atomic.Int32
	f := newFakeBroker5(t, u16prop(propReceiveMaximum, 2), func(c net.Conn, r *bufio.Reader) {
		var ids []uint16
		for got := 0; got < 6; {
			_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
			h, body, err := readPacket(r, nil)
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				// Stalled: the client is waiting for acks. Ack everything.
				if int32(len(ids)) > maxOutstanding.Load() {
					maxOutstanding.Store(int32(len(ids)))
				}
				for _, id := range ids {
					_, _ = c.Write([]byte{pPuback << 4, 2, byte(id >> 8), byte(id)})
				}
				ids = ids[:0]
				continue
			}
			if err != nil {
				return
			}
			if h>>4 == pPublish {
				tl := int(binary.BigEndian.Uint16(body))
				ids = append(ids, binary.BigEndian.Uint16(body[2+tl:]))
				got++
			}
		}
		for _, id := range ids {
			_, _ = c.Write([]byte{pPuback << 4, 2, byte(id >> 8), byte(id)})
		}
		_ = c.SetReadDeadline(time.Time{})
	})
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(leanCfg5(f.url())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	var msgs []Message
	for i := 0; i < 6; i++ {
		msgs = append(msgs, Message{Topic: "q", Payload: []byte("x"), QoS: 1})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n, err := m.PublishMany(ctx, msgs); err != nil || n != 6 {
		t.Fatalf("published %d: %v", n, err)
	}
	if got := maxOutstanding.Load(); got != 2 {
		t.Fatalf("%d publishes were outstanding at once, want the broker's Receive Maximum of 2", got)
	}
}

// Broker limits from CONNACK are enforced before sending.
func TestLeanV5BrokerLimits(t *testing.T) {
	f := newFakeBroker5(t, cat([]byte{propMaximumQoS, 1, propRetainAvailable, 0}, u32prop(propMaximumPacketSize, 64)), nil)
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(leanCfg5(f.url())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	for _, c := range []struct {
		msg  Message
		want string
	}{
		{Message{Topic: "a", QoS: 2}, "QoS up to 1"},
		{Message{Topic: "a", Retain: true}, "retained"},
		{Message{Topic: "a", Payload: make([]byte, 100)}, "maximum packet size"},
	} {
		if _, err := m.PublishMany(context.Background(), []Message{c.msg}); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("want error containing %q, got %v", c.want, err)
		}
	}
	if _, err := m.PublishMany(context.Background(), []Message{{Topic: "a", Payload: []byte("ok")}}); err != nil {
		t.Fatalf("a publish within limits failed: %v", err)
	}
}

// PUBACK / SUBACK / DISCONNECT reason codes reach the caller and status.
func TestLeanV5ReasonCodes(t *testing.T) {
	withLen := func(props []byte) []byte { return append(appendVarint(nil, len(props)), props...) }
	reason := withLen(strprop(propReasonString, "acl says no"))
	f := newFakeBroker5(t, nil, func(c net.Conn, r *bufio.Reader) {
		for {
			h, body, err := readPacket(r, nil)
			if err != nil {
				return
			}
			if h>>4 != pPublish {
				continue
			}
			tl := int(binary.BigEndian.Uint16(body))
			id := body[2+tl : 4+tl]
			_, _ = c.Write(packet(pPuback<<4, cat(id, []byte{0x87}, reason)))
			time.Sleep(50 * time.Millisecond)
			_, _ = c.Write(packet(pDisconnect<<4, cat([]byte{0x98}, withLen(strprop(propReasonString, "maintenance")))))
			return
		}
	})
	logs := &syncBuffer{}
	m := New(slog.New(slog.NewTextHandler(logs, nil)), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(leanCfg5(f.url())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	_, err := m.PublishMany(context.Background(), []Message{{Topic: "a", QoS: 1}})
	if err == nil || !strings.Contains(err.Error(), "not authorized: acl says no") {
		t.Fatalf("publish error %v", err)
	}
	// The client reconnects at once (clearing the status error), so check
	// the log.
	waitFor(t, "server disconnect reason", func() bool {
		return strings.Contains(logs.String(), "server disconnected: reason 0x98 administrative action: maintenance")
	})
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestLeanV5SubscriptionRejected(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		_, _, _ = readPacket(r, nil)
		_, _ = c.Write([]byte{pConnack << 4, 3, 0, 0, 0})
		_, body, _ := readPacket(r, nil)
		props := strprop(propReasonString, "no wildcards here")
		_, _ = c.Write(packet(pSuback<<4, cat(body[:2], appendVarint(nil, len(props)), props, []byte{0xA2})))
		_, _ = io.Copy(io.Discard, r)
	}()
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(leanCfg5("tcp://" + l.Addr().String())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "rejection with reason", func() bool {
		return strings.Contains(m.Status().LastError, "wildcard subscriptions not supported: no wildcards here")
	})
}

// A Server Keep Alive in CONNACK replaces the configured keepalive.
func TestLeanV5ServerKeepAlive(t *testing.T) {
	var conns atomic.Int32
	f := newFakeBroker5(t, u16prop(propServerKeepAlive, 1), func(net.Conn, *bufio.Reader) { conns.Add(1) })
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	cfg := leanCfg5(f.url())
	cfg.KeepAliveSec = 600
	if err := m.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	// With the broker's 1 s keepalive and no PINGRESP, the watchdog fires.
	waitFor(t, "watchdog reconnect", func() bool { return conns.Load() >= 2 })
}

// The CONNECT packet carries MQTT 5 level, session expiry and alias maximum.
func TestLeanV5ConnectPacket(t *testing.T) {
	aliasMax := uint16(16)
	cfg := leanCfg5("tcp://x")
	cfg.SessionExpirySec, cfg.TopicAliasMaximum = 3600, &aliasMax
	l := newLean(New(quietLogger(), nil), cfg, nil, 0)
	pkt := l.connectPacket()
	r := bufio.NewReader(bytes.NewReader(pkt))
	_, body, err := readPacket(r, nil)
	if err != nil {
		t.Fatal(err)
	}
	if body[6] != 5 {
		t.Fatalf("protocol level %d", body[6])
	}
	props, rest, err := splitProps(body[10:])
	if err != nil {
		t.Fatal(err)
	}
	want := cat(u32prop(propSessionExpiry, 3600), u16prop(propTopicAliasMaximum, 16))
	if !bytes.Equal(props, want) || string(rest[2:2+len("lean-test")]) != "lean-test" {
		t.Fatalf("properties %x, want %x", props, want)
	}
}

// The Eclipse paho.golang client remains available for MQTT 5.
func TestPahoV5ClientFallback(t *testing.T) {
	p := newPKI(t)
	_, addr := startBroker(t, p, brokerOpts{})
	m, r := newManager(t)
	if err := m.Apply(v5("tcp://"+addr, func(b *config.Broker) { b.Client = config.ClientPaho })); err != nil {
		t.Fatal(err)
	}
	expectConnected(t, m, r)
}

func FuzzLeanReadLoopV5(f *testing.F) {
	f.Add(publishPacket5("a/b", 0, 0, nil, "hello"))
	f.Add(publishPacket5("a/b", 1, 3, cat(u16prop(propTopicAlias, 1), strprop(propContentType, "x")), "p"))
	f.Add(publishPacket5("", 0, 0, u16prop(propTopicAlias, 1), "p"))
	f.Add(packet(pDisconnect<<4, []byte{0x98, 0}))
	f.Add(packet(pPuback<<4, []byte{0, 1, 0x87, 3, 0x1F, 0, 0}))
	f.Fuzz(func(t *testing.T, data []byte) {
		cfg := config.Broker{ProtocolVersion: 5}
		l := newLean(New(quietLogger(), func(store.Entry) {}), cfg, nil, 0)
		l.pending, l.inflight = map[uint16]*pending{}, map[uint16]bool{}
		r := bufio.NewReader(io.LimitReader(bytes.NewReader(data), 1<<20))
		_ = l.readLoop(r) // must not panic
	})
}
