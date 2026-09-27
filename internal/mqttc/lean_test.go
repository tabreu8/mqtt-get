package mqttc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// fakeBroker is a scripted MQTT 3.1.1 server for protocol edge cases that
// real brokers rarely produce on demand.
type fakeBroker struct {
	t    *testing.T
	l    net.Listener
	mu   sync.Mutex
	seen []byte // packet types received from the client (after CONNECT)
	// script runs after CONNACK + SUBACK with the connection.
	script func(c net.Conn, r *bufio.Reader)
	// connack return code (0 = accepted).
	rc byte
	// v5 speaks MQTT 5: CONNACK carries connackProps, SUBACK has properties.
	v5           bool
	connackProps []byte
}

func newFakeBroker(t *testing.T, script func(c net.Conn, r *bufio.Reader)) *fakeBroker {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeBroker{t: t, l: l, script: script}
	go f.serve()
	t.Cleanup(func() { l.Close() })
	return f
}

func (f *fakeBroker) url() string { return "tcp://" + f.l.Addr().String() }

func (f *fakeBroker) record(h byte) {
	f.mu.Lock()
	f.seen = append(f.seen, h)
	f.mu.Unlock()
}

func (f *fakeBroker) received() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.seen...)
}

func (f *fakeBroker) serve() {
	for {
		c, err := f.l.Accept()
		if err != nil {
			return
		}
		go func() {
			defer c.Close()
			r := bufio.NewReader(c)
			if h, _, err := readPacket(r, nil); err != nil || h>>4 != pConnect {
				return
			}
			if f.v5 {
				_, _ = c.Write(packet(pConnack<<4, append(appendVarint([]byte{0, f.rc}, len(f.connackProps)), f.connackProps...)))
			} else {
				_, _ = c.Write([]byte{pConnack << 4, 2, 0, f.rc})
			}
			if f.rc != 0 {
				return
			}
			h, body, err := readPacket(r, nil)
			if err != nil || h>>4 != pSubscribe {
				return
			}
			if f.v5 {
				_, _ = c.Write([]byte{pSuback << 4, 4, body[0], body[1], 0, 1})
			} else {
				_, _ = c.Write([]byte{pSuback << 4, 3, body[0], body[1], 1})
			}
			if f.script != nil {
				f.script(c, r)
			}
			for { // record whatever the client sends afterwards
				h, _, err := readPacket(r, nil)
				if err != nil {
					return
				}
				f.record(h)
			}
		}()
	}
}

func publishPacket(topic string, qos byte, id uint16, payload string, dup bool) []byte {
	var b []byte
	b = appendStr(b, topic)
	if qos > 0 {
		b = binary.BigEndian.AppendUint16(b, id)
	}
	b = append(b, payload...)
	h := byte(pPublish<<4) | qos<<1
	if dup {
		h |= 0x08
	}
	return packet(h, b)
}

type collector struct {
	mu   sync.Mutex
	msgs []store.Entry
}

func (c *collector) handle(e store.Entry) {
	c.mu.Lock()
	c.msgs = append(c.msgs, e)
	c.mu.Unlock()
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func leanCfg(url string) config.Broker {
	return config.Broker{URLs: []string{url}, ClientID: "lean-test", ConnectTimeoutSec: 2, Client: config.ClientLean,
		Subscriptions: []config.Subscription{{Filter: "#", QoS: 2}}}
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// QoS 2 exactly-once: a redelivered PUBLISH (DUP) before PUBREL must be
// acknowledged but not delivered twice; the handshake must complete.
func TestLeanQoS2ExactlyOnce(t *testing.T) {
	f := newFakeBroker(t, func(c net.Conn, _ *bufio.Reader) {
		_, _ = c.Write(publishPacket("a/b", 2, 7, "once", false))
		_, _ = c.Write(publishPacket("a/b", 2, 7, "once", true)) // redelivery
		time.Sleep(100 * time.Millisecond)
		_, _ = c.Write([]byte{pPubrel<<4 | 2, 2, 0, 7})
		_, _ = c.Write(publishPacket("a/c", 1, 8, "qos1", false))
	})
	col := &collector{}
	m := New(quietLogger(), col.handle)
	defer m.Close()
	if err := m.Apply(leanCfg(f.url())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "acks", func() bool { return len(f.received()) >= 4 })
	time.Sleep(100 * time.Millisecond)
	if col.count() != 2 {
		t.Fatalf("delivered %d messages, want 2 (QoS 2 duplicate must be suppressed)", col.count())
	}
	// PUBREC, PUBREC (for the duplicate), PUBCOMP, PUBACK.
	want := []byte{pPubrec << 4, pPubrec << 4, pPubcomp << 4, pPuback << 4}
	if got := f.received()[:4]; !bytes.Equal(got, want) {
		t.Fatalf("client sent %x, want %x", got, want)
	}
}

// A broker that stops answering is detected by the keepalive watchdog and
// the client reconnects.
func TestLeanKeepaliveWatchdog(t *testing.T) {
	var conns atomic.Int32
	f := newFakeBroker(t, func(c net.Conn, _ *bufio.Reader) {
		conns.Add(1)
		// Never answer PINGREQ.
	})
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	cfg := leanCfg(f.url())
	cfg.KeepAliveSec = 1
	if err := m.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "first connection", func() bool { return conns.Load() == 1 })
	waitFor(t, "watchdog reconnect", func() bool { return conns.Load() >= 2 })
	pings := 0
	for _, h := range f.received() {
		if h>>4 == pPingreq {
			pings++
		}
	}
	if pings == 0 {
		t.Fatal("client never sent PINGREQ")
	}
}

func TestLeanConnackRefused(t *testing.T) {
	f := newFakeBroker(t, nil)
	f.rc = 5
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(leanCfg(f.url())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "refusal reported", func() bool { return strings.Contains(m.Status().LastError, "not authorized") })
}

// Malformed packets close the connection (and reconnect); they never panic.
func TestLeanMalformedPublish(t *testing.T) {
	var conns atomic.Int32
	f := newFakeBroker(t, func(c net.Conn, _ *bufio.Reader) {
		conns.Add(1)
		// Topic length 200 but only 3 bytes of body.
		_, _ = c.Write([]byte{pPublish << 4, 3, 0, 200, 'x'})
	})
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(leanCfg(f.url())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "reconnect after malformed packet", func() bool { return conns.Load() >= 2 })
}

// Broker restart: the client reconnects and resubscribes by itself.
func TestLeanReconnectAndResubscribe(t *testing.T) {
	p := newPKI(t)
	b, addr := startBroker(t, p, brokerOpts{})
	col := &collector{}
	m := New(quietLogger(), col.handle)
	defer m.Close()
	cfg := leanCfg("tcp://" + addr)
	if err := m.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	_ = b.Close()
	waitFor(t, "disconnect noticed", func() bool { return !m.Status().Connected })
	b2 := restartBroker(t, addr)
	waitFor(t, "reconnect", func() bool { return m.Status().Connected })
	time.Sleep(200 * time.Millisecond)
	_ = b2.Publish("after/restart", []byte("ok"), false, 1)
	waitFor(t, "message after restart", func() bool { return col.count() == 1 })
}

func TestLeanSubscriptionRejected(t *testing.T) {
	// A broker whose SUBACK rejects the filter (0x80).
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
		_, _ = c.Write([]byte{pConnack << 4, 2, 0, 0})
		_, body, _ := readPacket(r, nil)
		_, _ = c.Write([]byte{pSuback << 4, 3, body[0], body[1], 0x80})
		_, _ = io.Copy(io.Discard, r)
	}()
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(leanCfg("tcp://" + l.Addr().String())); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "rejection reported", func() bool { return strings.Contains(m.Status().LastError, "subscription rejected") })
}

// FuzzLeanReadLoop feeds arbitrary bytes to the packet parser: it must
// return an error or deliver messages, never panic.
func FuzzLeanReadLoop(f *testing.F) {
	f.Add(publishPacket("a/b", 0, 0, "hello", false))
	f.Add(publishPacket("a/b", 2, 9, "x", false))
	f.Add([]byte{pPubrel<<4 | 2, 2, 0, 9, pPingresp << 4, 0})
	f.Add([]byte{pPublish << 4, 0xFF, 0xFF, 0xFF, 0x7F})
	f.Fuzz(func(t *testing.T, data []byte) {
		l := newLean(New(quietLogger(), func(store.Entry) {}), config.Broker{}, nil, 0)
		l.pending, l.inflight = map[uint16]*pending{}, map[uint16]bool{}
		r := bufio.NewReader(io.LimitReader(bytes.NewReader(data), 1<<20))
		_ = l.readLoop(r) // must not panic
	})
}

func TestLeanPublishQoS(t *testing.T) {
	p := newPKI(t)
	_, addr := startBroker(t, p, brokerOpts{})
	col := &collector{}
	m := New(quietLogger(), col.handle)
	defer m.Close()
	if err := m.Apply(leanCfg("tcp://" + addr)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	time.Sleep(150 * time.Millisecond)
	var msgs []Message
	for i := 0; i < 300; i++ {
		msgs = append(msgs, Message{Topic: fmt.Sprintf("q/%d", i%3), Payload: []byte(fmt.Sprint(i)), QoS: byte(i % 3)})
	}
	if n, err := m.PublishMany(t.Context(), msgs); err != nil || n != 300 {
		t.Fatalf("published %d: %v", n, err)
	}
	waitFor(t, "300 messages back through the broker", func() bool { return col.count() == 300 })
}

// The paho client remains available as a fallback (client: "paho").
func TestPahoClientFallback(t *testing.T) {
	p := newPKI(t)
	_, plain := startBroker(t, p, brokerOpts{users: []string{"alice:s3cret"}})
	_, mtls := startBroker(t, p, brokerOpts{serverCert: p.server, mtls: true})
	for name, cfg := range map[string]config.Broker{
		"password": broker("tcp://"+plain, func(b *config.Broker) { b.Username, b.Password = "alice", "s3cret" }),
		"mtls": broker("mqtts://"+mtls, func(b *config.Broker) {
			b.TLS.CAPEM, b.TLS.CertPEM, b.TLS.KeyPEM = p.ca.certPEM, p.client.certPEM, p.client.keyPEM
		}),
	} {
		t.Run(name, func(t *testing.T) {
			cfg.Client = config.ClientPaho
			m, r := newManager(t)
			if err := m.Apply(cfg); err != nil {
				t.Fatal(err)
			}
			expectConnected(t, m, r)
		})
	}
}
