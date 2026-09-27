package mqttc

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// scriptedConns serves one handler per incoming connection, in order, after
// reading the CONNECT (each handler writes its own CONNACK).
func scriptedConns(t *testing.T, handlers ...func(c net.Conn, r *bufio.Reader)) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for i := 0; ; i++ {
			c, err := l.Accept()
			if err != nil {
				return
			}
			r := bufio.NewReader(c)
			if h, _, err := readPacket(r, nil); err != nil || h>>4 != pConnect {
				c.Close()
				continue
			}
			if i >= len(handlers) {
				c.Close()
				continue
			}
			go func(h func(net.Conn, *bufio.Reader)) {
				defer c.Close()
				h(c, r)
			}(handlers[i])
		}
	}()
	return "tcp://" + l.Addr().String()
}

// expectPacket reads the next packet of type want.
func expectPacket(t *testing.T, r *bufio.Reader, want byte) (byte, []byte) {
	t.Helper()
	h, body, err := readPacket(r, nil)
	if err != nil {
		t.Errorf("waiting for packet type %d: %v", want, err)
		return 0, nil
	}
	if h>>4 != want {
		t.Errorf("got packet type %d, want %d", h>>4, want)
	}
	return h, body
}

func persistentCfg(url string) config.Broker {
	no := false
	b := leanCfg(url)
	b.CleanSession = &no
	b.Subscriptions = []config.Subscription{} // no SUBSCRIBE: keeps the scripts short
	return b
}

func publishID(body []byte) uint16 {
	tl := int(binary.BigEndian.Uint16(body))
	return binary.BigEndian.Uint16(body[2+tl:])
}

func TestLeanSessionResumeQoS1(t *testing.T) {
	var firstID atomic.Uint32
	url := scriptedConns(t,
		func(c net.Conn, r *bufio.Reader) {
			_, _ = c.Write([]byte{pConnack << 4, 2, 0, 0})
			_, body := expectPacket(t, r, pPublish)
			firstID.Store(uint32(publishID(body))) // no PUBACK: the connection drops
		},
		func(c net.Conn, r *bufio.Reader) {
			_, _ = c.Write([]byte{pConnack << 4, 2, 1, 0}) // session present
			h, body := expectPacket(t, r, pPublish)
			if h&0x08 == 0 || uint32(publishID(body)) != firstID.Load() || !strings.HasSuffix(string(body), "hello") {
				t.Errorf("resent PUBLISH: header %x id %d (want DUP, id %d)", h, publishID(body), firstID.Load())
			}
			id := publishID(body)
			_, _ = c.Write([]byte{pPuback << 4, 2, byte(id >> 8), byte(id)})
			time.Sleep(time.Second)
		})
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(persistentCfg(url)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := m.PublishMany(ctx, []Message{{Topic: "a/b", Payload: []byte("hello"), QoS: 1}}); err != nil {
		t.Fatalf("publish should survive the reconnect: %v", err)
	}
}

func TestLeanSessionResumeQoS2(t *testing.T) {
	url := scriptedConns(t,
		func(c net.Conn, r *bufio.Reader) {
			_, _ = c.Write([]byte{pConnack << 4, 2, 0, 0})
			_, body := expectPacket(t, r, pPublish)
			id := publishID(body)
			_, _ = c.Write([]byte{pPubrec << 4, 2, byte(id >> 8), byte(id)})
			expectPacket(t, r, pPubrel) // no PUBCOMP: the connection drops
			// Meanwhile an inbound QoS 2 message, acknowledged with PUBREC.
			_, _ = c.Write(publishPacket("in/q2", 2, 40, "once", false))
			expectPacket(t, r, pPubrec)
		},
		func(c net.Conn, r *bufio.Reader) {
			_, _ = c.Write([]byte{pConnack << 4, 2, 1, 0}) // session present
			_, body := expectPacket(t, r, pPubrel)
			id := binary.BigEndian.Uint16(body)
			_, _ = c.Write([]byte{pPubcomp << 4, 2, byte(id >> 8), byte(id)})
			// The broker redelivers the inbound message: it must not be
			// delivered twice.
			_, _ = c.Write(publishPacket("in/q2", 2, 40, "once", true))
			expectPacket(t, r, pPubrec)
			_, _ = c.Write([]byte{pPubrel<<4 | 2, 2, 0, 40})
			expectPacket(t, r, pPubcomp)
			time.Sleep(time.Second)
		})
	col := &collector{}
	m := New(quietLogger(), col.handle)
	defer m.Close()
	if err := m.Apply(persistentCfg(url)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if _, err := m.PublishMany(ctx, []Message{{Topic: "a/b", Payload: []byte("x"), QoS: 2}}); err != nil {
		t.Fatalf("QoS 2 publish should complete after the reconnect: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if n := col.count(); n != 1 {
		t.Fatalf("inbound QoS 2 message delivered %d times, want 1", n)
	}
}

// If the broker did not keep the session, in-flight publishes fail with a
// clear error rather than hanging or silently succeeding.
func TestLeanSessionLost(t *testing.T) {
	url := scriptedConns(t,
		func(c net.Conn, r *bufio.Reader) {
			_, _ = c.Write([]byte{pConnack << 4, 2, 0, 0})
			expectPacket(t, r, pPublish)
		},
		func(c net.Conn, r *bufio.Reader) {
			_, _ = c.Write([]byte{pConnack << 4, 2, 0, 0}) // no session
			time.Sleep(time.Second)
		})
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(persistentCfg(url)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err := m.PublishMany(ctx, []Message{{Topic: "a/b", QoS: 1}})
	if !errors.Is(err, errSessionLost) {
		t.Fatalf("want errSessionLost, got %v", err)
	}
}

// Clean sessions keep the old behaviour: in-flight publishes fail as soon
// as the connection drops.
func TestLeanCleanSessionFailsFast(t *testing.T) {
	url := scriptedConns(t, func(c net.Conn, r *bufio.Reader) {
		_, _ = c.Write([]byte{pConnack << 4, 2, 0, 0})
		expectPacket(t, r, pPublish)
	})
	cfg := persistentCfg(url)
	cfg.CleanSession = nil
	m := New(quietLogger(), func(store.Entry) {})
	defer m.Close()
	if err := m.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "connect", func() bool { return m.Status().Connected })
	start := time.Now()
	_, err := m.PublishMany(context.Background(), []Message{{Topic: "a/b", QoS: 1}})
	if !errors.Is(err, ErrNotConnected) || time.Since(start) > 2*time.Second {
		t.Fatalf("want a fast ErrNotConnected, got %v after %v", err, time.Since(start))
	}
}
