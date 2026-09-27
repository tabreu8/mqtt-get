package mqttc

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
)

// With subscription_mode "split" each connection subscribes to a different
// subset of the filters, so ingest is spread over connections without any
// broker support, and every message is received exactly once.
func TestSplitSubscriptions(t *testing.T) {
	p := newPKI(t)
	broker, addr := startBroker(t, p, brokerOpts{})
	m, _ := newManager(t)
	cfg := config.Broker{
		URLs: []string{"tcp://" + addr}, ClientID: "split", Connections: 3,
		SubscriptionMode: config.SubscriptionSplit,
		Subscriptions: []config.Subscription{
			{Filter: "factory/#", QoS: 1}, {Filter: "energy/#", QoS: 1}, {Filter: "fleet/#", QoS: 1},
		},
	}
	if err := m.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for m.Status().ConnectedCount != 3 {
		if time.Now().After(deadline) {
			t.Fatalf("status: %+v", m.Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 30; i++ {
		_ = broker.Publish(fmt.Sprintf("factory/m/%d", i), []byte("x"), false, 1)
		if i < 20 {
			_ = broker.Publish(fmt.Sprintf("energy/m/%d", i), []byte("x"), false, 1)
		}
		if i < 10 {
			_ = broker.Publish(fmt.Sprintf("fleet/m/%d", i), []byte("x"), false, 1)
		}
		_ = broker.Publish("other/ignored", []byte("x"), false, 1)
	}
	for m.Status().MessagesReceived < 60 {
		if time.Now().After(deadline.Add(5 * time.Second)) {
			t.Fatalf("received %d of 60", m.Status().MessagesReceived)
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	st := m.Status()
	if st.MessagesReceived != 60 {
		t.Fatalf("received %d, want exactly 60 (no duplicates)", st.MessagesReceived)
	}
	want := []uint64{30, 20, 10}
	for i, w := range want {
		if st.ReceivedPerConnection[i] != w {
			t.Fatalf("per connection %v, want %v (filters %v)", st.ReceivedPerConnection, want, st.FiltersPerConnection)
		}
	}
	if st.FiltersPerConnection[1][0] != "energy/#" {
		t.Fatalf("filters per connection: %v", st.FiltersPerConnection)
	}
}

type recordConn struct {
	net.Conn
	frames [][]byte
}

func (r *recordConn) Write(p []byte) (int, error) {
	r.frames = append(r.frames, append([]byte(nil), p...))
	return len(p), nil
}

func TestPacketFramedConn(t *testing.T) {
	rec := &recordConn{}
	c := &packetFramedConn{Conn: rec}
	pingreq := []byte{0xC0, 0x00}
	big := append([]byte{0x30, 0x80, 0x01}, make([]byte, 128)...) // remaining length 128 (2-byte varint)
	// Header and body written separately, and two packets in one write.
	writes := [][]byte{big[:2], big[2:3], big[3:70], append(big[70:], pingreq...)}
	for _, w := range writes {
		if n, err := c.Write(w); err != nil || n != len(w) {
			t.Fatal(n, err)
		}
	}
	if len(rec.frames) != 2 || len(rec.frames[0]) != len(big) || string(rec.frames[1]) != string(pingreq) {
		t.Fatalf("frames: %d %v", len(rec.frames), rec.frames)
	}
}
