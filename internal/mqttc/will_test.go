package mqttc

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// Last Will as a status topic, on every client implementation: "online" on
// connect, the will ("offline") from the broker after an unexpected
// disconnect, and "offline" from mqtt-get itself on a graceful shutdown.
func TestWillStatusTopic(t *testing.T) {
	p := newPKI(t)
	b, addr := startBroker(t, p, brokerOpts{})
	for _, c := range []struct {
		name     string
		protocol uint
		client   string
	}{
		{"lean_v311", 4, config.ClientLean},
		{"lean_v5", 5, config.ClientLean},
		{"paho_v311", 4, config.ClientPaho},
		{"paho_v5", 5, config.ClientPaho},
	} {
		t.Run(c.name, func(t *testing.T) {
			statusTopic := "tls/status/" + c.name
			var mu sync.Mutex
			var seen []string
			observer := New(quietLogger(), func(e store.Entry) {
				if e.Topic == statusTopic {
					mu.Lock()
					seen = append(seen, string(e.Payload))
					mu.Unlock()
				}
			})
			defer observer.Close()
			if err := observer.Apply(broker("tcp://"+addr, func(b *config.Broker) { b.ClientID = "observer-" + c.name })); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "observer connected", func() bool { return observer.Status().Connected })
			time.Sleep(100 * time.Millisecond)
			got := func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(seen) }

			cfg := broker("tcp://"+addr, func(b *config.Broker) {
				b.ClientID = "will-" + c.name
				b.ProtocolVersion, b.Client = c.protocol, c.client
				b.Will = &config.Will{Topic: statusTopic, Payload: "offline", QoS: 1, OnlinePayload: "online"}
			})
			m := New(quietLogger(), func(store.Entry) {})
			if err := m.Apply(cfg); err != nil {
				t.Fatal(err)
			}
			waitFor(t, "online", func() bool { return slices.Equal(got(), []string{"online"}) })

			// Unexpected disconnect: the broker publishes the will; mqtt-get
			// reconnects and says it is online again.
			cl, ok := b.Clients.Get("will-" + c.name)
			if !ok {
				t.Fatal("client not found on the broker")
			}
			cl.Stop(errors.New("kicked by test"))
			defer func() {
				if t.Failed() {
					t.Logf("seen: %q", got())
				}
			}()
			waitFor(t, "will, then online again", func() bool {
				return slices.Equal(got(), []string{"online", "offline", "online"})
			})

			m.Close()
			waitFor(t, "offline on shutdown", func() bool {
				return slices.Equal(got(), []string{"online", "offline", "online", "offline"})
			})
		})
	}
}

func TestWillValidation(t *testing.T) {
	for _, c := range []struct {
		will *config.Will
		v    uint
	}{
		{&config.Will{Topic: "a/+"}, 4},
		{&config.Will{Topic: ""}, 4},
		{&config.Will{Topic: "a", QoS: 3}, 4},
		{&config.Will{Topic: "a", DelaySec: 5}, 4},
	} {
		b := config.Broker{URLs: []string{"tcp://x"}, ProtocolVersion: c.v, Will: c.will}
		b.Normalize()
		if b.Validate() == nil {
			t.Errorf("will %+v on protocol %d should be rejected", c.will, c.v)
		}
	}
}
