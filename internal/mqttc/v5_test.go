package mqttc

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

func v5(url string, mut func(*config.Broker)) config.Broker {
	b := broker(url, mut)
	b.ProtocolVersion = 5
	return b
}

// The same auth and transport matrix as MQTT 3.1.1, over MQTT 5.
func TestV5Transports(t *testing.T) {
	p := newPKI(t)
	_, plain := startBroker(t, p, brokerOpts{users: []string{"alice:s3cret"}})
	_, tlsAddr := startBroker(t, p, brokerOpts{serverCert: p.server})
	_, mtls := startBroker(t, p, brokerOpts{serverCert: p.server, mtls: true})
	_, wss := startBroker(t, p, brokerOpts{serverCert: p.server, websocket: true})
	cases := map[string]config.Broker{
		"password": v5("tcp://"+plain, func(b *config.Broker) { b.Username, b.Password = "alice", "s3cret" }),
		"tls":      v5("mqtts://"+tlsAddr, func(b *config.Broker) { b.TLS.CAPEM = p.ca.certPEM }),
		"mtls": v5("mqtts://"+mtls, func(b *config.Broker) {
			b.TLS.CAPEM, b.TLS.CertPEM, b.TLS.KeyPEM = p.ca.certPEM, p.client.certPEM, p.client.keyPEM
		}),
		"wss": v5("wss://"+wss+"/mqtt", func(b *config.Broker) { b.TLS.CAPEM = p.ca.certPEM }),
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			m, r := newManager(t)
			if err := m.Apply(cfg); err != nil {
				t.Fatal(err)
			}
			expectConnected(t, m, r)
			if m.Status().Protocol != "MQTT 5" {
				t.Fatalf("protocol %q", m.Status().Protocol)
			}
		})
	}
	t.Run("wrong_password_reason", func(t *testing.T) {
		m, _ := newManager(t)
		if err := m.Apply(v5("tcp://"+plain, func(b *config.Broker) { b.Username, b.Password = "alice", "nope" })); err != nil {
			t.Fatal(err)
		}
		// MQTT 5 brokers return a reason code; it must reach the status.
		expectRejected(t, m, "bad user name or password")
	})
	t.Run("password_file_rotation", func(t *testing.T) {
		pf := writeFile(t, "token", "stale\n")
		m, r := newManager(t)
		if err := m.Apply(v5("tcp://"+plain, func(b *config.Broker) { b.Username, b.PasswordFile = "alice", pf })); err != nil {
			t.Fatal(err)
		}
		expectRejected(t, m, "")
		_ = writeFileAt(pf, "s3cret\n")
		expectConnected(t, m, r)
	})
}

func TestV5Properties(t *testing.T) {
	p := newPKI(t)
	_, addr := startBroker(t, p, brokerOpts{})
	m, r := newManager(t)
	if err := m.Apply(v5("tcp://"+addr, nil)); err != nil {
		t.Fatal(err)
	}
	expectConnected(t, m, r)
	props := &store.Props{
		ContentType: "application/json", ResponseTopic: "tls/reply", CorrelationData: []byte("req-42"),
		UserProperties: []store.UserProperty{{Key: "site", Value: "lisbon"}, {Key: "site", Value: "porto"}},
		MessageExpiry:  60, PayloadUTF8: true,
	}
	if _, err := m.PublishMany(context.Background(), []Message{{Topic: "tls/props", Payload: []byte(`{"a":1}`), QoS: 1, Props: props}}); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-r.got:
		got := e.Props
		if e.Topic != "tls/props" || got == nil || got.ContentType != props.ContentType || got.ResponseTopic != props.ResponseTopic ||
			string(got.CorrelationData) != "req-42" || len(got.UserProperties) != 2 || got.UserProperties[1].Value != "porto" ||
			got.MessageExpiry == 0 || got.MessageExpiry > 60 || !got.PayloadUTF8 {
			t.Fatalf("properties did not round-trip: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message")
	}
}

// Many messages on the same few topics: the broker may use topic aliases
// (we allow 1024); every message must still carry the right topic.
func TestV5ManyMessagesKeepTopics(t *testing.T) {
	p := newPKI(t)
	b, addr := startBroker(t, p, brokerOpts{})
	got := make(chan string, 1000)
	m := New(quietLogger(), func(e store.Entry) { got <- e.Topic })
	t.Cleanup(m.Close)
	if err := m.Apply(v5("tcp://"+addr, nil)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !m.Status().Connected && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	for i := 0; i < 200; i++ {
		_ = b.Publish(fmt.Sprintf("tls/alias/%d", i%3), []byte("x"), false, 0)
	}
	for i := 0; i < 200; i++ {
		select {
		case tp := <-got:
			if !strings.HasPrefix(tp, "tls/alias/") {
				t.Fatalf("message %d has topic %q", i, tp)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("received %d of 200", i)
		}
	}
}

func TestV3RejectsProperties(t *testing.T) {
	p := newPKI(t)
	_, addr := startBroker(t, p, brokerOpts{})
	m, r := newManager(t)
	if err := m.Apply(broker("tcp://"+addr, nil)); err != nil {
		t.Fatal(err)
	}
	expectConnected(t, m, r)
	_, err := m.PublishMany(context.Background(), []Message{{Topic: "tls/x", Props: &store.Props{ContentType: "text/plain"}}})
	if err != ErrPropertiesNeedV5 {
		t.Fatalf("want ErrPropertiesNeedV5, got %v", err)
	}
}
