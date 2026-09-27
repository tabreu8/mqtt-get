package mqttc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// v3link is an MQTT 3.1 / 3.1.1 connection (eclipse/paho.mqtt.golang).
type v3link struct {
	m      *Manager
	client mqtt.Client
	up     atomic.Bool
	ctr    counters
}

func newV3(m *Manager, cfg config.Broker, tlsCfg *tls.Config, idx int) *v3link {
	l := &v3link{m: m}
	o := mqtt.NewClientOptions()
	for _, u := range cfg.URLs {
		o.AddBroker(u)
	}
	clientID := clientIDFor(cfg, idx)
	o.SetClientID(clientID)
	o.SetProtocolVersion(cfg.ProtocolVersion)
	o.SetCleanSession(cfg.UseCleanSession())
	o.SetKeepAlive(time.Duration(cfg.KeepAliveSec) * time.Second)
	o.SetConnectTimeout(time.Duration(cfg.ConnectTimeoutSec) * time.Second)
	o.SetWriteTimeout(10 * time.Second)
	o.SetAutoReconnect(true)
	o.SetConnectRetry(true)
	o.SetConnectRetryInterval(2 * time.Second)
	o.SetMaxReconnectInterval(30 * time.Second)
	// Deliver messages sequentially on the connection's goroutine: our
	// handler is non-blocking, and this avoids a goroutine per message.
	o.SetOrderMatters(true)
	o.SetCustomOpenConnectionFn(dialer(cfg.TLS.Enabled))
	if tlsCfg != nil {
		o.SetTLSConfig(tlsCfg)
	}
	if len(cfg.WSHeaders) > 0 {
		h := http.Header{}
		for k, v := range cfg.WSHeaders {
			h.Set(k, v)
		}
		o.SetHTTPHeaders(h)
	}
	if hasCredentials(cfg) {
		o.SetCredentialsProvider(func() (string, string) { return m.credentials(cfg) })
	}
	filters := map[string]byte{}
	for _, s := range cfg.SubscriptionsFor(idx) {
		filters[s.Filter] = s.QoS
	}
	o.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) {
		m.deliver(&l.ctr, store.Entry{
			Topic:    msg.Topic(),
			Payload:  msg.Payload(),
			QoS:      msg.Qos(),
			Retained: msg.Retained(),
			Time:     time.Now().UnixNano(),
		})
	})
	if w := cfg.Will; w != nil && idx == 0 {
		o.SetBinaryWill(w.Topic, []byte(w.Payload), w.QoS, w.Retain)
	}
	o.SetOnConnectHandler(func(cl mqtt.Client) {
		l.up.Store(true)
		m.linkUp(clientID)
		// paho replays its stored in-flight messages concurrently with this
		// handler after a reconnect; a QoS 1/2 publish made during that
		// replay is sent twice (and races inside paho). Wait it out.
		m.announceOnline(cfg, idx, l, 250*time.Millisecond)
		if len(filters) == 0 {
			return
		}
		// nil callback: messages go to the default publish handler.
		tok := cl.SubscribeMultiple(filters, nil)
		go func() {
			if !tok.WaitTimeout(30 * time.Second) {
				m.setErr("subscribe timed out")
				return
			}
			if err := tok.Error(); err != nil {
				m.setErr("subscribe: " + err.Error())
				return
			}
			if st, ok := tok.(*mqtt.SubscribeToken); ok {
				for f, code := range st.Result() {
					if code >= 0x80 {
						m.subscriptionRejected(f, "")
					}
				}
			}
		}()
	})
	o.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		l.up.Store(false)
		m.linkDown(err.Error())
	})
	o.SetConnectionNotificationHandler(func(_ mqtt.Client, n mqtt.ConnectionNotification) {
		if f, ok := n.(mqtt.ConnectionNotificationFailed); ok {
			m.setErr("connect failed: " + f.Reason.Error())
		}
		if f, ok := n.(mqtt.ConnectionNotificationBrokerFailed); ok {
			m.setErr("connect to " + f.Broker.Redacted() + " failed: " + f.Reason.Error())
		}
	})
	l.client = mqtt.NewClient(o)
	return l
}

func (l *v3link) start()           { l.client.Connect() } // retries in the background
func (l *v3link) isUp() bool       { return l.up.Load() }
func (l *v3link) stats() *counters { return &l.ctr }

func (l *v3link) stop() {
	l.client.Disconnect(250)
	l.up.Store(false)
}

func (l *v3link) send(ctx context.Context, msg Message) func() error {
	if !msg.Props.Empty() {
		return func() error { return ErrPropertiesNeedV5 }
	}
	tok := l.client.Publish(msg.Topic, msg.QoS, msg.Retain, msg.Payload)
	return func() error {
		select {
		case <-tok.Done():
			return tok.Error()
		case <-ctx.Done():
			return fmt.Errorf("publish: %w", ctx.Err())
		}
	}
}
