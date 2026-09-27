package mqttc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// v5link is an MQTT 5 connection (eclipse/paho.golang with autopaho for
// reconnection). Transport (TLS, mTLS, websockets, buffering) is shared with
// the MQTT 3 client through openConn.
type v5link struct {
	m        *Manager
	cfg      config.Broker
	tlsCfg   *tls.Config
	idx      int
	clientID string

	cm     *autopaho.ConnectionManager
	cancel context.CancelFunc
	up     atomic.Bool
	ctr    counters

	// aliases resolves inbound topic aliases, which are only valid within
	// one network connection. Only touched from paho's publish-routing
	// goroutine; aliasClient detects a new connection (new paho client).
	aliases     map[uint16]string
	aliasClient *paho.Client

	serverReason atomic.Pointer[string] // the broker's DISCONNECT reason, if any
}

// defaultTopicAliasMaximum is how many topic aliases the broker may use
// towards mqtt-get unless configured.
const defaultTopicAliasMaximum = 1024

// receiveMaximum is the MQTT 5 Receive Maximum sent in CONNECT.
const receiveMaximum = 1024

func newV5(m *Manager, cfg config.Broker, tlsCfg *tls.Config, idx int) *v5link {
	return &v5link{m: m, cfg: cfg, tlsCfg: tlsCfg, idx: idx, clientID: clientIDFor(cfg, idx)}
}

func (l *v5link) isUp() bool       { return l.up.Load() }
func (l *v5link) stats() *counters { return &l.ctr }

func (l *v5link) start() {
	cfg := l.cfg
	var urls []*url.URL
	for _, s := range cfg.URLs {
		u, err := url.Parse(s)
		if err != nil {
			l.m.setErr("invalid broker url: " + err.Error())
			return
		}
		urls = append(urls, u)
	}
	var headers http.Header
	if len(cfg.WSHeaders) > 0 {
		headers = http.Header{}
		for k, v := range cfg.WSHeaders {
			headers.Set(k, v)
		}
	}
	timeout := time.Duration(cfg.ConnectTimeoutSec) * time.Second
	// Receive Maximum bounds unacknowledged QoS 1/2 messages from the broker,
	// and paho.golang also sizes its inbound queue with it: 65535 let each
	// connection buffer tens of MB under load for no throughput gain.
	receiveMax := uint16(receiveMaximum)
	aliasMax := uint16(defaultTopicAliasMaximum)
	if cfg.TopicAliasMaximum != nil {
		aliasMax = *cfg.TopicAliasMaximum
	}
	subs := cfg.SubscriptionsFor(l.idx)

	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	cc := autopaho.ClientConfig{
		ServerUrls:                    urls,
		TlsCfg:                        l.tlsCfg,
		KeepAlive:                     uint16(cfg.KeepAliveSec),
		CleanStartOnInitialConnection: cfg.UseCleanSession(),
		SessionExpiryInterval:         cfg.SessionExpirySec,
		ConnectTimeout:                timeout,
		ReconnectBackoff:              backoff,
		AttemptConnection: func(_ context.Context, _ autopaho.ClientConfig, u *url.URL) (net.Conn, error) {
			c, err := openConn(u, dialOptions{TLSConfig: l.tlsCfg, ConnectTimeout: timeout, HTTPHeaders: headers, ForceTLS: cfg.TLS.Enabled})
			if err != nil {
				return nil, err
			}
			return packets.NewThreadSafeConn(c), nil
		},
		ConnectPacketBuilder: func(cp *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			if hasCredentials(cfg) {
				user, pass := l.m.credentials(cfg) // re-read on every connect
				cp.Username, cp.UsernameFlag = user, user != ""
				cp.Password, cp.PasswordFlag = []byte(pass), pass != ""
			}
			if cp.Properties == nil {
				cp.Properties = &paho.ConnectProperties{}
			}
			cp.Properties.ReceiveMaximum = &receiveMax
			cp.Properties.TopicAliasMaximum = &aliasMax
			cp.Properties.RequestProblemInfo = true
			return cp, nil
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			l.up.Store(true)
			l.m.linkUp(l.clientID)
			if len(subs) == 0 {
				return
			}
			go l.subscribe(ctx, cm, subs)
		},
		OnConnectionDown: func() bool {
			l.up.Store(false)
			reason := "disconnected"
			if r := l.serverReason.Swap(nil); r != nil {
				reason = *r
			}
			l.m.linkDown(reason)
			return true // keep reconnecting
		},
		OnConnectError: func(err error) { l.m.setErr("connect failed: " + describeV5Error(err)) },
		ClientConfig: paho.ClientConfig{
			ClientID:          l.clientID,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){l.onPublish},
			OnClientError:     func(err error) { l.m.setErr("client error: " + err.Error()) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				r := "server disconnected: " + reasonText(d.ReasonCode, disconnectReason(d))
				l.serverReason.Store(&r)
				l.m.setErr(r)
			},
		},
	}
	cm, err := autopaho.NewConnection(ctx, cc)
	if err != nil {
		l.m.setErr("mqtt 5: " + err.Error())
		return
	}
	l.cm = cm
}

func disconnectReason(d *paho.Disconnect) string {
	if d.Properties != nil {
		return d.Properties.ReasonString
	}
	return ""
}

// backoff: first attempt immediately, then 2s doubling up to 30s.
func backoff(attempt int) time.Duration {
	if attempt == 0 {
		return 0
	}
	d := 2 * time.Second << min(attempt-1, 4)
	return min(d, 30*time.Second)
}

func (l *v5link) subscribe(ctx context.Context, cm *autopaho.ConnectionManager, subs []config.Subscription) {
	opts := make([]paho.SubscribeOptions, len(subs))
	for i, s := range subs {
		opts[i] = paho.SubscribeOptions{Topic: s.Filter, QoS: s.QoS}
	}
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	sa, err := cm.Subscribe(sctx, &paho.Subscribe{Subscriptions: opts})
	if sa != nil {
		reason := ""
		if sa.Properties != nil {
			reason = sa.Properties.ReasonString
		}
		for i, code := range sa.Reasons {
			if code >= 0x80 && i < len(subs) {
				l.m.subscriptionRejected(subs[i].Filter, reasonText(code, reason))
			}
		}
		return
	}
	if err != nil && ctx.Err() == nil {
		l.m.setErr("subscribe: " + err.Error())
	}
}

func (l *v5link) onPublish(pr paho.PublishReceived) (bool, error) {
	p := pr.Packet
	topic := p.Topic
	var props *store.Props
	if pp := p.Properties; pp != nil {
		if pp.TopicAlias != nil {
			if l.aliases == nil || pr.Client != l.aliasClient {
				l.aliases, l.aliasClient = map[uint16]string{}, pr.Client
			}
			if topic != "" {
				l.aliases[*pp.TopicAlias] = topic
			} else {
				topic = l.aliases[*pp.TopicAlias]
			}
		}
		props = fromPahoProps(pp)
	}
	if topic == "" {
		l.m.setErr("received a publish with an unknown topic alias")
		return true, nil
	}
	l.m.deliver(&l.ctr, store.Entry{
		Topic:    topic,
		Payload:  p.Payload,
		QoS:      p.QoS,
		Retained: p.Retain,
		Time:     time.Now().UnixNano(),
		Props:    props,
	})
	return true, nil
}

func fromPahoProps(pp *paho.PublishProperties) *store.Props {
	p := &store.Props{
		ContentType:     pp.ContentType,
		ResponseTopic:   pp.ResponseTopic,
		CorrelationData: pp.CorrelationData,
	}
	if pp.MessageExpiry != nil {
		p.MessageExpiry = *pp.MessageExpiry
	}
	if pp.PayloadFormat != nil && *pp.PayloadFormat == 1 {
		p.PayloadUTF8 = true
	}
	for _, u := range pp.User {
		p.UserProperties = append(p.UserProperties, store.UserProperty{Key: u.Key, Value: u.Value})
	}
	if p.Empty() {
		return nil
	}
	return p
}

func toPahoProps(p *store.Props) *paho.PublishProperties {
	if p.Empty() {
		return nil
	}
	pp := &paho.PublishProperties{ContentType: p.ContentType, ResponseTopic: p.ResponseTopic, CorrelationData: p.CorrelationData}
	if p.MessageExpiry > 0 {
		v := p.MessageExpiry
		pp.MessageExpiry = &v
	}
	if p.PayloadUTF8 {
		one := byte(1)
		pp.PayloadFormat = &one
	}
	for _, u := range p.UserProperties {
		pp.User = append(pp.User, paho.UserProperty{Key: u.Key, Value: u.Value})
	}
	return pp
}

func (l *v5link) stop() {
	l.up.Store(false)
	if l.cm != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		_ = l.cm.Disconnect(ctx)
		cancel()
	}
	if l.cancel != nil {
		l.cancel()
	}
}

func (l *v5link) send(ctx context.Context, msg Message) func() error {
	cm := l.cm
	if cm == nil {
		return func() error { return ErrNotConnected }
	}
	done := make(chan error, 1)
	go func() {
		pr, err := cm.Publish(ctx, &paho.Publish{
			Topic: msg.Topic, QoS: msg.QoS, Retain: msg.Retain, Payload: msg.Payload, Properties: toPahoProps(msg.Props),
		})
		if err == nil && pr != nil && pr.ReasonCode >= 0x80 {
			reason := ""
			if pr.Properties != nil {
				reason = pr.Properties.ReasonString
			}
			err = errors.New("publish rejected by broker: " + reasonText(pr.ReasonCode, reason))
		}
		done <- err
	}()
	return func() error {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return fmt.Errorf("publish: %w", ctx.Err())
		}
	}
}

// describeV5Error renders CONNACK refusals with their reason code name and
// the broker's reason string.
func describeV5Error(err error) string {
	var ce *autopaho.ConnackError
	if errors.As(err, &ce) {
		return reasonText(ce.ReasonCode, ce.Reason)
	}
	return err.Error()
}

// MQTT 5 reason codes (the ones brokers commonly return).
var reasonNames = map[byte]string{
	0x80: "unspecified error", 0x81: "malformed packet", 0x82: "protocol error",
	0x83: "implementation specific error", 0x84: "unsupported protocol version",
	0x85: "client identifier not valid", 0x86: "bad user name or password", 0x87: "not authorized",
	0x88: "server unavailable", 0x89: "server busy", 0x8A: "banned", 0x8B: "server shutting down",
	0x8C: "bad authentication method", 0x8D: "keep alive timeout", 0x8E: "session taken over",
	0x8F: "topic filter invalid", 0x90: "topic name invalid", 0x93: "receive maximum exceeded",
	0x94: "topic alias invalid", 0x95: "packet too large", 0x96: "message rate too high",
	0x97: "quota exceeded", 0x98: "administrative action", 0x99: "payload format invalid",
	0x9A: "retain not supported", 0x9B: "QoS not supported", 0x9C: "use another server",
	0x9D: "server moved", 0x9E: "shared subscriptions not supported", 0x9F: "connection rate exceeded",
	0xA0: "maximum connect time", 0xA1: "subscription identifiers not supported",
	0xA2: "wildcard subscriptions not supported",
}

func reasonText(code byte, reason string) string {
	s := fmt.Sprintf("reason 0x%02X", code)
	if n, ok := reasonNames[code]; ok {
		s += " " + n
	}
	if reason != "" {
		s += ": " + reason
	}
	return s
}
