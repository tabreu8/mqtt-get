// Package mqttc manages the connection(s) to the MQTT broker: connecting with
// any supported authentication method, subscribing, reconnecting, feeding
// received messages to a handler and publishing.
package mqttc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// ErrNotConnected is returned when publishing without a live connection.
var ErrNotConnected = errors.New("not connected to MQTT broker")

// Handler receives every inbound message. It runs on the connection's
// receive goroutine and must not block.
type Handler func(e *store.Entry)

// Status describes the connection state.
type Status struct {
	Configured       bool      `json:"configured"`
	Connected        bool      `json:"connected"`
	Connections      int       `json:"connections"`
	ConnectedCount   int       `json:"connected_count"`
	URLs             []string  `json:"urls"`
	LastError        string    `json:"last_error,omitempty"`
	LastErrorAt      time.Time `json:"last_error_at,omitzero"`
	ConnectedSince   time.Time `json:"connected_since,omitzero"`
	MessagesReceived uint64    `json:"messages_received"`
	BytesReceived    uint64    `json:"bytes_received"`
	MessagesSent     uint64    `json:"messages_published"`
	PublishErrors    uint64    `json:"publish_errors"`
}

type conn struct {
	client    mqtt.Client
	connected atomic.Bool
}

// Manager owns the broker connections.
type Manager struct {
	log     *slog.Logger
	handler Handler

	mu    sync.Mutex
	cfg   config.Broker
	conns []*conn

	rr        atomic.Uint64
	received  atomic.Uint64
	bytes     atomic.Uint64
	sent      atomic.Uint64
	pubErrors atomic.Uint64

	errMu     sync.Mutex
	lastErr   string
	lastErrAt time.Time
	since     time.Time
}

// New creates a manager. Call Apply to connect.
func New(log *slog.Logger, h Handler) *Manager {
	return &Manager{log: log, handler: h}
}

// Config returns the active broker configuration.
func (m *Manager) Config() config.Broker {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg
}

// Apply (re)connects using cfg. It returns once connection attempts have
// started; connection happens in the background with automatic retries.
// Configuration errors (e.g. unreadable certificates) are returned.
func (m *Manager) Apply(cfg config.Broker) error {
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return err
	}
	var tlsCfg *tls.Config
	if cfg.IsConfigured() {
		var err error
		if tlsCfg, err = buildTLS(cfg); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnectLocked()
	m.cfg = cfg
	m.setErr("")
	if !cfg.IsConfigured() {
		return nil
	}
	m.conns = make([]*conn, cfg.Connections)
	for i := range m.conns {
		c := &conn{}
		c.client = mqtt.NewClient(m.options(cfg, tlsCfg, i, c))
		m.conns[i] = c
		c.client.Connect() // retries in the background (ConnectRetry)
	}
	m.log.Info("mqtt: connecting", "urls", cfg.URLs, "connections", cfg.Connections, "client_id", cfg.ClientID)
	return nil
}

// Reconnect drops and re-establishes the connections with the current config.
func (m *Manager) Reconnect() error { return m.Apply(m.Config()) }

// Close disconnects from the broker.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnectLocked()
}

func (m *Manager) disconnectLocked() {
	for _, c := range m.conns {
		c.client.Disconnect(250)
		c.connected.Store(false)
	}
	m.conns = nil
}

func (m *Manager) options(cfg config.Broker, tlsCfg *tls.Config, idx int, c *conn) *mqtt.ClientOptions {
	o := mqtt.NewClientOptions()
	for _, u := range cfg.URLs {
		o.AddBroker(u)
	}
	clientID := cfg.ClientID
	if cfg.Connections > 1 {
		clientID += "-" + strconv.Itoa(idx)
	}
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
	o.SetCustomOpenConnectionFn(openConn)
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
	if cfg.Username != "" || cfg.Password != "" || cfg.PasswordFile != "" {
		username, password, passwordFile := cfg.Username, cfg.Password, cfg.PasswordFile
		// Evaluated on every (re)connect so rotated tokens in files are picked up.
		o.SetCredentialsProvider(func() (string, string) {
			if passwordFile != "" {
				if b, err := os.ReadFile(passwordFile); err == nil {
					return username, strings.TrimRight(string(b), "\r\n")
				} else {
					m.log.Error("mqtt: reading password file", "err", err)
				}
			}
			return username, password
		})
	}

	subscribe := idx == 0 || cfg.SharedGroup != ""
	filters := make(map[string]byte, len(cfg.Subscriptions))
	for _, s := range cfg.Subscriptions {
		f := s.Filter
		if cfg.SharedGroup != "" {
			f = "$share/" + cfg.SharedGroup + "/" + f
		}
		filters[f] = s.QoS
	}

	o.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) { m.onMessage(msg) })
	o.SetOnConnectHandler(func(cl mqtt.Client) {
		c.connected.Store(true)
		m.errMu.Lock()
		if m.since.IsZero() {
			m.since = time.Now()
		}
		m.lastErr = ""
		m.errMu.Unlock()
		m.log.Info("mqtt: connected", "client_id", clientID)
		if subscribe && len(filters) > 0 {
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
						if code == 0x80 {
							m.setErr("subscription rejected by broker: " + f)
						}
					}
				}
			}()
		}
	})
	o.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.connected.Store(false)
		m.errMu.Lock()
		m.since = time.Time{}
		m.errMu.Unlock()
		m.setErr("connection lost: " + err.Error())
	})
	o.SetConnectionNotificationHandler(func(_ mqtt.Client, n mqtt.ConnectionNotification) {
		if f, ok := n.(mqtt.ConnectionNotificationFailed); ok {
			m.setErr("connect failed: " + f.Reason.Error())
		}
		if f, ok := n.(mqtt.ConnectionNotificationBrokerFailed); ok {
			m.setErr("connect to " + f.Broker.Redacted() + " failed: " + f.Reason.Error())
		}
	})
	return o
}

func (m *Manager) setErr(s string) {
	m.errMu.Lock()
	changed := s != m.lastErr
	m.lastErr = s
	if s != "" {
		m.lastErrAt = time.Now()
	}
	m.errMu.Unlock()
	if s != "" && changed {
		m.log.Warn("mqtt: " + s)
	}
}

func (m *Manager) onMessage(msg mqtt.Message) {
	p := msg.Payload()
	m.received.Add(1)
	m.bytes.Add(uint64(len(p)))
	m.handler(&store.Entry{
		Topic:    msg.Topic(),
		Payload:  p,
		QoS:      msg.Qos(),
		Retained: msg.Retained(),
		Time:     time.Now().UnixNano(),
	})
}

// Publish sends a message, spreading load round-robin over connected
// connections. For QoS > 0 it waits for the broker acknowledgement.
func (m *Manager) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	c := m.pick()
	if c == nil {
		m.pubErrors.Add(1)
		return ErrNotConnected
	}
	tok := c.client.Publish(topic, qos, retain, payload)
	select {
	case <-tok.Done():
	case <-ctx.Done():
		m.pubErrors.Add(1)
		return fmt.Errorf("publish: %w", ctx.Err())
	}
	if err := tok.Error(); err != nil {
		m.pubErrors.Add(1)
		return err
	}
	m.sent.Add(1)
	return nil
}

func (m *Manager) pick() *conn {
	m.mu.Lock()
	conns := m.conns
	m.mu.Unlock()
	n := len(conns)
	if n == 0 {
		return nil
	}
	start := int(m.rr.Add(1) % uint64(n))
	for i := 0; i < n; i++ {
		c := conns[(start+i)%n]
		if c.connected.Load() {
			return c
		}
	}
	return nil
}

// Status returns the current connection status and counters.
func (m *Manager) Status() Status {
	m.mu.Lock()
	cfg := m.cfg
	connected := 0
	for _, c := range m.conns {
		if c.connected.Load() {
			connected++
		}
	}
	total := len(m.conns)
	m.mu.Unlock()
	m.errMu.Lock()
	defer m.errMu.Unlock()
	urls := make([]string, len(cfg.URLs))
	for i, u := range cfg.URLs {
		urls[i] = redactURL(u)
	}
	st := Status{
		Configured:       cfg.IsConfigured(),
		Connected:        connected > 0,
		Connections:      total,
		ConnectedCount:   connected,
		URLs:             urls,
		LastError:        m.lastErr,
		MessagesReceived: m.received.Load(),
		BytesReceived:    m.bytes.Load(),
		MessagesSent:     m.sent.Load(),
		PublishErrors:    m.pubErrors.Load(),
	}
	if m.lastErr != "" {
		st.LastErrorAt = m.lastErrAt
	}
	if connected > 0 {
		st.ConnectedSince = m.since
	}
	return st
}

func redactURL(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	return u.Redacted()
}

func buildTLS(cfg config.Broker) (*tls.Config, error) {
	t := cfg.TLS
	secureScheme := false
	for _, u := range cfg.URLs {
		if pu, err := url.Parse(u); err == nil {
			switch pu.Scheme {
			case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps", "wss":
				secureScheme = true
			}
		}
	}
	if !secureScheme && !t.Enabled && t.CAFile == "" && t.CAPEM == "" && t.CertFile == "" &&
		t.CertPEM == "" && !t.InsecureSkipVerify && t.ServerName == "" && len(t.ALPN) == 0 {
		return nil, nil
	}
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // explicit user opt-in
		NextProtos:         t.ALPN,
	}
	caPEM := []byte(t.CAPEM)
	if len(caPEM) == 0 && t.CAFile != "" {
		b, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read CA file: %w", err)
		}
		caPEM = b
	}
	if len(caPEM) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("tls: no valid certificates in CA")
		}
		tc.RootCAs = pool
	}
	certPEM, keyPEM := []byte(t.CertPEM), []byte(t.KeyPEM)
	if len(certPEM) == 0 && t.CertFile != "" {
		b, err := os.ReadFile(t.CertFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read client certificate: %w", err)
		}
		certPEM = b
	}
	if len(keyPEM) == 0 && t.KeyFile != "" {
		b, err := os.ReadFile(t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read client key: %w", err)
		}
		keyPEM = b
	}
	if len(certPEM) > 0 {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("tls: client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

// Message is one message to publish.
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

// PublishMany sends all messages, pipelining them before waiting for
// acknowledgements. It returns how many were published successfully and the
// first error.
func (m *Manager) PublishMany(ctx context.Context, msgs []Message) (int, error) {
	toks := make([]mqtt.Token, 0, len(msgs))
	for _, msg := range msgs {
		c := m.pick()
		if c == nil {
			m.pubErrors.Add(uint64(len(msgs) - len(toks)))
			break
		}
		toks = append(toks, c.client.Publish(msg.Topic, msg.QoS, msg.Retain, msg.Payload))
	}
	ok := 0
	var firstErr error
	for _, tok := range toks {
		select {
		case <-tok.Done():
			if err := tok.Error(); err != nil {
				m.pubErrors.Add(1)
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			ok++
			m.sent.Add(1)
		case <-ctx.Done():
			m.pubErrors.Add(1)
			if firstErr == nil {
				firstErr = fmt.Errorf("publish: %w", ctx.Err())
			}
		}
	}
	if firstErr == nil && ok < len(msgs) {
		firstErr = ErrNotConnected
	}
	return ok, firstErr
}
