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
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// ErrNotConnected is returned when publishing without a live connection.
var ErrNotConnected = errors.New("not connected to MQTT broker")

// Handler receives every inbound message. It runs on the connection's
// receive goroutine and must not block.
type Handler func(e store.Entry)

// Status describes the connection state.
type Status struct {
	Configured       bool      `json:"configured"`
	Connected        bool      `json:"connected"`
	Protocol         string    `json:"protocol,omitempty"`
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
	// The most recent connection loss and why (e.g. the broker's MQTT 5
	// reason). Unlike LastError it is kept after reconnecting.
	LastDisconnect   string    `json:"last_disconnect,omitempty"`
	LastDisconnectAt time.Time `json:"last_disconnect_at,omitzero"`
	Disconnects      uint64    `json:"disconnects"`
	// ReceivedPerConnection shows how ingest is spread over connections.
	ReceivedPerConnection []uint64 `json:"received_per_connection,omitempty"`
	// FiltersPerConnection lists what each connection subscribes to.
	FiltersPerConnection [][]string `json:"filters_per_connection,omitempty"`
}

// link is one broker connection. Implementations: leanlink (the default,
// MQTT 3.1/3.1.1/5), and with client "paho" v3link (MQTT 3.1/3.1.1) and
// v5link (MQTT 5).
type link interface {
	start()
	stop()
	isUp() bool
	stats() *counters
	// send starts publishing msg and returns a function that waits for the
	// result (broker acknowledgement for QoS > 0). Starting all sends before
	// waiting pipelines a batch.
	send(ctx context.Context, msg Message) func() error
}

// Manager owns the broker connections.
type Manager struct {
	log     *slog.Logger
	handler Handler

	mu    sync.Mutex
	cfg   config.Broker
	links []link

	rr        atomic.Uint64
	retired   counters // totals of links replaced by Apply/Reconnect
	sent      atomic.Uint64
	pubErrors atomic.Uint64

	errMu     sync.Mutex
	lastErr   string
	lastErrAt time.Time
	since     time.Time
	lastDown  string
	downAt    time.Time
	downs     uint64
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
	m.links = make([]link, cfg.Connections)
	for i := range m.links {
		switch {
		case cfg.Client == config.ClientPaho && cfg.ProtocolVersion == 5:
			m.links[i] = newV5(m, cfg, tlsCfg, i)
		case cfg.Client == config.ClientPaho:
			m.links[i] = newV3(m, cfg, tlsCfg, i)
		default:
			m.links[i] = newLean(m, cfg, tlsCfg, i)
		}
		m.links[i].start()
	}
	m.log.Info("mqtt: connecting", "urls", cfg.URLs, "protocol", protocolName(cfg.ProtocolVersion), "client", clientName(cfg),
		"connections", cfg.Connections, "client_id", cfg.ClientID,
		"subscription_mode", cfg.SubscriptionMode, "shared_group", cfg.SharedGroup)
	if cfg.SubscriptionMode == config.SubscriptionSplit && cfg.Connections > len(cfg.Subscriptions) {
		m.log.Warn("mqtt: split mode has more connections than filters; the extra connections only publish",
			"connections", cfg.Connections, "filters", len(cfg.Subscriptions))
	}
	return nil
}

func clientName(cfg config.Broker) string {
	switch {
	case cfg.Client != config.ClientPaho:
		return "lean"
	case cfg.ProtocolVersion == 5:
		return "paho.golang"
	}
	return "paho"
}

func protocolName(v uint) string {
	switch v {
	case 3:
		return "MQTT 3.1"
	case 5:
		return "MQTT 5"
	}
	return "MQTT 3.1.1"
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
	if w := m.cfg.Will; w != nil && w.OnlinePayload != "" && len(m.links) > 0 && m.links[0].isUp() {
		// Status topic: say we are going offline (the broker only publishes
		// the will after an unexpected disconnect).
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := m.links[0].send(ctx, willMessage(w, w.Payload))(); err != nil {
			m.log.Warn("mqtt: publishing the offline status failed", "err", err)
		}
		cancel()
	}
	for _, l := range m.links {
		l.stop()
		c := l.stats()
		m.retired.recv.Add(c.recv.Load())
		m.retired.bytes.Add(c.bytes.Load())
	}
	m.links = nil
}

func willMessage(w *config.Will, payload string) Message {
	return Message{Topic: w.Topic, Payload: []byte(payload), QoS: w.QoS, Retain: w.Retain}
}

// announceOnline publishes the will's online payload on the first
// connection after it connects (status topics), after delay.
func (m *Manager) announceOnline(cfg config.Broker, idx int, l link, delay time.Duration) {
	w := cfg.Will
	if w == nil || w.OnlinePayload == "" || idx != 0 {
		return
	}
	go func() {
		time.Sleep(delay)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := l.send(ctx, willMessage(w, w.OnlinePayload))(); err != nil {
			m.setErr("publishing the online status to " + w.Topic + " failed: " + err.Error())
		}
	}()
}

func clientIDFor(cfg config.Broker, idx int) string {
	if cfg.Connections > 1 {
		return cfg.ClientID + "-" + strconv.Itoa(idx)
	}
	return cfg.ClientID
}

// credentials returns the username and the current password; the password
// file is re-read on every (re)connect so rotated tokens are picked up.
func (m *Manager) credentials(cfg config.Broker) (string, string) {
	if cfg.PasswordFile != "" {
		b, err := os.ReadFile(cfg.PasswordFile)
		if err == nil {
			return cfg.Username, strings.TrimRight(string(b), "\r\n")
		}
		m.log.Error("mqtt: reading password file", "err", err)
	}
	return cfg.Username, cfg.Password
}

func hasCredentials(cfg config.Broker) bool {
	return cfg.Username != "" || cfg.Password != "" || cfg.PasswordFile != ""
}

// --- callbacks shared by all link implementations ---

func (m *Manager) linkUp(clientID string) {
	m.errMu.Lock()
	if m.since.IsZero() {
		m.since = time.Now()
	}
	m.lastErr = ""
	m.errMu.Unlock()
	m.log.Info("mqtt: connected", "client_id", clientID)
}

func (m *Manager) linkDown(reason string) {
	m.errMu.Lock()
	m.since = time.Time{}
	m.lastDown, m.downAt = reason, time.Now()
	m.downs++
	m.errMu.Unlock()
	m.setErr("connection lost: " + reason)
}

func (m *Manager) subscriptionRejected(filter, detail string) {
	msg := "subscription rejected by broker (check ACLs): " + filter
	if strings.HasPrefix(filter, "$share/") {
		msg = "shared subscription rejected by broker: " + filter +
			" (the broker may not support $share: use subscription_mode \"split\" / MQTT_SUBSCRIPTION_MODE=split instead of shared_group)"
	}
	if detail != "" {
		msg += " [" + detail + "]"
	}
	m.setErr(msg)
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

// counters are per connection (on their own cache line), so connections on
// different cores never contend on a shared counter; Status sums them.
type counters struct {
	recv  atomic.Uint64
	bytes atomic.Uint64
	_     [48]byte
}

func (m *Manager) deliver(c *counters, e store.Entry) {
	c.recv.Add(1)
	c.bytes.Add(uint64(len(e.Payload)))
	m.handler(e)
}

// Publish sends one message and waits for the result.
func (m *Manager) Publish(ctx context.Context, topic string, payload []byte, qos byte, retain bool) error {
	_, err := m.PublishMany(ctx, []Message{{Topic: topic, Payload: payload, QoS: qos, Retain: retain}})
	return err
}

func (m *Manager) pick() link {
	m.mu.Lock()
	links := m.links
	m.mu.Unlock()
	n := len(links)
	if n == 0 {
		return nil
	}
	start := int(m.rr.Add(1) % uint64(n))
	for i := 0; i < n; i++ {
		if l := links[(start+i)%n]; l.isUp() {
			return l
		}
	}
	return nil
}

// Status returns the current connection status and counters.
func (m *Manager) Status() Status {
	m.mu.Lock()
	cfg := m.cfg
	connected := 0
	var perConn []uint64
	received, bytes := m.retired.recv.Load(), m.retired.bytes.Load()
	for _, l := range m.links {
		if l.isUp() {
			connected++
		}
		c := l.stats()
		perConn = append(perConn, c.recv.Load())
		received += c.recv.Load()
		bytes += c.bytes.Load()
	}
	total := len(m.links)
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
		Protocol:         protocolName(cfg.ProtocolVersion),
		Connections:      total,
		ConnectedCount:   connected,
		URLs:             urls,
		LastError:        m.lastErr,
		MessagesReceived: received,
		BytesReceived:    bytes,
		MessagesSent:     m.sent.Load(),
		PublishErrors:    m.pubErrors.Load(),
		LastDisconnect:   m.lastDown,
		LastDisconnectAt: m.downAt,
		Disconnects:      m.downs,
	}
	if !cfg.IsConfigured() {
		st.Protocol = ""
	}
	if len(perConn) > 1 {
		st.ReceivedPerConnection = perConn
		for i := range perConn {
			var fs []string
			for _, sub := range cfg.SubscriptionsFor(i) {
				fs = append(fs, sub.Filter)
			}
			st.FiltersPerConnection = append(st.FiltersPerConnection, fs)
		}
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
	Props   *store.Props // MQTT 5 only
}

// ErrPropertiesNeedV5 is returned when publishing properties over MQTT 3.
var ErrPropertiesNeedV5 = errors.New("message properties need an MQTT 5 connection (protocol_version 5)")

// PublishMany sends all messages, pipelining them before waiting for
// acknowledgements. It returns how many were published successfully and the
// first error.
func (m *Manager) PublishMany(ctx context.Context, msgs []Message) (int, error) {
	waits := make([]func() error, 0, len(msgs))
	for _, msg := range msgs {
		l := m.pick()
		if l == nil {
			m.pubErrors.Add(uint64(len(msgs) - len(waits)))
			break
		}
		waits = append(waits, l.send(ctx, msg))
	}
	ok := 0
	var firstErr error
	for _, wait := range waits {
		if err := wait(); err != nil {
			m.pubErrors.Add(1)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		ok++
		m.sent.Add(1)
	}
	if firstErr == nil && ok < len(msgs) {
		firstErr = ErrNotConnected
	}
	return ok, firstErr
}
