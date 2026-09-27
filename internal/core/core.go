// Package core is mqtt-get's foundation: the MQTT connection, latest-value
// store, webhooks, watchers and persisted settings, exposed as one set of
// operations. The REST API (internal/httpapi, for systems) and the MCP server
// (internal/mcp, for agents) are thin interfaces over this package, so both
// always behave the same way.
package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
	"github.com/tabreu8/mqtt-get/internal/topic"
	"github.com/tabreu8/mqtt-get/internal/webhook"
)

// Version is set at build time.
var Version = "dev"

// Service is the application core.
type Service struct {
	cfg     config.Server
	log     *slog.Logger
	state   *state.State
	store   *store.Store
	mqtt    *mqttc.Manager
	hooks   *webhook.Dispatcher
	watch   *watchers
	started time.Time
	// brokerSource tells where the active broker config came from
	// ("saved", "env" or "none").
	brokerSource atomic.Value
}

// New creates the service. Call Start to connect.
func New(cfg config.Server, log *slog.Logger, st *state.State) *Service {
	s := &Service{
		cfg:     cfg,
		log:     log,
		state:   st,
		store:   store.New(cfg.MaxTopics),
		hooks:   webhook.New(log),
		watch:   newWatchers(),
		started: time.Now(),
	}
	s.mqtt = mqttc.New(log, s.Ingest)
	return s
}

// Ingest is the hot path for every inbound MQTT message. It is exported so
// tests and benchmarks can inject messages without a broker.
//
// The message is passed by value and only copied to the heap when a webhook
// or watcher matches it, so plain ingest allocates nothing here (and the
// store updates existing topics in place).
func (s *Service) Ingest(e store.Entry) {
	s.store.Set(&e)
	s.hooks.Dispatch(e)
	s.watch.dispatch(e)
}

// Start loads webhooks and connects to the broker. Broker configuration
// saved through the API takes precedence over MQTT_* environment variables.
func (s *Service) Start(envBroker config.Broker, envOK bool) error {
	s.hooks.Set(s.state.Webhooks())
	b, ok := s.state.Broker()
	switch {
	case ok:
		s.brokerSource.Store("saved")
	case envOK:
		b = envBroker
		s.brokerSource.Store("env")
	default:
		s.brokerSource.Store("none")
		s.log.Warn("no MQTT broker configured yet: set MQTT_URL or configure it in the web UI, REST API or MCP")
		return nil
	}
	return s.mqtt.Apply(b)
}

// Close shuts everything down.
func (s *Service) Close() {
	s.mqtt.Close()
	s.hooks.Close()
}

// Config returns the process settings.
func (s *Service) Config() config.Server { return s.cfg }

// Store exposes the value store (metrics, tests).
func (s *Service) Store() *store.Store { return s.store }

// MQTTStatus returns the broker connection status.
func (s *Service) MQTTStatus() mqttc.Status { return s.mqtt.Status() }

// WebhookStats returns delivery counters for every webhook.
func (s *Service) WebhookStats() map[string]webhook.Stats { return s.hooks.AllStats() }

// --- broker ---

func (s *Service) source() string {
	v, _ := s.brokerSource.Load().(string)
	if v == "" {
		return "none"
	}
	return v
}

// BrokerView is the broker configuration (secrets redacted) and status.
type BrokerView struct {
	Config config.Broker `json:"config"`
	Source string        `json:"source"`
	Status mqttc.Status  `json:"status"`
}

// Broker returns the redacted broker configuration and connection status.
func (s *Service) Broker() BrokerView {
	return BrokerView{Config: s.mqtt.Config().Redact(), Source: s.source(), Status: s.mqtt.Status()}
}

// BrokerConfig returns the active configuration including secrets. Use it
// only to build a modified configuration for SetBroker.
func (s *Service) BrokerConfig() config.Broker { return s.mqtt.Config() }

// SetBroker validates, applies (reconnects) and persists a broker
// configuration. Fields holding config.Redacted keep their stored secret.
func (s *Service) SetBroker(b config.Broker) error {
	b.MergeSecrets(s.mqtt.Config())
	b.Normalize()
	if err := b.Validate(); err != nil {
		return invalid(err)
	}
	if err := s.mqtt.Apply(b); err != nil {
		return invalid(err)
	}
	s.brokerSource.Store("saved")
	return s.state.SetBroker(b)
}

// Reconnect drops and re-establishes the broker connections.
func (s *Service) Reconnect() error {
	if err := s.mqtt.Reconnect(); err != nil {
		return invalid(err)
	}
	return nil
}

// --- values ---

// Latest returns the most recent message on an exact topic.
func (s *Service) Latest(name string) (*store.Entry, error) {
	if err := topic.ValidateName(name); err != nil {
		return nil, invalid(fmt.Errorf("topic %q: %w", name, err))
	}
	e, ok := s.store.Get(name)
	if !ok {
		return nil, notFound("no value received yet for topic %q", name)
	}
	return e, nil
}

// Query returns the latest values of topics matching filter ("" = all),
// sorted by topic, plus the total number of matches.
func (s *Service) Query(filter string, limit int) ([]*store.Entry, int, error) {
	if filter == "" {
		filter = "#"
	}
	if err := topic.ValidateFilter(filter); err != nil {
		return nil, 0, invalid(fmt.Errorf("filter %q: %w", filter, err))
	}
	if limit <= 0 {
		limit = 1000
	}
	if limit > 100000 {
		limit = 100000
	}
	res, total := s.store.Query(filter, limit)
	return res, total, nil
}

// Forget removes one topic ("" = all topics) from the store.
func (s *Service) Forget(name string) error {
	if name == "" {
		s.store.Clear()
		return nil
	}
	if !s.store.Delete(name) {
		return notFound("topic %q not found", name)
	}
	return nil
}

// --- publish ---

// PublishRequest is one message to publish, as received from a client.
type PublishRequest struct {
	Topic string `json:"topic"`
	// Payload: a JSON string is sent as its text; any other JSON value is
	// sent as its JSON encoding.
	Payload  json.RawMessage `json:"payload"`
	Encoding string          `json:"encoding,omitempty"` // "", "text" or "base64"
	QoS      byte            `json:"qos"`
	Retain   bool            `json:"retain"`
}

// Message validates the request and converts it to an MQTT message.
func (p PublishRequest) Message() (mqttc.Message, error) {
	if err := topic.ValidateName(p.Topic); err != nil {
		return mqttc.Message{}, invalid(fmt.Errorf("topic %q: %w", p.Topic, err))
	}
	if p.QoS > 2 {
		return mqttc.Message{}, invalid(errors.New("qos must be 0, 1 or 2"))
	}
	switch p.Encoding {
	case "", "text", "base64":
	default:
		return mqttc.Message{}, invalid(errors.New(`encoding must be "text" or "base64"`))
	}
	var payload []byte
	raw := strings.TrimSpace(string(p.Payload))
	switch {
	case raw == "" || raw == "null":
	case raw[0] == '"':
		var str string
		if err := json.Unmarshal(p.Payload, &str); err != nil {
			return mqttc.Message{}, invalid(err)
		}
		payload = []byte(str)
		if p.Encoding == "base64" {
			b, err := base64.StdEncoding.DecodeString(str)
			if err != nil {
				return mqttc.Message{}, invalid(fmt.Errorf("payload: invalid base64: %w", err))
			}
			payload = b
		}
	default:
		payload = []byte(raw)
	}
	return mqttc.Message{Topic: p.Topic, Payload: payload, QoS: p.QoS, Retain: p.Retain}, nil
}

// Publish sends messages, waiting for broker acknowledgements (QoS > 0).
func (s *Service) Publish(ctx context.Context, msgs []mqttc.Message) (int, error) {
	if len(msgs) == 0 {
		return 0, invalid(errors.New("no messages to publish"))
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
	defer cancel()
	n, err := s.mqtt.PublishMany(ctx, msgs)
	if errors.Is(err, mqttc.ErrNotConnected) {
		return n, &Error{Kind: Unavailable, Msg: err.Error()}
	}
	if err != nil {
		return n, &Error{Kind: Upstream, Msg: err.Error()}
	}
	return n, nil
}

// --- webhooks ---

// WebhookView is a webhook (secrets redacted) with delivery stats.
type WebhookView struct {
	config.Webhook
	Stats webhook.Stats `json:"stats"`
}

// Webhooks lists all webhooks.
func (s *Service) Webhooks() []WebhookView {
	stats := s.hooks.AllStats()
	list := s.state.Webhooks()
	out := make([]WebhookView, 0, len(list))
	for _, w := range list {
		out = append(out, WebhookView{Webhook: w.Redact(), Stats: stats[w.ID]})
	}
	return out
}

// Webhook returns one webhook.
func (s *Service) Webhook(id string) (WebhookView, error) {
	w, err := s.state.Webhook(id)
	if err != nil {
		return WebhookView{}, notFound("webhook %q not found", id)
	}
	st, _ := s.hooks.Stats(id)
	return WebhookView{Webhook: w.Redact(), Stats: st}, nil
}

// WebhookConfig returns a webhook including secrets, to build a partial update.
func (s *Service) WebhookConfig(id string) (config.Webhook, error) {
	w, err := s.state.Webhook(id)
	if err != nil {
		return config.Webhook{}, notFound("webhook %q not found", id)
	}
	return w, nil
}

// PutWebhook creates (create=true) or replaces a webhook.
func (s *Service) PutWebhook(w config.Webhook, create bool) (WebhookView, error) {
	old, err := s.state.Webhook(w.ID)
	exists := err == nil
	switch {
	case create && w.ID == "":
		w.ID = state.NewID()
	case create && exists:
		return WebhookView{}, &Error{Kind: Conflict, Msg: fmt.Sprintf("webhook id %q already exists", w.ID)}
	case !create && !exists:
		return WebhookView{}, notFound("webhook %q not found", w.ID)
	}
	if exists {
		w.MergeSecrets(old)
	}
	w.Normalize()
	if err := w.Validate(); err != nil {
		return WebhookView{}, invalid(err)
	}
	if err := s.state.PutWebhook(w); err != nil {
		return WebhookView{}, err
	}
	s.hooks.Put(w)
	return s.Webhook(w.ID)
}

// DeleteWebhook removes a webhook.
func (s *Service) DeleteWebhook(id string) error {
	if err := s.state.DeleteWebhook(id); err != nil {
		return notFound("webhook %q not found", id)
	}
	s.hooks.Remove(id)
	return nil
}

// TestWebhook sends one synthetic (or the latest real) message to a webhook.
func (s *Service) TestWebhook(id, topicName string, payload []byte) error {
	w, err := s.state.Webhook(id)
	if err != nil {
		return notFound("webhook %q not found", id)
	}
	e := &store.Entry{Topic: topicName, Payload: payload, Time: time.Now().UnixNano()}
	if e.Topic == "" {
		e.Topic = "mqtt-get/test"
		for _, f := range w.Topics {
			if !topic.HasWildcard(f) {
				e.Topic = f
				if latest, ok := s.store.Get(f); ok && payload == nil {
					e = latest
				}
				break
			}
		}
	}
	if e.Payload == nil {
		e.Payload = []byte(`{"test":true}`)
	}
	if err := s.hooks.Test(w, e); err != nil {
		return &Error{Kind: Upstream, Msg: "webhook test failed: " + err.Error()}
	}
	return nil
}

// --- API keys ---

// Principal is an authenticated caller.
type Principal struct {
	ID     string   `json:"id,omitempty"`
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

// Can reports whether the principal holds scope (admin implies all).
func (p *Principal) Can(scope string) bool {
	if p == nil {
		return false
	}
	for _, s := range p.Scopes {
		if s == scope || s == config.ScopeAdmin {
			return true
		}
	}
	return false
}

// Anonymous is used when authentication is disabled.
var Anonymous = &Principal{Name: "anonymous (auth disabled)", Scopes: []string{config.ScopeAdmin}}

// Authenticate resolves an API key. With AUTH_DISABLED every caller is admin.
func (s *Service) Authenticate(key string) (*Principal, error) {
	if s.cfg.AuthDisabled {
		return Anonymous, nil
	}
	k, ok := s.state.Lookup(key)
	if !ok {
		return nil, &Error{Kind: Unauthenticated, Msg: "missing or invalid API key"}
	}
	return &Principal{ID: k.ID, Name: k.Name, Scopes: k.Scopes}, nil
}

// Require returns a Forbidden error unless p holds scope.
func Require(p *Principal, scope string) error {
	if !p.Can(scope) {
		return &Error{Kind: Forbidden, Msg: "API key lacks the " + scope + " scope"}
	}
	return nil
}

// Keys lists API keys (never secrets or hashes).
func (s *Service) Keys() []config.APIKey { return s.state.Keys() }

// CreateKey creates a key; the secret is returned only here.
func (s *Service) CreateKey(name string, scopes []string) (config.APIKey, string, error) {
	k, secret, err := s.state.CreateKey(name, scopes)
	if err != nil {
		return k, "", invalid(err)
	}
	return k, secret, nil
}

// DeleteKey revokes a persisted key. A caller cannot revoke its own key.
func (s *Service) DeleteKey(caller *Principal, id string) error {
	if caller != nil && caller.ID == id {
		return invalid(errors.New("refusing to revoke the key used for this request"))
	}
	if err := s.state.DeleteKey(id); err != nil {
		return notFound("key %q not found (environment keys cannot be revoked)", id)
	}
	return nil
}

// --- status ---

// StatusView summarizes the service.
type StatusView struct {
	Version       string       `json:"version"`
	UptimeSec     int64        `json:"uptime_sec"`
	MQTT          mqttc.Status `json:"mqtt"`
	Topics        int          `json:"topics"`
	TopicsDropped uint64       `json:"topics_dropped"`
	MaxTopics     int          `json:"max_topics"`
	Webhooks      int          `json:"webhooks"`
	Watchers      int          `json:"active_watchers"`
	BrokerSource  string       `json:"broker_source"`
	StateFile     string       `json:"state_file,omitempty"`
}

// Status returns the service status.
func (s *Service) Status() StatusView {
	return StatusView{
		Version:       Version,
		UptimeSec:     int64(time.Since(s.started).Seconds()),
		MQTT:          s.mqtt.Status(),
		Topics:        s.store.Len(),
		TopicsDropped: s.store.Dropped(),
		MaxTopics:     s.cfg.MaxTopics,
		Webhooks:      len(s.state.Webhooks()),
		Watchers:      s.watch.count(),
		BrokerSource:  s.source(),
		StateFile:     s.state.Path(),
	}
}
