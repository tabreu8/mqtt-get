// Package api exposes the REST API, MCP endpoint, metrics and web UI.
package api

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

// Server is the application: it ties the MQTT connection, value store,
// webhooks and persisted state together and serves HTTP.
type Server struct {
	cfg     config.Server
	log     *slog.Logger
	state   *state.State
	store   *store.Store
	mqtt    *mqttc.Manager
	hooks   *webhook.Dispatcher
	started time.Time
	// brokerSource tells where the active broker config came from
	// ("saved", "env" or "none").
	brokerSource atomic.Value
}

// New creates the server. Call Start to connect.
func New(cfg config.Server, log *slog.Logger, st *state.State) *Server {
	s := &Server{
		cfg:     cfg,
		log:     log,
		state:   st,
		store:   store.New(cfg.MaxTopics),
		hooks:   webhook.New(log),
		started: time.Now(),
	}
	s.mqtt = mqttc.New(log, s.ingest)
	return s
}

// ingest is the hot path for every inbound MQTT message.
func (s *Server) ingest(e *store.Entry) {
	s.store.Set(e)
	s.hooks.Dispatch(e)
}

// MQTTStatus returns the broker connection status.
func (s *Server) MQTTStatus() mqttc.Status { return s.mqtt.Status() }

// Store exposes the value store (tests).
func (s *Server) Store() *store.Store { return s.store }

// Start loads webhooks and connects to the broker. Broker configuration
// saved through the API takes precedence over MQTT_* environment variables.
func (s *Server) Start(envBroker config.Broker, envOK bool) error {
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
		s.log.Warn("no MQTT broker configured yet: set MQTT_URL or configure it in the web UI / API")
		return nil
	}
	return s.mqtt.Apply(b)
}

// Close shuts everything down.
func (s *Server) Close() {
	s.mqtt.Close()
	s.hooks.Close()
}

// --- broker ---

func (s *Server) source() string {
	v, _ := s.brokerSource.Load().(string)
	if v == "" {
		return "none"
	}
	return v
}

type brokerView struct {
	Config config.Broker `json:"config"`
	Source string        `json:"source"`
	Status mqttc.Status  `json:"status"`
}

func (s *Server) brokerInfo() brokerView {
	return brokerView{Config: s.mqtt.Config().Redact(), Source: s.source(), Status: s.mqtt.Status()}
}

func (s *Server) setBroker(b config.Broker) error {
	b.MergeSecrets(s.mqtt.Config())
	b.Normalize()
	if err := b.Validate(); err != nil {
		return badRequest(err)
	}
	if err := s.mqtt.Apply(b); err != nil {
		return badRequest(err)
	}
	s.brokerSource.Store("saved")
	return s.state.SetBroker(b)
}

// --- values ---

func (s *Server) latest(name string) (*store.Entry, error) {
	if err := topic.ValidateName(name); err != nil {
		return nil, badRequest(err)
	}
	e, ok := s.store.Get(name)
	if !ok {
		return nil, notFound("no value received for topic %q", name)
	}
	return e, nil
}

func (s *Server) query(filter string, limit int) ([]*store.Entry, int, error) {
	if filter == "" {
		filter = "#"
	}
	if err := topic.ValidateFilter(filter); err != nil {
		return nil, 0, badRequest(err)
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

// --- publish ---

// publishRequest is the JSON body for publishing.
type publishRequest struct {
	Topic string `json:"topic"`
	// Payload: a JSON string is sent as its text; any other JSON value is
	// sent as its JSON encoding.
	Payload  json.RawMessage `json:"payload"`
	Encoding string          `json:"encoding,omitempty"` // "" or "base64"
	QoS      byte            `json:"qos"`
	Retain   bool            `json:"retain"`
}

func (p publishRequest) toMessage() (mqttc.Message, error) {
	if err := topic.ValidateName(p.Topic); err != nil {
		return mqttc.Message{}, badRequest(fmt.Errorf("topic: %w", err))
	}
	if p.QoS > 2 {
		return mqttc.Message{}, badRequest(errors.New("qos must be 0, 1 or 2"))
	}
	var payload []byte
	raw := strings.TrimSpace(string(p.Payload))
	switch {
	case raw == "" || raw == "null":
	case raw[0] == '"':
		var str string
		if err := json.Unmarshal(p.Payload, &str); err != nil {
			return mqttc.Message{}, badRequest(err)
		}
		payload = []byte(str)
		if p.Encoding == "base64" {
			b, err := base64.StdEncoding.DecodeString(str)
			if err != nil {
				return mqttc.Message{}, badRequest(fmt.Errorf("payload: invalid base64: %w", err))
			}
			payload = b
		}
	default:
		payload = []byte(raw)
	}
	return mqttc.Message{Topic: p.Topic, Payload: payload, QoS: p.QoS, Retain: p.Retain}, nil
}

func (s *Server) publish(ctx context.Context, msgs []mqttc.Message) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
	defer cancel()
	n, err := s.mqtt.PublishMany(ctx, msgs)
	if errors.Is(err, mqttc.ErrNotConnected) {
		return n, &apiError{Status: 503, Msg: err.Error()}
	}
	if err != nil {
		return n, &apiError{Status: 502, Msg: err.Error()}
	}
	return n, nil
}

// --- webhooks ---

type webhookView struct {
	config.Webhook
	Stats webhook.Stats `json:"stats"`
}

func (s *Server) webhookList() []webhookView {
	stats := s.hooks.AllStats()
	list := s.state.Webhooks()
	out := make([]webhookView, 0, len(list))
	for _, w := range list {
		out = append(out, webhookView{Webhook: w.Redact(), Stats: stats[w.ID]})
	}
	return out
}

func (s *Server) webhookGet(id string) (webhookView, error) {
	w, err := s.state.Webhook(id)
	if err != nil {
		return webhookView{}, notFound("webhook %q not found", id)
	}
	st, _ := s.hooks.Stats(id)
	return webhookView{Webhook: w.Redact(), Stats: st}, nil
}

// webhookPut creates (create=true) or replaces a webhook.
func (s *Server) webhookPut(w config.Webhook, create bool) (webhookView, error) {
	old, err := s.state.Webhook(w.ID)
	exists := err == nil
	switch {
	case create && w.ID == "":
		w.ID = state.NewID()
	case create && exists:
		return webhookView{}, &apiError{Status: 409, Msg: "webhook id already exists"}
	case !create && !exists:
		return webhookView{}, notFound("webhook %q not found", w.ID)
	}
	if exists {
		w.MergeSecrets(old)
	}
	w.Normalize()
	if err := w.Validate(); err != nil {
		return webhookView{}, badRequest(err)
	}
	if err := s.state.PutWebhook(w); err != nil {
		return webhookView{}, err
	}
	s.hooks.Put(w)
	return s.webhookGet(w.ID)
}

func (s *Server) webhookDelete(id string) error {
	if err := s.state.DeleteWebhook(id); err != nil {
		return notFound("webhook %q not found", id)
	}
	s.hooks.Remove(id)
	return nil
}

func (s *Server) webhookTest(id, topicName string, payload []byte) error {
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
		return &apiError{Status: 502, Msg: "webhook test failed: " + err.Error()}
	}
	return nil
}

// --- status ---

type statusView struct {
	Version       string       `json:"version"`
	UptimeSec     int64        `json:"uptime_sec"`
	MQTT          mqttc.Status `json:"mqtt"`
	Topics        int          `json:"topics"`
	TopicsDropped uint64       `json:"topics_dropped"`
	MaxTopics     int          `json:"max_topics"`
	Webhooks      int          `json:"webhooks"`
	BrokerSource  string       `json:"broker_source"`
	StateFile     string       `json:"state_file,omitempty"`
}

func (s *Server) status() statusView {
	return statusView{
		Version:       Version,
		UptimeSec:     int64(time.Since(s.started).Seconds()),
		MQTT:          s.mqtt.Status(),
		Topics:        s.store.Len(),
		TopicsDropped: s.store.Dropped(),
		MaxTopics:     s.cfg.MaxTopics,
		Webhooks:      len(s.state.Webhooks()),
		BrokerSource:  s.source(),
		StateFile:     s.state.Path(),
	}
}

// --- errors ---

type apiError struct {
	Status int
	Msg    string
}

func (e *apiError) Error() string { return e.Msg }

func badRequest(err error) error { return &apiError{Status: 400, Msg: err.Error()} }

func notFound(format string, a ...any) error {
	return &apiError{Status: 404, Msg: fmt.Sprintf(format, a...)}
}
