// Package config defines the configuration model shared by the service,
// loads it from environment variables and validates it.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tabreu8/mqtt-get/internal/topic"
)

// Redacted replaces secrets in API responses. Sending it back in an update
// keeps the stored secret unchanged.
const Redacted = "********"

// Subscription is a topic filter the service subscribes to.
type Subscription struct {
	Filter string `json:"filter"`
	QoS    byte   `json:"qos"`
}

// TLS holds TLS / mutual-TLS settings. PEM fields take precedence over files.
type TLS struct {
	Enabled            bool     `json:"enabled,omitempty"`
	CAFile             string   `json:"ca_file,omitempty"`
	CAPEM              string   `json:"ca_pem,omitempty"`
	CertFile           string   `json:"cert_file,omitempty"`
	CertPEM            string   `json:"cert_pem,omitempty"`
	KeyFile            string   `json:"key_file,omitempty"`
	KeyPEM             string   `json:"key_pem,omitempty"`
	ServerName         string   `json:"server_name,omitempty"`
	InsecureSkipVerify bool     `json:"insecure_skip_verify,omitempty"`
	ALPN               []string `json:"alpn,omitempty"`
}

// Broker describes how to connect to the MQTT broker.
type Broker struct {
	// URLs of the broker (failover order). Schemes: tcp, mqtt, ssl, tls,
	// mqtts, ws, wss, unix.
	URLs         []string `json:"urls"`
	ClientID     string   `json:"client_id,omitempty"`
	Username     string   `json:"username,omitempty"`
	Password     string   `json:"password,omitempty"`
	PasswordFile string   `json:"password_file,omitempty"`
	// AuthMethod selects MQTT 5 enhanced authentication with the username
	// and password: SCRAM-SHA-1, SCRAM-SHA-256 or SCRAM-SHA-512. The
	// password is then never sent to the broker.
	AuthMethod string `json:"auth_method,omitempty"`
	// ProtocolVersion: 3 = MQTT 3.1, 4 = MQTT 3.1.1 (default), 5 = MQTT 5.
	ProtocolVersion uint `json:"protocol_version,omitempty"`
	// MQTT 5 only: session expiry after disconnect (0 = end with the
	// connection) and how many topic aliases the broker may use towards us
	// (saves bandwidth on long topic names; default 1024, 0 disables).
	SessionExpirySec  uint32            `json:"session_expiry_sec,omitempty"`
	TopicAliasMaximum *uint16           `json:"topic_alias_maximum,omitempty"`
	CleanSession      *bool             `json:"clean_session,omitempty"`
	KeepAliveSec      int               `json:"keepalive_sec,omitempty"`
	ConnectTimeoutSec int               `json:"connect_timeout_sec,omitempty"`
	TLS               TLS               `json:"tls"`
	WSHeaders         map[string]string `json:"ws_headers,omitempty"`
	Subscriptions     []Subscription    `json:"subscriptions"`
	// Connections > 1 opens several connections, spreading ingest over
	// several CPU cores in one of two ways:
	//   - SharedGroup set: every connection subscribes to every filter as
	//     $share/<group>/<filter> and the broker load-balances messages
	//     (needs broker support for shared subscriptions).
	//   - SubscriptionMode "split": the filters are divided between the
	//     connections, so each connection receives a different part of the
	//     traffic. Works with any broker; filters must not overlap.
	// Otherwise only the first connection subscribes and the others are
	// used for publishing.
	Connections      int    `json:"connections,omitempty"`
	SharedGroup      string `json:"shared_group,omitempty"`
	SubscriptionMode string `json:"subscription_mode,omitempty"` // "" / "auto" or "split"
	// Client selects the MQTT implementation: "lean" (default, a compact
	// client built for ingest speed) or "paho" (eclipse paho.mqtt.golang for
	// MQTT 3.1/3.1.1, eclipse paho.golang for MQTT 5).
	Client string `json:"client,omitempty"`
	// Will is published by the broker if mqtt-get disconnects unexpectedly.
	Will *Will `json:"will,omitempty"`
}

// Will is an MQTT Last Will and Testament. It is registered on the first
// connection only. With OnlinePayload set, Topic becomes a status topic:
// OnlinePayload is published (with the will's QoS and retain flag) on every
// connect, and Payload on a graceful shutdown, so subscribers always see
// whether mqtt-get is up.
type Will struct {
	Topic         string `json:"topic"`
	Payload       string `json:"payload,omitempty"`
	QoS           byte   `json:"qos,omitempty"`
	Retain        bool   `json:"retain,omitempty"`
	DelaySec      uint32 `json:"delay_sec,omitempty"` // MQTT 5: wait this long before publishing it
	OnlinePayload string `json:"online_payload,omitempty"`
}

// MQTT client implementations.
const (
	ClientLean = "lean"
	ClientPaho = "paho"
)

// Subscription modes.
const (
	SubscriptionAuto  = "auto"
	SubscriptionSplit = "split"
)

// SubscriptionsFor returns the filters connection idx must subscribe to
// (already prefixed with $share/<group>/ when a shared group is set).
func (b *Broker) SubscriptionsFor(idx int) []Subscription {
	n := b.Connections
	if n <= 0 {
		n = 1
	}
	var out []Subscription
	switch {
	case b.SharedGroup != "":
		for _, s := range b.Subscriptions {
			out = append(out, Subscription{Filter: "$share/" + b.SharedGroup + "/" + s.Filter, QoS: s.QoS})
		}
	case b.SubscriptionMode == SubscriptionSplit:
		for i, s := range b.Subscriptions {
			if i%n == idx {
				out = append(out, s)
			}
		}
	case idx == 0:
		out = append(out, b.Subscriptions...)
	}
	return out
}

// IsConfigured reports whether a broker URL has been set.
func (b *Broker) IsConfigured() bool { return len(b.URLs) > 0 }

// UseCleanSession returns the effective clean-session flag (default true).
func (b *Broker) UseCleanSession() bool { return b.CleanSession == nil || *b.CleanSession }

// Normalize fills defaults.
func (b *Broker) Normalize() {
	if b.ProtocolVersion == 0 {
		b.ProtocolVersion = 4
	}
	if b.KeepAliveSec == 0 {
		b.KeepAliveSec = 30
	}
	if b.ConnectTimeoutSec == 0 {
		b.ConnectTimeoutSec = 10
	}
	if b.Connections <= 0 {
		b.Connections = 1
	}
	if b.ClientID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "host"
		}
		b.ClientID = "mqtt-get-" + host
	}
	if b.Subscriptions == nil {
		b.Subscriptions = []Subscription{{Filter: "#", QoS: 0}}
	}
}

// Validate checks the broker configuration.
func (b *Broker) Validate() error {
	for _, u := range b.URLs {
		pu, err := url.Parse(u)
		if err != nil {
			return fmt.Errorf("invalid broker url %q: %w", u, err)
		}
		switch pu.Scheme {
		case "tcp", "mqtt", "ssl", "tls", "mqtts", "mqtt+ssl", "tcps", "ws", "wss", "unix":
		default:
			return fmt.Errorf("unsupported broker url scheme %q (use tcp, mqtt, ssl, mqtts, ws, wss or unix)", pu.Scheme)
		}
	}
	if b.ProtocolVersion < 3 || b.ProtocolVersion > 5 {
		return errors.New("protocol_version must be 3 (MQTT 3.1), 4 (MQTT 3.1.1) or 5 (MQTT 5)")
	}
	if b.ProtocolVersion != 5 && (b.SessionExpirySec != 0 || b.TopicAliasMaximum != nil) {
		return errors.New("session_expiry_sec and topic_alias_maximum need protocol_version 5")
	}
	switch b.AuthMethod {
	case "":
	case "SCRAM-SHA-1", "SCRAM-SHA-256", "SCRAM-SHA-512":
		if b.ProtocolVersion != 5 {
			return errors.New("auth_method (enhanced authentication) needs protocol_version 5")
		}
		if b.Username == "" || (b.Password == "" && b.PasswordFile == "") {
			return errors.New("auth_method " + b.AuthMethod + " needs a username and a password (or password_file)")
		}
	default:
		return errors.New("auth_method must be SCRAM-SHA-1, SCRAM-SHA-256 or SCRAM-SHA-512")
	}
	if w := b.Will; w != nil {
		if err := topic.ValidateName(w.Topic); err != nil {
			return fmt.Errorf("will topic: %w", err)
		}
		if w.QoS > 2 {
			return errors.New("will qos must be 0, 1 or 2")
		}
		if w.DelaySec != 0 && b.ProtocolVersion != 5 {
			return errors.New("will delay_sec needs protocol_version 5")
		}
	}
	if b.Connections > 64 {
		return errors.New("connections must be <= 64")
	}
	switch b.Client {
	case "", ClientLean, ClientPaho:
	default:
		return errors.New(`client must be "lean" or "paho"`)
	}
	switch b.SubscriptionMode {
	case "", SubscriptionAuto:
	case SubscriptionSplit:
		if b.SharedGroup != "" {
			return errors.New("subscription_mode \"split\" and shared_group are alternatives: set only one")
		}
		if a, c, ok := overlapping(b.Subscriptions); ok {
			return fmt.Errorf("subscription_mode \"split\" needs non-overlapping filters, but %q and %q overlap "+
				"(a message matching both would be received twice)", a, c)
		}
	default:
		return errors.New(`subscription_mode must be "auto" or "split"`)
	}
	for _, s := range b.Subscriptions {
		if err := topic.ValidateFilter(s.Filter); err != nil {
			return fmt.Errorf("subscription %q: %w", s.Filter, err)
		}
		if s.QoS > 2 {
			return fmt.Errorf("subscription %q: qos must be 0, 1 or 2", s.Filter)
		}
	}
	if (b.TLS.CertPEM != "" || b.TLS.CertFile != "") != (b.TLS.KeyPEM != "" || b.TLS.KeyFile != "") {
		return errors.New("tls: client certificate and key must be provided together")
	}
	return nil
}

// Redact returns a copy with secrets replaced by Redacted.
func (b Broker) Redact() Broker {
	if b.Password != "" {
		b.Password = Redacted
	}
	if b.TLS.KeyPEM != "" {
		b.TLS.KeyPEM = Redacted
	}
	if len(b.WSHeaders) > 0 {
		h := make(map[string]string, len(b.WSHeaders))
		for k, v := range b.WSHeaders {
			if strings.EqualFold(k, "authorization") {
				v = Redacted
			}
			h[k] = v
		}
		b.WSHeaders = h
	}
	return b
}

// MergeSecrets copies secrets from old into b wherever b holds Redacted.
func (b *Broker) MergeSecrets(old Broker) {
	if b.Password == Redacted {
		b.Password = old.Password
	}
	if b.TLS.KeyPEM == Redacted {
		b.TLS.KeyPEM = old.TLS.KeyPEM
	}
	for k, v := range b.WSHeaders {
		if v == Redacted {
			b.WSHeaders[k] = old.WSHeaders[k]
		}
	}
}

// Webhook forwards matching MQTT messages to an HTTP endpoint.
type Webhook struct {
	ID      string            `json:"id"`
	Name    string            `json:"name,omitempty"`
	URL     string            `json:"url"`
	Topics  []string          `json:"topics"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	// Secret enables an HMAC-SHA256 signature header.
	Secret string `json:"secret,omitempty"`
	// Format: "json" (default, message envelope) or "raw" (payload as body,
	// metadata in headers).
	Format string `json:"format,omitempty"`
	// BatchSize > 1 sends up to that many messages per request as a JSON
	// array (json format only), flushed at least every BatchIntervalMs.
	BatchSize       int   `json:"batch_size,omitempty"`
	BatchIntervalMs int   `json:"batch_interval_ms,omitempty"`
	TimeoutMs       int   `json:"timeout_ms,omitempty"`
	MaxRetries      *int  `json:"max_retries,omitempty"`
	QueueSize       int   `json:"queue_size,omitempty"`
	Concurrency     int   `json:"concurrency,omitempty"`
	Enabled         *bool `json:"enabled,omitempty"`
}

// IsEnabled returns the effective enabled flag (default true).
func (w *Webhook) IsEnabled() bool { return w.Enabled == nil || *w.Enabled }

// Retries returns the effective retry count (default 3).
func (w *Webhook) Retries() int {
	if w.MaxRetries == nil {
		return 3
	}
	return *w.MaxRetries
}

// Normalize fills defaults.
func (w *Webhook) Normalize() {
	if w.Method == "" {
		w.Method = "POST"
	}
	w.Method = strings.ToUpper(w.Method)
	if w.Format == "" {
		w.Format = "json"
	}
	if w.BatchSize <= 0 {
		w.BatchSize = 1
	}
	if w.BatchIntervalMs <= 0 {
		w.BatchIntervalMs = 200
	}
	if w.TimeoutMs <= 0 {
		w.TimeoutMs = 5000
	}
	if w.QueueSize <= 0 {
		w.QueueSize = 10000
	}
	if w.Concurrency <= 0 {
		w.Concurrency = 1
	}
}

// Validate checks the webhook.
func (w *Webhook) Validate() error {
	u, err := url.Parse(w.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("url must be an absolute http(s) URL")
	}
	if len(w.Topics) == 0 {
		return errors.New("at least one topic filter is required")
	}
	for _, f := range w.Topics {
		if err := topic.ValidateFilter(f); err != nil {
			return fmt.Errorf("topic %q: %w", f, err)
		}
	}
	switch w.Method {
	case "POST", "PUT", "PATCH":
	default:
		return errors.New("method must be POST, PUT or PATCH")
	}
	switch w.Format {
	case "json":
	case "raw":
		if w.BatchSize > 1 {
			return errors.New("batching requires format \"json\"")
		}
	default:
		return errors.New("format must be \"json\" or \"raw\"")
	}
	if w.BatchSize > 10000 || w.QueueSize > 10_000_000 || w.Concurrency > 256 {
		return errors.New("batch_size, queue_size or concurrency too large")
	}
	return nil
}

// Redact returns a copy with secrets hidden.
func (w Webhook) Redact() Webhook {
	if w.Secret != "" {
		w.Secret = Redacted
	}
	if len(w.Headers) > 0 {
		h := make(map[string]string, len(w.Headers))
		for k, v := range w.Headers {
			if isSecretHeader(k) {
				v = Redacted
			}
			h[k] = v
		}
		w.Headers = h
	}
	return w
}

// MergeSecrets copies secrets from old wherever w holds Redacted.
func (w *Webhook) MergeSecrets(old Webhook) {
	if w.Secret == Redacted {
		w.Secret = old.Secret
	}
	for k, v := range w.Headers {
		if v == Redacted {
			w.Headers[k] = old.Headers[k]
		}
	}
}

func isSecretHeader(k string) bool {
	k = strings.ToLower(k)
	return k == "authorization" || strings.Contains(k, "token") || strings.Contains(k, "key") || strings.Contains(k, "secret")
}

// HistoryRule keeps a bounded history of the messages on matching topics,
// in memory. Every limit applies per topic except MaxBytes, which bounds all
// the topics of the rule together. When several rules match a topic, the
// first one (in configuration order) applies.
type HistoryRule struct {
	ID     string   `json:"id"`
	Name   string   `json:"name,omitempty"`
	Topics []string `json:"topics"`
	// MaxPoints is the ring buffer size per topic (default 1000).
	MaxPoints int `json:"max_points,omitempty"`
	// MaxAgeSec drops points older than this (0 = no age limit).
	MaxAgeSec int `json:"max_age_sec,omitempty"`
	// MinIntervalMs keeps at most one point per interval per topic (the
	// first one), downsampling fast publishers. 0 keeps every message.
	MinIntervalMs int `json:"min_interval_ms,omitempty"`
	// OnChange skips a message whose payload equals the previous point's.
	OnChange bool `json:"on_change,omitempty"`
	// MaxBytes bounds the memory of all the rule's topics together
	// (0 = only the global HISTORY_MAX_BYTES applies).
	MaxBytes int64 `json:"max_bytes,omitempty"`
	Enabled  *bool `json:"enabled,omitempty"`
	// Env marks the rule built from HISTORY_* environment variables (not
	// persisted, read-only through the API).
	Env bool `json:"env,omitempty"`
}

// History limits.
const (
	DefaultHistoryPoints = 1000
	MaxHistoryPoints     = 1_000_000
)

// IsEnabled returns the effective enabled flag (default true).
func (r *HistoryRule) IsEnabled() bool { return r.Enabled == nil || *r.Enabled }

// Normalize fills defaults.
func (r *HistoryRule) Normalize() {
	if r.MaxPoints <= 0 {
		r.MaxPoints = DefaultHistoryPoints
	}
}

// Validate checks the rule.
func (r *HistoryRule) Validate() error {
	if len(r.Topics) == 0 {
		return errors.New("at least one topic filter is required")
	}
	for _, f := range r.Topics {
		if err := topic.ValidateFilter(f); err != nil {
			return fmt.Errorf("topic %q: %w", f, err)
		}
	}
	if r.MaxPoints > MaxHistoryPoints {
		return fmt.Errorf("max_points must be <= %d", MaxHistoryPoints)
	}
	if r.MaxAgeSec < 0 || r.MinIntervalMs < 0 || r.MaxBytes < 0 {
		return errors.New("max_age_sec, min_interval_ms and max_bytes must not be negative")
	}
	return nil
}

// Sparkplug configures Sparkplug B support: messages on spBv1.0/# are
// decoded into plain topics <prefix>/<group>/<edge_node>[/<device>]/<metric>
// holding JSON values, which feed the store, history, webhooks and agents.
type Sparkplug struct {
	Enabled bool `json:"enabled"`
	// Prefix of the translated topics (default "spb").
	Prefix string `json:"prefix,omitempty"`
	// KeepRaw also stores the raw protobuf messages under spBv1.0/...
	KeepRaw bool `json:"keep_raw,omitempty"`
	// Rebirth publishes a rebirth request (NCMD Node Control/Rebirth) when
	// a node's metrics can't be decoded (no birth seen, unknown alias) or
	// messages were lost, at most once per RebirthIntervalSec per node.
	Rebirth            bool `json:"rebirth,omitempty"`
	RebirthIntervalSec int  `json:"rebirth_interval_sec,omitempty"`
	// Commands lets publishes to translated metric topics be sent to the
	// edge node as NCMD/DCMD writes.
	Commands bool `json:"commands,omitempty"`
	// CommandsFilter limits writes to the translated metric topics matching
	// one of these topic filters (e.g. "spb/plant1/+/+/Setpoints/#").
	// Empty: every metric is writable when Commands is on.
	CommandsFilter []string `json:"commands_filter,omitempty"`
}

// Normalize fills defaults.
func (s *Sparkplug) Normalize() {
	s.Prefix = strings.Trim(s.Prefix, "/")
	if s.Prefix == "" {
		s.Prefix = "spb"
	}
	if s.RebirthIntervalSec <= 0 {
		s.RebirthIntervalSec = 30
	}
	var filters []string
	for _, f := range s.CommandsFilter {
		if f = strings.TrimSpace(f); f != "" {
			filters = append(filters, f)
		}
	}
	s.CommandsFilter = filters
}

// Validate checks the settings.
func (s *Sparkplug) Validate() error {
	if err := topic.ValidateName(s.Prefix); err != nil {
		return fmt.Errorf("prefix: %w", err)
	}
	if s.Prefix == "spBv1.0" || strings.HasPrefix(s.Prefix, "spBv1.0/") {
		return errors.New("prefix must not be inside the spBv1.0 namespace")
	}
	if s.RebirthIntervalSec > 86400 {
		return errors.New("rebirth_interval_sec must be <= 86400")
	}
	if len(s.CommandsFilter) > 1000 {
		return errors.New("commands_filter: at most 1000 filters")
	}
	for _, f := range s.CommandsFilter {
		if err := topic.ValidateFilter(f); err != nil {
			return fmt.Errorf("commands_filter %q: %w", f, err)
		}
	}
	return nil
}

// API key scopes.
const (
	ScopeRead    = "read"    // read latest values, topics, status
	ScopePublish = "publish" // publish messages
	ScopeAdmin   = "admin"   // everything, including configuration
)

// ValidScope reports whether s is a known scope.
func ValidScope(s string) bool { return s == ScopeRead || s == ScopePublish || s == ScopeAdmin }

// APIKey is a stored API key. Only the SHA-256 hash of the key is kept.
type APIKey struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Hash      string    `json:"hash,omitempty"`
	Prefix    string    `json:"prefix"`
	Scopes    []string  `json:"scopes"`
	CreatedAt time.Time `json:"created_at"`
	// Env marks keys that come from environment variables (not persisted).
	Env bool `json:"env,omitempty"`
}

// Server holds process-level settings (environment only).
type Server struct {
	HTTPAddr         string
	DataDir          string
	AuthDisabled     bool
	AdminKeys        []string
	ReadKeys         []string
	PublishKeys      []string
	MaxTopics        int
	MaxBodyBytes     int64
	UIEnabled        bool
	MCPEnabled       bool
	MetricsPublic    bool
	AllowQueryAPIKey bool
	CORSOrigins      string
	LogLevel         string
	LogFormat        string
	PublishTimeout   time.Duration
	TLSCertFile      string
	TLSKeyFile       string
	// History: global memory limit, and whether (and how often) history is
	// saved to DATA_DIR/history.bin to survive restarts.
	HistoryMaxBytes    int64
	HistoryPersist     bool
	HistorySnapshotSec int
}

// Env is the source of environment variables (overridable in tests).
var Env = os.Getenv

// LoadServer reads server settings from the environment.
func LoadServer() (Server, error) {
	s := Server{
		HTTPAddr:         envStr("HTTP_ADDR", ":8080"),
		DataDir:          envStr("DATA_DIR", "./data"),
		AuthDisabled:     envBool("AUTH_DISABLED", false),
		AdminKeys:        envList("API_KEYS"),
		ReadKeys:         envList("API_KEYS_READ"),
		PublishKeys:      envList("API_KEYS_PUBLISH"),
		MaxTopics:        envInt("MAX_TOPICS", 1_000_000),
		MaxBodyBytes:     int64(envInt("MAX_BODY_BYTES", 1<<20)),
		UIEnabled:        envBool("UI_ENABLED", true),
		MCPEnabled:       envBool("MCP_ENABLED", true),
		MetricsPublic:    envBool("METRICS_PUBLIC", false),
		AllowQueryAPIKey: envBool("ALLOW_QUERY_API_KEY", false),
		CORSOrigins:      envStr("CORS_ORIGINS", ""),
		LogLevel:         envStr("LOG_LEVEL", "info"),
		LogFormat:        envStr("LOG_FORMAT", "text"),
		PublishTimeout:   time.Duration(envInt("PUBLISH_TIMEOUT_MS", 5000)) * time.Millisecond,
		TLSCertFile:      envStr("HTTP_TLS_CERT_FILE", ""),
		TLSKeyFile:       envStr("HTTP_TLS_KEY_FILE", ""),

		HistoryMaxBytes:    int64(envInt("HISTORY_MAX_BYTES", 256<<20)),
		HistoryPersist:     envBool("HISTORY_PERSIST", true),
		HistorySnapshotSec: envInt("HISTORY_SNAPSHOT_SEC", 300),
	}
	if (s.TLSCertFile == "") != (s.TLSKeyFile == "") {
		return s, errors.New("HTTP_TLS_CERT_FILE and HTTP_TLS_KEY_FILE must be set together")
	}
	return s, nil
}

// HistoryFromEnv builds the history rule given by HISTORY_TOPICS (and the
// other HISTORY_* variables). ok is false when HISTORY_TOPICS is not set.
func HistoryFromEnv() (r HistoryRule, ok bool, err error) {
	r.Topics = envList("HISTORY_TOPICS")
	if len(r.Topics) == 0 {
		return r, false, nil
	}
	r.ID, r.Name, r.Env = "env", "environment (HISTORY_TOPICS)", true
	r.MaxPoints = envInt("HISTORY_MAX_POINTS", DefaultHistoryPoints)
	r.MaxAgeSec = envInt("HISTORY_MAX_AGE_SEC", 0)
	r.MinIntervalMs = envInt("HISTORY_MIN_INTERVAL_MS", 0)
	r.OnChange = envBool("HISTORY_ON_CHANGE", false)
	r.Normalize()
	return r, true, r.Validate()
}

// SparkplugFromEnv reads SPARKPLUG_* variables. ok is false when
// SPARKPLUG_ENABLED is not set.
func SparkplugFromEnv() (s Sparkplug, ok bool, err error) {
	if Env("SPARKPLUG_ENABLED") == "" {
		return s, false, nil
	}
	s = Sparkplug{
		Enabled:            envBool("SPARKPLUG_ENABLED", false),
		Prefix:             envStr("SPARKPLUG_PREFIX", ""),
		KeepRaw:            envBool("SPARKPLUG_KEEP_RAW", false),
		Rebirth:            envBool("SPARKPLUG_REBIRTH", false),
		RebirthIntervalSec: envInt("SPARKPLUG_REBIRTH_INTERVAL_SEC", 0),
		Commands:           envBool("SPARKPLUG_COMMANDS", false),
		CommandsFilter:     envList("SPARKPLUG_COMMANDS_FILTER"),
	}
	s.Normalize()
	return s, true, s.Validate()
}

// BrokerFromEnv builds a broker configuration from MQTT_* variables. ok is
// false when MQTT_URL is not set.
func BrokerFromEnv() (b Broker, ok bool, err error) {
	b.URLs = envList("MQTT_URL")
	if len(b.URLs) == 0 {
		return b, false, nil
	}
	b.ClientID = envStr("MQTT_CLIENT_ID", "")
	b.Username = envStr("MQTT_USERNAME", "")
	b.Password = envStr("MQTT_PASSWORD", "")
	b.PasswordFile = envStr("MQTT_PASSWORD_FILE", "")
	b.AuthMethod = envStr("MQTT_AUTH_METHOD", "")
	b.ProtocolVersion = uint(envInt("MQTT_PROTOCOL_VERSION", 4))
	if v := Env("MQTT_CLEAN_SESSION"); v != "" {
		c := envBool("MQTT_CLEAN_SESSION", true)
		b.CleanSession = &c
	}
	b.KeepAliveSec = envInt("MQTT_KEEPALIVE_SEC", 30)
	b.ConnectTimeoutSec = envInt("MQTT_CONNECT_TIMEOUT_SEC", 10)
	b.Connections = envInt("MQTT_CONNECTIONS", 1)
	b.SharedGroup = envStr("MQTT_SHARED_GROUP", "")
	b.SubscriptionMode = envStr("MQTT_SUBSCRIPTION_MODE", "")
	b.Client = envStr("MQTT_CLIENT", "")
	b.SessionExpirySec = uint32(envInt("MQTT_SESSION_EXPIRY_SEC", 0))
	if v := Env("MQTT_TOPIC_ALIAS_MAXIMUM"); v != "" {
		n := uint16(envInt("MQTT_TOPIC_ALIAS_MAXIMUM", 0))
		b.TopicAliasMaximum = &n
	}
	if t := envStr("MQTT_WILL_TOPIC", ""); t != "" {
		b.Will = &Will{
			Topic:         t,
			Payload:       envStr("MQTT_WILL_PAYLOAD", ""),
			QoS:           byte(envInt("MQTT_WILL_QOS", 0)),
			Retain:        envBool("MQTT_WILL_RETAIN", false),
			DelaySec:      uint32(envInt("MQTT_WILL_DELAY_SEC", 0)),
			OnlinePayload: envStr("MQTT_WILL_ONLINE_PAYLOAD", ""),
		}
	}
	b.TLS = TLS{
		Enabled:            envBool("MQTT_TLS", false),
		CAFile:             envStr("MQTT_TLS_CA_FILE", ""),
		CAPEM:              envStr("MQTT_TLS_CA_PEM", ""),
		CertFile:           envStr("MQTT_TLS_CERT_FILE", ""),
		CertPEM:            envStr("MQTT_TLS_CERT_PEM", ""),
		KeyFile:            envStr("MQTT_TLS_KEY_FILE", ""),
		KeyPEM:             envStr("MQTT_TLS_KEY_PEM", ""),
		ServerName:         envStr("MQTT_TLS_SERVER_NAME", ""),
		InsecureSkipVerify: envBool("MQTT_TLS_INSECURE", false),
		ALPN:               envList("MQTT_TLS_ALPN"),
	}
	if hs := envList("MQTT_WS_HEADERS"); len(hs) > 0 {
		b.WSHeaders = map[string]string{}
		for _, h := range hs {
			k, v, found := strings.Cut(h, ":")
			if !found {
				return b, true, fmt.Errorf("MQTT_WS_HEADERS: expected Name:Value, got %q", h)
			}
			b.WSHeaders[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	defQoS := envInt("MQTT_QOS", 0)
	topics := envList("MQTT_TOPICS")
	if len(topics) == 0 {
		topics = []string{"#"}
	}
	for _, t := range topics {
		// "filter" or "filter:qos" (filters never contain ':'-suffixed digits
		// in practice; only a trailing :0/:1/:2 is treated as QoS).
		q := defQoS
		if i := strings.LastIndexByte(t, ':'); i > 0 && i == len(t)-2 && t[i+1] >= '0' && t[i+1] <= '2' {
			q = int(t[i+1] - '0')
			t = t[:i]
		}
		b.Subscriptions = append(b.Subscriptions, Subscription{Filter: t, QoS: byte(q)})
	}
	b.Normalize()
	return b, true, b.Validate()
}

func envStr(k, def string) string {
	if v := Env(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := Env(k); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v := Env(k); v != "" {
		if b, err := strconv.ParseBool(strings.TrimSpace(v)); err == nil {
			return b
		}
	}
	return def
}

func envList(k string) []string {
	v := Env(k)
	if v == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
