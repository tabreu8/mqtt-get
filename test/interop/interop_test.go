// Package interop runs mqtt-get end-to-end against real MQTT brokers.
//
// It is skipped unless MQTT_INTEROP_CONFIG points at a JSON file describing
// the broker endpoints to test (see brokers.example.json and README.md).
// Every endpoint is exercised through the public HTTP API of a full
// mqtt-get server: connect/auth, ingest, REST GET, REST publish at every
// QoS, retained messages, binary payloads, wildcard queries, webhooks and,
// optionally, shared subscriptions and negative auth checks.
package interop

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/httpapi"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// Endpoint is one way of connecting to a broker.
type Endpoint struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	// TLS: "" (none), "ca" (verify server with the CA) or "mtls" (also
	// present the client certificate).
	TLS        string `json:"tls,omitempty"`
	ServerName string `json:"server_name,omitempty"`
	// Negative checks.
	WrongPasswordRejected bool `json:"wrong_password_rejected,omitempty"`
	NoClientCertRejected  bool `json:"no_client_cert_rejected,omitempty"`
	// SharedSubscriptions runs the $share scale-out check.
	SharedSubscriptions bool `json:"shared_subscriptions,omitempty"`
	// SplitSubscriptions runs the split-filters scale-out check.
	SplitSubscriptions bool `json:"split_subscriptions,omitempty"`
	// Skip lists sub-tests to skip for this endpoint, with a reason.
	Skip map[string]string `json:"skip,omitempty"`
}

// Broker groups the endpoints of one broker product.
type Broker struct {
	Name      string     `json:"name"`
	Endpoints []Endpoint `json:"endpoints"`
}

// File is the MQTT_INTEROP_CONFIG format. Relative paths are resolved
// against the file's directory.
type File struct {
	CAFile         string   `json:"ca_file"`
	ClientCertFile string   `json:"client_cert_file"`
	ClientKeyFile  string   `json:"client_key_file"`
	Brokers        []Broker `json:"brokers"`
}

func loadFile(t *testing.T) File {
	path := os.Getenv("MQTT_INTEROP_CONFIG")
	if path == "" {
		t.Skip("MQTT_INTEROP_CONFIG not set; see test/interop/README.md")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(path)
	for _, p := range []*string{&f.CAFile, &f.ClientCertFile, &f.ClientKeyFile} {
		if *p != "" && !filepath.IsAbs(*p) {
			*p = filepath.Join(dir, *p)
		}
	}
	if only := os.Getenv("MQTT_INTEROP_ONLY"); only != "" {
		var keep []Broker
		for _, b := range f.Brokers {
			for _, o := range strings.Split(only, ",") {
				if strings.EqualFold(strings.TrimSpace(o), b.Name) {
					keep = append(keep, b)
				}
			}
		}
		f.Brokers = keep
	}
	return f
}

func (f File) brokerConfig(ep Endpoint, clientID string, topics ...string) config.Broker {
	b := config.Broker{
		URLs:              []string{ep.URL},
		ClientID:          clientID,
		Username:          ep.Username,
		Password:          ep.Password,
		ConnectTimeoutSec: 5,
	}
	for _, tp := range topics {
		b.Subscriptions = append(b.Subscriptions, config.Subscription{Filter: tp, QoS: 1})
	}
	switch ep.TLS {
	case "ca", "mtls":
		b.TLS.CAFile = f.CAFile
		b.TLS.ServerName = ep.ServerName
		if ep.TLS == "mtls" {
			b.TLS.CertFile, b.TLS.KeyFile = f.ClientCertFile, f.ClientKeyFile
		}
	}
	return b
}

// --- harness -----------------------------------------------------------

type harness struct {
	t    *testing.T
	f    File
	ep   Endpoint
	ns   string // unique topic namespace for this run
	srv  *core.Service
	http *httptest.Server

	peer *mqttc.Manager // independent client acting as a device
	got  chan *store.Entry
}

const apiKey = "interop-admin"

func newHarness(t *testing.T, f File, ep Endpoint) *harness {
	h := &harness{t: t, f: f, ep: ep, got: make(chan *store.Entry, 1024)}
	h.ns = fmt.Sprintf("mginterop/%s/%d", sanitize(ep.Name), rand.Int63())

	st, err := state.Open("")
	if err != nil {
		t.Fatal(err)
	}
	st.SetEnvKeys([]string{apiKey}, nil, nil)
	cfg := config.Server{MaxTopics: 100000, MaxBodyBytes: 1 << 20, PublishTimeout: 10 * time.Second}
	h.srv = core.New(cfg, quietLog(), st)
	if err := h.srv.Start(f.brokerConfig(ep, clientID("mg"), h.ns+"/#"), true); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(h.srv.Close)
	h.http = httptest.NewServer(httpapi.New(h.srv, nil).Handler())
	t.Cleanup(h.http.Close)

	h.peer = mqttc.New(quietLog(), func(e *store.Entry) {
		select {
		case h.got <- e:
		default:
		}
	})
	if err := h.peer.Apply(f.brokerConfig(ep, clientID("peer"), h.ns+"/to-device/#")); err != nil {
		t.Fatalf("peer: %v", err)
	}
	t.Cleanup(h.peer.Close)
	return h
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func clientID(prefix string) string { return fmt.Sprintf("%s-%d", prefix, rand.Int31()) }

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '/' || r == '+' || r == '#' || r == ' ' {
			return '_'
		}
		return r
	}, s)
}

func waitFor(t *testing.T, what string, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (h *harness) waitConnected() {
	h.t.Helper()
	waitFor(h.t, "mqtt-get and peer to connect", 15*time.Second, func() bool {
		return h.status().MQTT.Connected && h.peer.Status().Connected
	})
	time.Sleep(300 * time.Millisecond) // let SUBACKs land
}

type statusResp struct {
	MQTT struct {
		Connected bool   `json:"connected"`
		LastError string `json:"last_error"`
		Received  uint64 `json:"messages_received"`
	} `json:"mqtt"`
}

func (h *harness) status() statusResp {
	var s statusResp
	_, body := h.do("GET", "/api/v1/status", "")
	_ = json.Unmarshal(body, &s)
	return s
}

func (h *harness) do(method, path, body string) (int, []byte) {
	h.t.Helper()
	req, _ := http.NewRequest(method, h.http.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

type valueResp struct {
	Topic    string          `json:"topic"`
	Payload  json.RawMessage `json:"payload"`
	Encoding string          `json:"encoding"`
	QoS      int             `json:"qos"`
	Retained bool            `json:"retained"`
}

// getValue polls GET /api/v1/values until check passes.
func (h *harness) getValue(topic string, check func(valueResp) bool) valueResp {
	h.t.Helper()
	var v valueResp
	waitFor(h.t, "value on "+topic, 10*time.Second, func() bool {
		code, body := h.do("GET", "/api/v1/values?topic="+topic, "")
		if code != 200 {
			return false
		}
		_ = json.Unmarshal(body, &v)
		return check(v)
	})
	return v
}

// expectPeer waits for the device-side client to receive topic.
func (h *harness) expectPeer(topic string) *store.Entry {
	h.t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case e := <-h.got:
			if e.Topic == topic {
				return e
			}
		case <-deadline:
			h.t.Fatalf("peer did not receive %s", topic)
			return nil
		}
	}
}

func (h *harness) peerPublish(topic string, payload []byte, qos byte, retain bool) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.peer.Publish(ctx, topic, payload, qos, retain); err != nil {
		h.t.Fatalf("peer publish: %v", err)
	}
}

// --- tests -------------------------------------------------------------

func TestInterop(t *testing.T) {
	f := loadFile(t)
	for _, b := range f.Brokers {
		t.Run(b.Name, func(t *testing.T) {
			for _, ep := range b.Endpoints {
				t.Run(ep.Name, func(t *testing.T) { runEndpoint(t, f, ep) })
			}
		})
	}
}

func runEndpoint(t *testing.T, f File, ep Endpoint) {
	h := newHarness(t, f, ep)
	h.waitConnected()
	sub := func(name string, fn func(t *testing.T)) {
		if why, ok := ep.Skip[name]; ok {
			t.Run(name, func(t *testing.T) { t.Skip(why) })
			return
		}
		t.Run(name, fn)
	}

	sub("ingest_and_rest_get", func(t *testing.T) {
		topic := h.ns + "/sensors/temp"
		h.peerPublish(topic, []byte(`{"celsius":21.5}`), 1, false)
		v := h.getValue(topic, func(v valueResp) bool { return string(v.Payload) == `{"celsius":21.5}` })
		if v.Encoding != "json" {
			t.Fatalf("encoding = %s", v.Encoding)
		}
		// Raw format returns the exact bytes.
		code, body := h.do("GET", "/api/v1/values?topic="+topic+"&format=raw", "")
		if code != 200 || string(body) != `{"celsius":21.5}` {
			t.Fatalf("raw: %d %s", code, body)
		}
	})

	sub("latest_value_wins", func(t *testing.T) {
		topic := h.ns + "/counter"
		for i := 1; i <= 20; i++ {
			h.peerPublish(topic, []byte(strconv.Itoa(i)), 1, false)
		}
		h.getValue(topic, func(v valueResp) bool { return string(v.Payload) == "20" })
	})

	for _, qos := range []int{0, 1, 2} {
		sub(fmt.Sprintf("rest_publish_qos%d", qos), func(t *testing.T) {
			topic := fmt.Sprintf("%s/to-device/qos%d", h.ns, qos)
			code, body := h.do("POST", fmt.Sprintf("/api/v1/publish?qos=%d", qos),
				fmt.Sprintf(`{"topic":%q,"payload":"hello-%d","qos":%d}`, topic, qos, qos))
			if code != 200 {
				t.Fatalf("publish: %d %s", code, body)
			}
			if e := h.expectPeer(topic); string(e.Payload) != fmt.Sprintf("hello-%d", qos) {
				t.Fatalf("payload %q", e.Payload)
			}
		})
	}

	sub("rest_publish_batch", func(t *testing.T) {
		var msgs []string
		for i := 0; i < 50; i++ {
			msgs = append(msgs, fmt.Sprintf(`{"topic":"%s/to-device/batch/%d","payload":%d,"qos":1}`, h.ns, i, i))
		}
		code, body := h.do("POST", "/api/v1/publish", "["+strings.Join(msgs, ",")+"]")
		if code != 200 || !strings.Contains(string(body), `"published":50`) {
			t.Fatalf("batch: %d %s", code, body)
		}
		seen := map[string]bool{}
		deadline := time.After(10 * time.Second)
		for len(seen) < 50 {
			select {
			case e := <-h.got:
				if strings.HasPrefix(e.Topic, h.ns+"/to-device/batch/") {
					seen[e.Topic] = true
				}
			case <-deadline:
				t.Fatalf("peer received %d/50 batch messages", len(seen))
			}
		}
	})

	sub("binary_payload", func(t *testing.T) {
		topic := h.ns + "/to-device/bin"
		raw := []byte{0x00, 0xff, 0x10, 0x80, 0x7f}
		req, _ := http.NewRequest("POST", h.http.URL+"/api/v1/publish/"+topic+"?qos=1", bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("publish raw: %v %v", err, resp)
		}
		resp.Body.Close()
		if e := h.expectPeer(topic); !bytes.Equal(e.Payload, raw) {
			t.Fatalf("payload %x", e.Payload)
		}
		// And binary ingest is exposed as base64.
		in := h.ns + "/bin-in"
		h.peerPublish(in, raw, 1, false)
		v := h.getValue(in, func(v valueResp) bool { return v.Encoding == "base64" })
		var s string
		_ = json.Unmarshal(v.Payload, &s)
		if dec, _ := base64.StdEncoding.DecodeString(s); !bytes.Equal(dec, raw) {
			t.Fatalf("base64 payload %s", s)
		}
	})

	sub("retained_after_reconnect", func(t *testing.T) {
		topic := h.ns + "/retained/state"
		h.peerPublish(topic, []byte("ON"), 1, true)
		h.getValue(topic, func(v valueResp) bool { return string(v.Payload) == `"ON"` })
		// Forget everything and reconnect: the broker must replay the
		// retained message and mqtt-get must flag it as retained.
		if code, _ := h.do("DELETE", "/api/v1/values", ""); code != 204 {
			t.Fatal("clear")
		}
		if code, body := h.do("POST", "/api/v1/broker/reconnect", ""); code != 200 {
			t.Fatalf("reconnect: %d %s", code, body)
		}
		v := h.getValue(topic, func(v valueResp) bool { return string(v.Payload) == `"ON"` })
		if !v.Retained {
			t.Fatal("expected retained flag after resubscribe")
		}
		h.peerPublish(topic, nil, 1, true) // clean up the retained message
	})

	sub("wildcard_query", func(t *testing.T) {
		for _, room := range []string{"kitchen", "office", "garage"} {
			h.peerPublish(h.ns+"/rooms/"+room+"/temp", []byte("20"), 1, false)
		}
		h.peerPublish(h.ns+"/rooms/kitchen/humidity", []byte("40"), 1, false)
		filter := strings.ReplaceAll(h.ns+"/rooms/+/temp", "+", "%2B")
		waitFor(t, "3 matching topics", 10*time.Second, func() bool {
			_, body := h.do("GET", "/api/v1/values?filter="+filter, "")
			return strings.Contains(string(body), `"total":3,`)
		})
	})

	sub("webhook", func(t *testing.T) {
		var mu sync.Mutex
		var bodies []string
		recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			bodies = append(bodies, string(b))
			mu.Unlock()
		}))
		defer recv.Close()
		wh := fmt.Sprintf(`{"url":%q,"topics":[%q],"batch_size":10,"batch_interval_ms":100}`, recv.URL, h.ns+"/alarms/#")
		if code, body := h.do("POST", "/api/v1/webhooks", wh); code != 201 {
			t.Fatalf("create webhook: %d %s", code, body)
		}
		for i := 0; i < 5; i++ {
			h.peerPublish(fmt.Sprintf("%s/alarms/%d", h.ns, i), []byte(`{"level":"high"}`), 1, false)
		}
		h.peerPublish(h.ns+"/not-an-alarm", []byte("x"), 1, false)
		waitFor(t, "5 webhook deliveries", 10*time.Second, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return strings.Count(strings.Join(bodies, ""), `"level":"high"`) == 5
		})
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(strings.Join(bodies, ""), "not-an-alarm") {
			t.Fatal("webhook received a non-matching topic")
		}
	})

	if ep.WrongPasswordRejected {
		sub("wrong_password_rejected", func(t *testing.T) {
			bad := ep
			bad.Password = "definitely-wrong"
			expectRejected(t, f.brokerConfig(bad, clientID("bad"), "x/#"))
		})
	}
	if ep.NoClientCertRejected {
		sub("no_client_cert_rejected", func(t *testing.T) {
			noCert := ep
			noCert.TLS = "ca"
			expectRejected(t, f.brokerConfig(noCert, clientID("nocert"), "x/#"))
		})
	}
	if ep.TLS != "" {
		sub("untrusted_ca_rejected", func(t *testing.T) {
			cfg := f.brokerConfig(ep, clientID("noca"), "x/#")
			cfg.TLS.CAFile = "" // system roots only
			expectRejected(t, cfg)
		})
	}
	if ep.SharedSubscriptions {
		sub("shared_subscriptions", func(t *testing.T) { testShared(t, f, ep, h) })
	}
	if ep.SplitSubscriptions {
		sub("split_subscriptions", func(t *testing.T) { testSplit(t, f, ep, h) })
	}
}

func expectRejected(t *testing.T, cfg config.Broker) {
	t.Helper()
	m := mqttc.New(quietLog(), func(*store.Entry) {})
	defer m.Close()
	if err := m.Apply(cfg); err != nil {
		return // rejected at configuration time
	}
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		st := m.Status()
		if st.Connected {
			t.Fatal("connection unexpectedly accepted")
		}
		if st.LastError != "" {
			t.Logf("rejected as expected: %s", st.LastError)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no connection and no error reported")
}

// testShared checks that with 3 connections in a shared-subscription group
// every message is ingested exactly once (not once per connection).
func testShared(t *testing.T, f File, ep Endpoint, h *harness) {
	ns := h.ns + "/shared"
	st, _ := state.Open("")
	srv := core.New(config.Server{MaxTopics: 100000}, quietLog(), st)
	cfg := f.brokerConfig(ep, clientID("mgshared"), ns+"/#")
	cfg.Connections = 3
	cfg.SharedGroup = "mginterop" + strconv.Itoa(rand.Intn(1e6))
	if err := srv.Start(cfg, true); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	waitFor(t, "3 shared connections", 15*time.Second, func() bool { return srvConnected(srv) == 3 })
	time.Sleep(500 * time.Millisecond)
	const n = 300
	for i := 0; i < n; i++ {
		h.peerPublish(fmt.Sprintf("%s/%d", ns, i), []byte("x"), 1, false)
	}
	waitFor(t, "all shared messages", 15*time.Second, func() bool { return srv.Store().Len() == n })
	time.Sleep(500 * time.Millisecond)
	if got := srv.MQTTStatus().MessagesReceived; got != n {
		t.Fatalf("received %d messages for %d published: shared subscription not honoured", got, n)
	}
	per := srv.MQTTStatus().ReceivedPerConnection
	t.Logf("messages per connection: %v", per)
	busy := 0
	for _, c := range per {
		if c > 0 {
			busy++
		}
	}
	if busy < 2 {
		t.Fatalf("broker did not spread shared-subscription load: %v", per)
	}
}

func srvConnected(s *core.Service) int { return s.MQTTStatus().ConnectedCount }

// testSplit checks subscription_mode "split": 3 connections each subscribe
// to one of 3 filters; every message is ingested exactly once, on the
// connection that owns its filter. Needs no broker support.
func testSplit(t *testing.T, f File, ep Endpoint, h *harness) {
	ns := h.ns + "/split"
	st, _ := state.Open("")
	srv := core.New(config.Server{MaxTopics: 100000}, quietLog(), st)
	cfg := f.brokerConfig(ep, clientID("mgsplit"))
	cfg.Subscriptions = []config.Subscription{{Filter: ns + "/a/#", QoS: 1}, {Filter: ns + "/b/#", QoS: 1}, {Filter: ns + "/c/#", QoS: 1}}
	cfg.Connections = 3
	cfg.SubscriptionMode = config.SubscriptionSplit
	if err := srv.Start(cfg, true); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	waitFor(t, "3 split connections", 15*time.Second, func() bool { return srvConnected(srv) == 3 })
	time.Sleep(500 * time.Millisecond)
	const per = 100
	for i := 0; i < per; i++ {
		for _, part := range []string{"a", "b", "c"} {
			h.peerPublish(fmt.Sprintf("%s/%s/%d", ns, part, i), []byte("x"), 1, false)
		}
	}
	waitFor(t, "all split messages", 15*time.Second, func() bool { return srv.Store().Len() == 3*per })
	time.Sleep(500 * time.Millisecond)
	st2 := srv.MQTTStatus()
	if st2.MessagesReceived != 3*per {
		t.Fatalf("received %d messages for %d published", st2.MessagesReceived, 3*per)
	}
	t.Logf("messages per connection: %v", st2.ReceivedPerConnection)
	for i, c := range st2.ReceivedPerConnection {
		if c != per {
			t.Fatalf("connection %d received %d, want %d: %v", i, c, per, st2.ReceivedPerConnection)
		}
	}
}
