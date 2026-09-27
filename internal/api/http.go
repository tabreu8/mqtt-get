package api

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
)

//go:embed ui
var uiFS embed.FS

type ctxKey struct{}

// keyFrom returns the API key that authenticated the request (nil when auth
// is disabled).
func keyFrom(ctx context.Context) *config.APIKey {
	k, _ := ctx.Value(ctxKey{}).(*config.APIKey)
	return k
}

// Handler returns the HTTP handler for the whole service.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	read, pub, admin := config.ScopeRead, config.ScopePublish, config.ScopeAdmin

	mux.HandleFunc("GET /healthz", s.handleHealth)
	if s.cfg.MetricsPublic {
		mux.HandleFunc("GET /metrics", s.handleMetrics)
	} else {
		mux.HandleFunc("GET /metrics", s.auth(read, s.handleMetrics))
	}

	mux.HandleFunc("GET /api/v1/status", s.auth(read, s.handleStatus))
	mux.HandleFunc("GET /api/v1/whoami", s.auth("", s.handleWhoami))

	mux.HandleFunc("GET /api/v1/values", s.auth(read, s.handleValues))
	mux.HandleFunc("GET /api/v1/values/{topic...}", s.auth(read, s.handleValue))
	mux.HandleFunc("DELETE /api/v1/values", s.auth(admin, s.handleClearValues))
	mux.HandleFunc("DELETE /api/v1/values/{topic...}", s.auth(admin, s.handleDeleteValue))
	mux.HandleFunc("GET /api/v1/topics", s.auth(read, s.handleTopics))

	mux.HandleFunc("POST /api/v1/publish", s.auth(pub, s.handlePublishJSON))
	mux.HandleFunc("POST /api/v1/publish/{topic...}", s.auth(pub, s.handlePublishRaw))

	mux.HandleFunc("GET /api/v1/webhooks", s.auth(admin, s.handleWebhookList))
	mux.HandleFunc("POST /api/v1/webhooks", s.auth(admin, s.handleWebhookCreate))
	mux.HandleFunc("GET /api/v1/webhooks/{id}", s.auth(admin, s.handleWebhookGet))
	mux.HandleFunc("PUT /api/v1/webhooks/{id}", s.auth(admin, s.handleWebhookUpdate))
	mux.HandleFunc("DELETE /api/v1/webhooks/{id}", s.auth(admin, s.handleWebhookDelete))
	mux.HandleFunc("POST /api/v1/webhooks/{id}/test", s.auth(admin, s.handleWebhookTest))

	mux.HandleFunc("GET /api/v1/broker", s.auth(admin, s.handleBrokerGet))
	mux.HandleFunc("PUT /api/v1/broker", s.auth(admin, s.handleBrokerPut))
	mux.HandleFunc("POST /api/v1/broker/reconnect", s.auth(admin, s.handleBrokerReconnect))

	mux.HandleFunc("GET /api/v1/keys", s.auth(admin, s.handleKeyList))
	mux.HandleFunc("POST /api/v1/keys", s.auth(admin, s.handleKeyCreate))
	mux.HandleFunc("DELETE /api/v1/keys/{id}", s.auth(admin, s.handleKeyDelete))

	if s.cfg.MCPEnabled {
		mux.HandleFunc("POST /mcp", s.auth("", s.handleMCP))
		mux.HandleFunc("GET /mcp", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", "POST")
			http.Error(w, "SSE stream not supported; use POST", http.StatusMethodNotAllowed)
		})
	}

	if s.cfg.UIEnabled {
		sub, _ := fs.Sub(uiFS, "ui")
		mux.Handle("GET /", http.FileServerFS(sub))
	}

	var h http.Handler = mux
	if s.cfg.CORSOrigins != "" {
		h = s.cors(h)
	}
	return h
}

// --- middleware ---

func (s *Server) apiKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if k, ok := strings.CutPrefix(h, "Bearer "); ok {
			return strings.TrimSpace(k)
		}
	}
	if k := r.Header.Get("X-API-Key"); k != "" {
		return k
	}
	if s.cfg.AllowQueryAPIKey {
		return r.URL.Query().Get("api_key")
	}
	return ""
}

// auth requires a valid API key with the given scope ("" = any valid key).
func (s *Server) auth(scope string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.AuthDisabled {
			next(w, r)
			return
		}
		k, ok := s.state.Lookup(s.apiKey(r))
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="mqtt-get"`)
			writeError(w, &apiError{Status: 401, Msg: "missing or invalid API key"})
			return
		}
		if scope != "" && !state.HasScope(k, scope) {
			writeError(w, &apiError{Status: 403, Msg: "API key lacks the " + scope + " scope"})
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, k)))
	}
}

func (s *Server) cors(next http.Handler) http.Handler {
	allowed := map[string]bool{}
	all := false
	for _, o := range strings.Split(s.cfg.CORSOrigins, ",") {
		o = strings.TrimSpace(o)
		all = all || o == "*"
		allowed[o] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" && (all || allowed[o]) {
			w.Header().Set("Access-Control-Allow-Origin", o)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, X-API-Key, Content-Type, Mcp-Session-Id, Mcp-Protocol-Version")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Expose-Headers", "X-MQTT-Topic, X-MQTT-QoS, X-MQTT-Retained, X-MQTT-Timestamp")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// --- helpers ---

var bufPool = sync.Pool{New: func() any { b := make([]byte, 0, 1024); return &b }}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	var ae *apiError
	if !errors.As(err, &ae) {
		ae = &apiError{Status: 500, Msg: err.Error()}
	}
	writeJSON(w, ae.Status, map[string]string{"error": ae.Msg})
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return badRequest(fmt.Errorf("invalid JSON body: %w", err))
	}
	return nil
}

func topicParam(r *http.Request) string {
	if t := r.URL.Query().Get("topic"); t != "" {
		return t
	}
	return r.PathValue("topic")
}

func intParam(r *http.Request, name string, def int) int {
	if v := r.URL.Query().Get(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// --- handlers ---

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	st := s.mqtt.Status()
	code := http.StatusOK
	if st.Configured && !st.Connected {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{"ok": code == 200, "mqtt_connected": st.Connected, "mqtt_configured": st.Configured})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, s.status())
}

func (s *Server) handleWhoami(w http.ResponseWriter, r *http.Request) {
	k := keyFrom(r.Context())
	if k == nil {
		writeJSON(w, 200, map[string]any{"auth_disabled": true, "scopes": []string{config.ScopeAdmin}})
		return
	}
	writeJSON(w, 200, map[string]any{"id": k.ID, "name": k.Name, "scopes": k.Scopes})
}

func (s *Server) handleValue(w http.ResponseWriter, r *http.Request) {
	e, err := s.latest(topicParam(r))
	if err != nil {
		writeError(w, err)
		return
	}
	s.writeEntry(w, r, e)
}

func (s *Server) writeEntry(w http.ResponseWriter, r *http.Request, e *store.Entry) {
	if maxAge := r.URL.Query().Get("max_age"); maxAge != "" {
		d, err := time.ParseDuration(maxAge)
		if err != nil {
			writeError(w, badRequest(fmt.Errorf("max_age: %w", err)))
			return
		}
		if time.Since(time.Unix(0, e.Time)) > d {
			writeError(w, notFound("latest value for %q is older than %s", e.Topic, d))
			return
		}
	}
	h := w.Header()
	h.Set("X-MQTT-Topic", e.Topic)
	h.Set("X-MQTT-QoS", strconv.Itoa(int(e.QoS)))
	h.Set("X-MQTT-Retained", strconv.FormatBool(e.Retained))
	h.Set("X-MQTT-Timestamp", time.Unix(0, e.Time).UTC().Format(time.RFC3339Nano))
	h.Set("Cache-Control", "no-store")
	if r.URL.Query().Get("format") == "raw" {
		switch store.Encoding(e.Payload) {
		case store.EncJSON:
			h.Set("Content-Type", "application/json")
		case store.EncUTF8:
			h.Set("Content-Type", "text/plain; charset=utf-8")
		default:
			h.Set("Content-Type", "application/octet-stream")
		}
		h.Set("Content-Length", strconv.Itoa(len(e.Payload)))
		_, _ = w.Write(e.Payload)
		return
	}
	bp := bufPool.Get().(*[]byte)
	buf := store.AppendJSON((*bp)[:0], e)
	buf = append(buf, '\n')
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(buf)))
	_, _ = w.Write(buf)
	if cap(buf) <= 64<<10 {
		*bp = buf
		bufPool.Put(bp)
	}
}

func (s *Server) handleValues(w http.ResponseWriter, r *http.Request) {
	if t := r.URL.Query().Get("topic"); t != "" {
		s.handleValue(w, r)
		return
	}
	res, total, err := s.query(r.URL.Query().Get("filter"), intParam(r, "limit", 1000))
	if err != nil {
		writeError(w, err)
		return
	}
	buf := make([]byte, 0, 64+len(res)*128)
	buf = append(buf, `{"total":`...)
	buf = strconv.AppendInt(buf, int64(total), 10)
	buf = append(buf, `,"count":`...)
	buf = strconv.AppendInt(buf, int64(len(res)), 10)
	buf = append(buf, `,"values":[`...)
	for i, e := range res {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = store.AppendJSON(buf, e)
	}
	buf = append(buf, "]}\n"...)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(buf)
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	res, total, err := s.query(r.URL.Query().Get("filter"), intParam(r, "limit", 1000))
	if err != nil {
		writeError(w, err)
		return
	}
	type item struct {
		Topic     string    `json:"topic"`
		Size      int       `json:"size"`
		Timestamp time.Time `json:"timestamp"`
	}
	items := make([]item, len(res))
	for i, e := range res {
		items[i] = item{e.Topic, len(e.Payload), time.Unix(0, e.Time).UTC()}
	}
	writeJSON(w, 200, map[string]any{"total": total, "count": len(items), "topics": items})
}

func (s *Server) handleDeleteValue(w http.ResponseWriter, r *http.Request) {
	if !s.store.Delete(topicParam(r)) {
		writeError(w, notFound("topic not found"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleClearValues(w http.ResponseWriter, r *http.Request) {
	if t := r.URL.Query().Get("topic"); t != "" {
		s.handleDeleteValue(w, r)
		return
	}
	s.store.Clear()
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePublishRaw(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	qos, _ := strconv.Atoi(q.Get("qos"))
	retain, _ := strconv.ParseBool(q.Get("retain"))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		writeError(w, &apiError{Status: 413, Msg: err.Error()})
		return
	}
	req := publishRequest{Topic: topicParam(r), QoS: byte(qos), Retain: retain}
	if qos < 0 || qos > 2 {
		writeError(w, badRequest(errors.New("qos must be 0, 1 or 2")))
		return
	}
	msg, err := req.toMessage()
	if err != nil {
		writeError(w, err)
		return
	}
	msg.Payload = body
	n, err := s.publish(r.Context(), []mqttc.Message{msg})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "published": n})
}

func (s *Server) handlePublishJSON(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, &apiError{Status: 413, Msg: err.Error()})
		return
	}
	var reqs []publishRequest
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "[") {
		err = json.Unmarshal(raw, &reqs)
	} else {
		var one publishRequest
		err = json.Unmarshal(raw, &one)
		reqs = []publishRequest{one}
	}
	if err != nil {
		writeError(w, badRequest(fmt.Errorf("invalid JSON body: %w", err)))
		return
	}
	if len(reqs) == 0 {
		writeError(w, badRequest(errors.New("no messages")))
		return
	}
	msgs := make([]mqttc.Message, len(reqs))
	for i, p := range reqs {
		if msgs[i], err = p.toMessage(); err != nil {
			writeError(w, err)
			return
		}
	}
	n, err := s.publish(r.Context(), msgs)
	if err != nil {
		writeJSON(w, err.(*apiError).Status, map[string]any{"error": err.Error(), "published": n})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "published": n})
}

func (s *Server) handleWebhookList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"webhooks": s.webhookList()})
}

func (s *Server) handleWebhookGet(w http.ResponseWriter, r *http.Request) {
	v, err := s.webhookGet(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, v)
}

func (s *Server) handleWebhookCreate(w http.ResponseWriter, r *http.Request) {
	var wh config.Webhook
	if err := s.decode(w, r, &wh); err != nil {
		writeError(w, err)
		return
	}
	v, err := s.webhookPut(wh, true)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 201, v)
}

func (s *Server) handleWebhookUpdate(w http.ResponseWriter, r *http.Request) {
	var wh config.Webhook
	if err := s.decode(w, r, &wh); err != nil {
		writeError(w, err)
		return
	}
	wh.ID = r.PathValue("id")
	v, err := s.webhookPut(wh, false)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, v)
}

func (s *Server) handleWebhookDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.webhookDelete(r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleWebhookTest(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Topic   string          `json:"topic"`
		Payload json.RawMessage `json:"payload"`
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		writeError(w, &apiError{Status: 413, Msg: err.Error()})
		return
	}
	if len(strings.TrimSpace(string(raw))) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			writeError(w, badRequest(fmt.Errorf("invalid JSON body: %w", err)))
			return
		}
	}
	var payload []byte
	if len(body.Payload) > 0 {
		p := publishRequest{Topic: "x", Payload: body.Payload}
		m, err := p.toMessage()
		if err != nil {
			writeError(w, err)
			return
		}
		payload = m.Payload
	}
	if err := s.webhookTest(r.PathValue("id"), body.Topic, payload); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleBrokerGet(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, s.brokerInfo())
}

func (s *Server) handleBrokerPut(w http.ResponseWriter, r *http.Request) {
	var b config.Broker
	if err := s.decode(w, r, &b); err != nil {
		writeError(w, err)
		return
	}
	if err := s.setBroker(b); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, s.brokerInfo())
}

func (s *Server) handleBrokerReconnect(w http.ResponseWriter, _ *http.Request) {
	if err := s.mqtt.Reconnect(); err != nil {
		writeError(w, badRequest(err))
		return
	}
	writeJSON(w, 200, s.brokerInfo())
}

func (s *Server) handleKeyList(w http.ResponseWriter, _ *http.Request) {
	keys := s.state.Keys()
	sort.SliceStable(keys, func(i, j int) bool { return keys[i].CreatedAt.Before(keys[j].CreatedAt) })
	writeJSON(w, 200, map[string]any{"keys": keys})
}

func (s *Server) handleKeyCreate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name   string   `json:"name"`
		Scopes []string `json:"scopes"`
	}
	if err := s.decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	k, secret, err := s.state.CreateKey(body.Name, body.Scopes)
	if err != nil {
		writeError(w, badRequest(err))
		return
	}
	writeJSON(w, 201, map[string]any{"key": secret, "info": k,
		"note": "store this key now; it cannot be retrieved again"})
}

func (s *Server) handleKeyDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if k := keyFrom(r.Context()); k != nil && k.ID == id {
		writeError(w, badRequest(errors.New("refusing to delete the key used for this request")))
		return
	}
	if err := s.state.DeleteKey(id); err != nil {
		writeError(w, notFound("key %q not found (environment keys cannot be deleted)", id))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
