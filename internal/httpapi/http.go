// Package httpapi is the REST interface (for systems) over internal/core,
// plus the web UI, metrics and health endpoints. The MCP interface (for
// agents) lives in internal/mcp and is mounted at /mcp.
package httpapi

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
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/store"
)

//go:embed ui
var uiFS embed.FS

// Server serves the REST API, UI, metrics and (optionally) MCP.
type Server struct {
	svc *core.Service
	cfg config.Server
	mcp http.Handler
}

// New creates the HTTP interface. mcp may be nil (MCP disabled).
func New(svc *core.Service, mcp http.Handler) *Server {
	return &Server{svc: svc, cfg: svc.Config(), mcp: mcp}
}

type ctxKey struct{}

// principalFrom returns the authenticated caller.
func principalFrom(ctx context.Context) *core.Principal {
	p, _ := ctx.Value(ctxKey{}).(*core.Principal)
	return p
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

	mux.HandleFunc("GET /api/v1/tree", s.auth(read, s.handleTree))
	mux.HandleFunc("GET /api/v1/wait", s.auth(read, s.handleWait))

	if s.mcp != nil {
		for _, m := range []string{"GET", "POST", "DELETE"} {
			mux.Handle(m+" /mcp", s.mcp)
		}
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

// APIKey extracts the API key from a request (Authorization: Bearer,
// X-API-Key or, if allowed, ?api_key=).
func APIKey(r *http.Request, allowQuery bool) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if k, ok := strings.CutPrefix(h, "Bearer "); ok {
			return strings.TrimSpace(k)
		}
	}
	if k := r.Header.Get("X-API-Key"); k != "" {
		return k
	}
	if allowQuery {
		return r.URL.Query().Get("api_key")
	}
	return ""
}

// auth requires a valid API key with the given scope ("" = any valid key).
func (s *Server) auth(scope string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := s.svc.Authenticate(APIKey(r, s.cfg.AllowQueryAPIKey))
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="mqtt-get"`)
			writeError(w, err)
			return
		}
		if scope != "" {
			if err := core.Require(p, scope); err != nil {
				writeError(w, err)
				return
			}
		}
		next(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, p)))
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

// httpError is a transport-level error with an explicit status.
type httpError struct {
	status int
	msg    string
}

func (e *httpError) Error() string { return e.msg }

var kindStatus = map[core.Kind]int{
	core.Internal:        500,
	core.Invalid:         400,
	core.NotFound:        404,
	core.Conflict:        409,
	core.Unauthenticated: 401,
	core.Forbidden:       403,
	core.Unavailable:     503,
	core.Upstream:        502,
	core.Busy:            429,
}

func statusOf(err error) int {
	var he *httpError
	if errors.As(err, &he) {
		return he.status
	}
	return kindStatus[core.KindOf(err)]
}

func writeError(w http.ResponseWriter, err error) {
	writeJSON(w, statusOf(err), map[string]string{"error": err.Error()})
}

func badRequest(err error) error { return &httpError{400, err.Error()} }

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
	st := s.svc.MQTTStatus()
	code := http.StatusOK
	if st.Configured && !st.Connected {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{"ok": code == 200, "mqtt_connected": st.Connected, "mqtt_configured": st.Configured})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, s.svc.Status())
}

func (s *Server) handleWhoami(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	writeJSON(w, 200, map[string]any{"id": p.ID, "name": p.Name, "scopes": p.Scopes, "auth_disabled": s.cfg.AuthDisabled})
}

func (s *Server) handleValue(w http.ResponseWriter, r *http.Request) {
	e, err := s.svc.Latest(topicParam(r))
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
			writeError(w, &httpError{404, fmt.Sprintf("latest value for %q is older than %s", e.Topic, d)})
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
	res, total, err := s.svc.Query(r.URL.Query().Get("filter"), intParam(r, "limit", 1000))
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
	res, total, err := s.svc.Query(r.URL.Query().Get("filter"), intParam(r, "limit", 1000))
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
	if err := s.svc.Forget(topicParam(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleClearValues(w http.ResponseWriter, r *http.Request) {
	if t := r.URL.Query().Get("topic"); t != "" {
		s.handleDeleteValue(w, r)
		return
	}
	_ = s.svc.Forget("")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePublishRaw(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	qos, _ := strconv.Atoi(q.Get("qos"))
	retain, _ := strconv.ParseBool(q.Get("retain"))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		writeError(w, &httpError{413, err.Error()})
		return
	}
	req := core.PublishRequest{Topic: topicParam(r), QoS: byte(qos), Retain: retain}
	if qos < 0 || qos > 2 {
		writeError(w, badRequest(errors.New("qos must be 0, 1 or 2")))
		return
	}
	msg, err := req.Message()
	if err != nil {
		writeError(w, err)
		return
	}
	msg.Payload = body
	n, err := s.svc.Publish(r.Context(), []mqttc.Message{msg})
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
		writeError(w, &httpError{413, err.Error()})
		return
	}
	var reqs []core.PublishRequest
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "[") {
		err = json.Unmarshal(raw, &reqs)
	} else {
		var one core.PublishRequest
		err = json.Unmarshal(raw, &one)
		reqs = []core.PublishRequest{one}
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
		if msgs[i], err = p.Message(); err != nil {
			writeError(w, err)
			return
		}
	}
	n, err := s.svc.Publish(r.Context(), msgs)
	if err != nil {
		writeJSON(w, statusOf(err), map[string]any{"error": err.Error(), "published": n})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "published": n})
}

func (s *Server) handleWebhookList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"webhooks": s.svc.Webhooks()})
}

func (s *Server) handleWebhookGet(w http.ResponseWriter, r *http.Request) {
	v, err := s.svc.Webhook(r.PathValue("id"))
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
	v, err := s.svc.PutWebhook(wh, true)
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
	v, err := s.svc.PutWebhook(wh, false)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, v)
}

func (s *Server) handleWebhookDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.DeleteWebhook(r.PathValue("id")); err != nil {
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
		writeError(w, &httpError{413, err.Error()})
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
		p := core.PublishRequest{Topic: "x", Payload: body.Payload}
		m, err := p.Message()
		if err != nil {
			writeError(w, err)
			return
		}
		payload = m.Payload
	}
	if err := s.svc.TestWebhook(r.PathValue("id"), body.Topic, payload); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (s *Server) handleBrokerGet(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, s.svc.Broker())
}

func (s *Server) handleBrokerPut(w http.ResponseWriter, r *http.Request) {
	var b config.Broker
	if err := s.decode(w, r, &b); err != nil {
		writeError(w, err)
		return
	}
	if err := s.svc.SetBroker(b); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, s.svc.Broker())
}

func (s *Server) handleBrokerReconnect(w http.ResponseWriter, _ *http.Request) {
	if err := s.svc.Reconnect(); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, s.svc.Broker())
}

func (s *Server) handleKeyList(w http.ResponseWriter, _ *http.Request) {
	keys := s.svc.Keys()
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
	k, secret, err := s.svc.CreateKey(body.Name, body.Scopes)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"key": secret, "info": k,
		"note": "store this key now; it cannot be retrieved again"})
}

func (s *Server) handleKeyDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.DeleteKey(principalFrom(r.Context()), r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleTree(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tree, err := s.svc.TopicTree(q.Get("prefix"), intParam(r, "depth", 2), intParam(r, "max_children", 50))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, tree)
}

// handleWait long-polls for the next message matching ?filter= (up to
// ?timeout=30s, max 5m). 204 when nothing arrived in time.
func (s *Server) handleWait(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	timeout := core.DefaultWaitTimeout
	if v := q.Get("timeout"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			writeError(w, badRequest(fmt.Errorf("timeout: %w", err)))
			return
		}
		timeout = d
	}
	current, _ := strconv.ParseBool(q.Get("include_current"))
	e, err := s.svc.Wait(r.Context(), q.Get("filter"), timeout, current)
	if err != nil {
		writeError(w, err)
		return
	}
	if e == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.writeEntry(w, r, e)
}
