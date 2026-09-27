package mcp

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tabreu8/mqtt-get/internal/core"
)

// AuthFunc authenticates an HTTP request.
type AuthFunc func(r *http.Request) (*core.Principal, error)

// HTTPHandler returns the Streamable HTTP transport:
//
//	POST   /mcp  JSON-RPC messages (single or batch), JSON responses
//	GET    /mcp  SSE stream of server notifications (needs Mcp-Session-Id)
//	DELETE /mcp  end the session
//
// A session (Mcp-Session-Id) is created on initialize. Requests without a
// session id are served statelessly (everything works except resource
// subscriptions).
func (s *Server) HTTPHandler(auth AuthFunc, maxBody int64) http.Handler {
	if maxBody <= 0 {
		maxBody = 4 << 20
	}
	return &httpTransport{s: s, auth: auth, maxBody: maxBody}
}

type httpTransport struct {
	s       *Server
	auth    AuthFunc
	maxBody int64
}

func jsonError(w http.ResponseWriter, status, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errResponse(nil, code, msg))
}

// originAllowed protects against DNS rebinding: browsers always send Origin,
// which must then be same-origin or explicitly allowed.
func (t *httpTransport) originAllowed(r *http.Request) bool {
	o := r.Header.Get("Origin")
	if o == "" {
		return true
	}
	if u, err := url.Parse(o); err == nil && u.Host == r.Host {
		return true
	}
	for _, a := range t.s.opts.AllowedOrigins {
		if a == "*" || strings.EqualFold(a, o) {
			return true
		}
	}
	return false
}

func (t *httpTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !t.originAllowed(r) {
		jsonError(w, http.StatusForbidden, codeInvalidRequest, "origin not allowed")
		return
	}
	if v := r.Header.Get("MCP-Protocol-Version"); v != "" && !supportedVersion(v) {
		jsonError(w, http.StatusBadRequest, codeInvalidRequest, "unsupported MCP-Protocol-Version "+v)
		return
	}
	p, err := t.auth(r)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Bearer realm="mqtt-get"`)
		jsonError(w, http.StatusUnauthorized, codeInvalidRequest, err.Error())
		return
	}
	switch r.Method {
	case http.MethodPost:
		t.post(w, r, p)
	case http.MethodGet:
		t.stream(w, r, p)
	case http.MethodDelete:
		t.delete(w, r, p)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		jsonError(w, http.StatusMethodNotAllowed, codeInvalidRequest, "method not allowed")
	}
}

// session resolves the Mcp-Session-Id header. ok is false if an error was
// written.
func (t *httpTransport) session(w http.ResponseWriter, r *http.Request, p *core.Principal) (*session, bool) {
	id := r.Header.Get("Mcp-Session-Id")
	if id == "" {
		return nil, true
	}
	sess := t.s.getSession(id)
	if sess == nil {
		jsonError(w, http.StatusNotFound, codeInvalidRequest, "unknown or expired session; initialize again")
		return nil, false
	}
	if sess.principal.ID != p.ID {
		jsonError(w, http.StatusForbidden, codeInvalidRequest, "session belongs to another API key")
		return nil, false
	}
	return sess, true
}

func (t *httpTransport) post(w http.ResponseWriter, r *http.Request, p *core.Principal) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, t.maxBody))
	if err != nil {
		jsonError(w, http.StatusRequestEntityTooLarge, codeInvalidRequest, err.Error())
		return
	}
	var reqs []*request
	batch := len(bytes.TrimSpace(body)) > 0 && bytes.TrimSpace(body)[0] == '['
	if batch {
		err = json.Unmarshal(body, &reqs)
	} else {
		var one request
		err = json.Unmarshal(body, &one)
		reqs = []*request{&one}
	}
	if err != nil || len(reqs) == 0 {
		jsonError(w, http.StatusBadRequest, codeParse, "parse error")
		return
	}
	sess, ok := t.session(w, r, p)
	if !ok {
		return
	}
	if sess == nil {
		isInit := false
		for _, q := range reqs {
			isInit = isInit || q.Method == "initialize"
		}
		sess = newSession(p, isInit)
		if isInit {
			t.s.addSession(sess)
			w.Header().Set("Mcp-Session-Id", sess.id)
		} else {
			defer sess.close() // stateless request
		}
	}
	var out []*response
	for _, q := range reqs {
		if resp := t.s.handle(r.Context(), sess, q); resp != nil {
			out = append(out, resp)
		}
	}
	if len(out) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if batch {
		_ = enc.Encode(out)
	} else {
		_ = enc.Encode(out[0])
	}
}

func (t *httpTransport) stream(w http.ResponseWriter, r *http.Request, p *core.Principal) {
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		w.Header().Set("Allow", "POST, DELETE")
		jsonError(w, http.StatusMethodNotAllowed, codeInvalidRequest, "GET requires Accept: text/event-stream")
		return
	}
	sess, ok := t.session(w, r, p)
	if !ok {
		return
	}
	if sess == nil {
		jsonError(w, http.StatusBadRequest, codeInvalidRequest, "Mcp-Session-Id header required")
		return
	}
	if !sess.streams.CompareAndSwap(0, 1) {
		jsonError(w, http.StatusConflict, codeInvalidRequest, "a stream is already open for this session")
		return
	}
	defer sess.streams.Store(0)
	fl, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, http.StatusInternalServerError, codeInternal, "streaming unsupported")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, ": mqtt-get notification stream\n\n")
	fl.Flush()
	keepalive := time.NewTicker(25 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-sess.done:
			return
		case <-keepalive.C:
			_, _ = io.WriteString(w, ": ping\n\n")
		case msg := <-sess.out:
			sess.touch()
			_, _ = io.WriteString(w, "event: message\ndata: ")
			_, _ = w.Write(msg)
			_, _ = io.WriteString(w, "\n\n")
		}
		fl.Flush()
	}
}

func (t *httpTransport) delete(w http.ResponseWriter, r *http.Request, p *core.Principal) {
	sess, ok := t.session(w, r, p)
	if !ok {
		return
	}
	if sess == nil {
		jsonError(w, http.StatusBadRequest, codeInvalidRequest, "Mcp-Session-Id header required")
		return
	}
	t.s.removeSession(sess.id)
	w.WriteHeader(http.StatusNoContent)
}
