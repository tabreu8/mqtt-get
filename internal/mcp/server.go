// Package mcp is the Model Context Protocol interface to mqtt-get, the
// counterpart of the REST API for AI agents. Both are thin layers over
// internal/core.
//
// Besides tools, it offers what agents need and REST clients don't: topic
// namespace discovery, waiting for events, request/response publishing,
// resources with live update notifications, prompts, argument completion
// and tool safety annotations. Two transports are provided: Streamable HTTP
// (mounted at /mcp, with sessions and an SSE stream for notifications) and
// stdio (for local agents, optionally running the whole service
// in-process).
package mcp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tabreu8/mqtt-get/internal/core"
)

// ProtocolVersions lists supported MCP revisions, newest first.
var ProtocolVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"}

func supportedVersion(v string) bool {
	for _, s := range ProtocolVersions {
		if s == v {
			return true
		}
	}
	return false
}

// JSON-RPC error codes.
const (
	codeParse          = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternal       = -32603
	codeNotFound       = -32002 // MCP: resource not found
)

// Options configures the MCP server.
type Options struct {
	// NotifyInterval is the minimum time between two update notifications
	// for the same subscribed resource (default 1s).
	NotifyInterval time.Duration
	// SessionIdleTimeout expires HTTP sessions without traffic (default 30m).
	SessionIdleTimeout time.Duration
	// AllowedOrigins are extra browser origins accepted by the HTTP
	// transport (same-origin and non-browser requests are always accepted).
	AllowedOrigins []string
}

// Server implements MCP on top of core.Service.
type Server struct {
	svc  *core.Service
	log  *slog.Logger
	opts Options

	mu       sync.Mutex
	sessions map[string]*session
	stop     chan struct{}
	stopOnce sync.Once
}

// New creates the MCP server.
func New(svc *core.Service, log *slog.Logger, opts Options) *Server {
	if opts.NotifyInterval <= 0 {
		opts.NotifyInterval = time.Second
	}
	if opts.SessionIdleTimeout <= 0 {
		opts.SessionIdleTimeout = 30 * time.Minute
	}
	s := &Server{svc: svc, log: log, opts: opts, sessions: map[string]*session{}, stop: make(chan struct{})}
	go s.janitor()
	return s
}

// Close ends all sessions.
func (s *Server) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, sess := range s.sessions {
		sess.close()
		delete(s.sessions, id)
	}
}

// Sessions returns the number of open stateful sessions.
func (s *Server) Sessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

func (s *Server) janitor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			cutoff := time.Now().Add(-s.opts.SessionIdleTimeout).UnixNano()
			s.mu.Lock()
			for id, sess := range s.sessions {
				if sess.streams.Load() == 0 && sess.lastSeen.Load() < cutoff {
					sess.close()
					delete(s.sessions, id)
				}
			}
			s.mu.Unlock()
		}
	}
}

// --- sessions ---

type session struct {
	id        string
	principal *core.Principal
	// out carries server-to-client messages (notifications). nil for
	// stateless HTTP requests, which cannot receive notifications.
	out      chan []byte
	dropped  atomic.Uint64
	lastSeen atomic.Int64
	streams  atomic.Int32

	mu       sync.Mutex
	version  string
	client   string
	subs     map[string]func()
	inflight map[string]context.CancelFunc
	closed   bool
	done     chan struct{}
}

func newSession(p *core.Principal, stateful bool) *session {
	sess := &session{
		principal: p,
		subs:      map[string]func(){},
		inflight:  map[string]context.CancelFunc{},
		done:      make(chan struct{}),
	}
	if stateful {
		var b [16]byte
		_, _ = rand.Read(b[:])
		sess.id = hex.EncodeToString(b[:])
		sess.out = make(chan []byte, 512)
	}
	sess.touch()
	return sess
}

func (s *Server) addSession(sess *session) {
	s.mu.Lock()
	s.sessions[sess.id] = sess
	s.mu.Unlock()
}

func (s *Server) getSession(id string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id]
}

func (s *Server) removeSession(id string) bool {
	s.mu.Lock()
	sess, ok := s.sessions[id]
	delete(s.sessions, id)
	s.mu.Unlock()
	if ok {
		sess.close()
	}
	return ok
}

func (sess *session) touch() { sess.lastSeen.Store(time.Now().UnixNano()) }

func (sess *session) canNotify() bool { return sess.out != nil }

// notify queues a notification without blocking; it is dropped when the
// client isn't reading.
func (sess *session) notify(method string, params any) {
	if sess.out == nil {
		return
	}
	b, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
	if err != nil {
		return
	}
	sess.mu.Lock()
	closed := sess.closed
	sess.mu.Unlock()
	if closed {
		return
	}
	select {
	case sess.out <- b:
	default:
		sess.dropped.Add(1)
	}
}

func (sess *session) close() {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.closed {
		return
	}
	sess.closed = true
	for uri, cancel := range sess.subs {
		cancel()
		delete(sess.subs, uri)
	}
	for id, cancel := range sess.inflight {
		cancel()
		delete(sess.inflight, id)
	}
	close(sess.done)
}

func (sess *session) track(id json.RawMessage, cancel context.CancelFunc) {
	sess.mu.Lock()
	sess.inflight[string(id)] = cancel
	sess.mu.Unlock()
}

func (sess *session) untrack(id json.RawMessage) {
	sess.mu.Lock()
	delete(sess.inflight, string(id))
	sess.mu.Unlock()
}

func (sess *session) cancel(id json.RawMessage) {
	sess.mu.Lock()
	c := sess.inflight[string(id)]
	sess.mu.Unlock()
	if c != nil {
		c()
	}
}

// --- JSON-RPC ---

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

func (r *request) isNotification() bool { return len(r.ID) == 0 }

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func errResponse(id json.RawMessage, code int, msg string) *response {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	return &response{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg}}
}

// handle processes one JSON-RPC message; it returns nil for notifications.
func (s *Server) handle(ctx context.Context, sess *session, req *request) *response {
	sess.touch()
	if req.Method == "" && !req.isNotification() && req.JSONRPC == "2.0" {
		return nil // a response to a server request; we send none, ignore it
	}
	if req.JSONRPC != "2.0" || req.Method == "" {
		if req.isNotification() {
			return nil
		}
		return errResponse(req.ID, codeInvalidRequest, "invalid JSON-RPC 2.0 request")
	}
	if req.isNotification() {
		s.handleNotification(sess, req)
		return nil
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sess.track(req.ID, cancel)
	defer sess.untrack(req.ID)

	result, rerr := s.dispatch(ctx, sess, req)
	if ctx.Err() != nil && parent.Err() == nil {
		// Cancelled by the client (notifications/cancelled): per the spec,
		// no response is sent for a cancelled request.
		return nil
	}
	if rerr != nil {
		return &response{JSONRPC: "2.0", ID: req.ID, Error: rerr}
	}
	return &response{JSONRPC: "2.0", ID: req.ID, Result: result}
}

func (s *Server) handleNotification(sess *session, req *request) {
	switch req.Method {
	case "notifications/cancelled":
		var p struct {
			RequestID json.RawMessage `json:"requestId"`
		}
		if json.Unmarshal(req.Params, &p) == nil && len(p.RequestID) > 0 {
			sess.cancel(p.RequestID)
		}
	case "notifications/initialized", "notifications/roots/list_changed":
	}
}

func (s *Server) dispatch(ctx context.Context, sess *session, req *request) (any, *rpcError) {
	switch req.Method {
	case "initialize":
		return s.initialize(sess, req.Params)
	case "ping":
		return map[string]any{}, nil
	case "tools/list":
		return s.listTools(sess), nil
	case "tools/call":
		return s.callTool(ctx, sess, req.Params)
	case "resources/list":
		return s.listResources(sess, req.Params)
	case "resources/templates/list":
		return s.listResourceTemplates(sess), nil
	case "resources/read":
		return s.readResource(sess, req.Params)
	case "resources/subscribe":
		return s.subscribe(sess, req.Params)
	case "resources/unsubscribe":
		return s.unsubscribe(sess, req.Params)
	case "prompts/list":
		return s.listPrompts(sess), nil
	case "prompts/get":
		return s.getPrompt(sess, req.Params)
	case "completion/complete":
		return s.complete(sess, req.Params)
	case "logging/setLevel":
		return map[string]any{}, nil
	}
	return nil, &rpcError{Code: codeMethodNotFound, Message: "method not found: " + req.Method}
}

const instructions = `mqtt-get gives you live access to an MQTT broker: the latest value of every topic, publishing, and waiting for events.

How to work with it:
1. Discover: call describe_topic_tree (start with no prefix, then drill down with prefix) or list_topics with an MQTT filter. Topic levels are separated by "/"; in filters "+" matches one level and "#" matches all remaining levels.
2. Read: get_value for one topic, get_values for several topics or a filter. Each value has age_seconds; treat old values as possibly stale (max_age_seconds makes that explicit).
3. React: wait_for_message blocks until a matching message arrives (use it to observe changes or events instead of polling).
4. Act: publish sends a message. To control a device and confirm the result, use publish_and_wait (publish a command, wait for the state/response topic). Publishing can change real equipment: confirm intent with the user before sending commands, and never guess command formats. Look at existing values on the device's topics first.
Payloads that are valid JSON are returned as JSON (encoding "json"); text as a string ("utf8"); binary as base64 ("base64").
Resources: mqtt-get://topic/{topic} is the latest value of a topic (subscribe to get notified when it changes); mqtt-get://values?filter={filter} is the latest values matching a filter.`

func (s *Server) initialize(sess *session, params json.RawMessage) (any, *rpcError) {
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}
	_ = json.Unmarshal(params, &p)
	v := ProtocolVersions[0]
	if supportedVersion(p.ProtocolVersion) {
		v = p.ProtocolVersion
	}
	sess.mu.Lock()
	sess.version = v
	sess.client = p.ClientInfo.Name + " " + p.ClientInfo.Version
	sess.mu.Unlock()
	s.log.Debug("mcp: initialize", "client", sess.client, "version", v, "principal", sess.principal.Name)
	return map[string]any{
		"protocolVersion": v,
		"capabilities": map[string]any{
			"tools":       map[string]any{"listChanged": false},
			"resources":   map[string]any{"subscribe": sess.canNotify(), "listChanged": false},
			"prompts":     map[string]any{"listChanged": false},
			"completions": map[string]any{},
		},
		"serverInfo": map[string]any{
			"name":    "mqtt-get",
			"title":   "mqtt-get: live MQTT data for agents",
			"version": core.Version,
		},
		"instructions": instructions,
	}, nil
}

func invalidParams(msg string) *rpcError { return &rpcError{Code: codeInvalidParams, Message: msg} }
