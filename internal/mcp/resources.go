package mcp

import (
	"encoding/json"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
	"github.com/tabreu8/mqtt-get/internal/topic"
)

// Resource URIs.
const (
	uriStatus      = "mqtt-get://status"
	uriTree        = "mqtt-get://topic-tree"
	uriBroker      = "mqtt-get://broker"
	topicPrefix    = "mqtt-get://topic/"
	valuesPrefix   = "mqtt-get://values"
	topicTemplate  = "mqtt-get://topic/{+topic}"
	valuesTemplate = "mqtt-get://values{?filter,limit}"
	resourcePage   = 100
)

// TopicURI returns the resource URI of a topic. Each level is escaped
// separately so the "/" structure stays readable.
func TopicURI(t string) string {
	levels := strings.Split(t, "/")
	for i, l := range levels {
		levels[i] = url.PathEscape(l)
	}
	return topicPrefix + strings.Join(levels, "/")
}

func topicFromURI(uri string) (string, bool) {
	rest, ok := strings.CutPrefix(uri, topicPrefix)
	if !ok {
		return "", false
	}
	levels := strings.Split(rest, "/")
	for i, l := range levels {
		u, err := url.PathUnescape(l)
		if err != nil {
			return "", false
		}
		levels[i] = u
	}
	return strings.Join(levels, "/"), true
}

// filterFromURI parses mqtt-get://values?filter=...&limit=...
func filterFromURI(uri string) (filter string, limit int, ok bool) {
	rest, found := strings.CutPrefix(uri, valuesPrefix)
	if !found || (rest != "" && rest[0] != '?') {
		return "", 0, false
	}
	q, err := url.ParseQuery(strings.TrimPrefix(rest, "?"))
	if err != nil {
		return "", 0, false
	}
	filter = q.Get("filter")
	if filter == "" {
		filter = "#"
	}
	limit, _ = strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	return filter, limit, true
}

type resource struct {
	URI         string         `json:"uri"`
	Name        string         `json:"name"`
	Title       string         `json:"title,omitempty"`
	Description string         `json:"description,omitempty"`
	MimeType    string         `json:"mimeType"`
	Size        int            `json:"size,omitempty"`
	Annotations map[string]any `json:"annotations,omitempty"`
}

func (s *Server) listResources(sess *session, params json.RawMessage) (any, *rpcError) {
	var p struct {
		Cursor string `json:"cursor"`
	}
	_ = json.Unmarshal(params, &p)
	if !sess.principal.Can(config.ScopeRead) {
		return map[string]any{"resources": []resource{}}, nil
	}
	var out []resource
	offset := 0
	if p.Cursor != "" {
		n, err := strconv.Atoi(p.Cursor)
		if err != nil || n < 0 {
			return nil, invalidParams("invalid cursor")
		}
		offset = n
	} else {
		out = append(out,
			resource{URI: uriStatus, Name: "status", Title: "Service status", MimeType: "application/json",
				Description: "Broker connection state and counters"},
			resource{URI: uriTree, Name: "topic-tree", Title: "Topic tree", MimeType: "application/json",
				Description: "The top two levels of the topic namespace with topic counts"})
		if sess.principal.Can(config.ScopeAdmin) {
			out = append(out, resource{URI: uriBroker, Name: "broker", Title: "Broker settings", MimeType: "application/json",
				Description: "Broker connection settings (secrets redacted)"})
		}
	}
	// Topics, paginated in topic order. Offsets are simple and fine for a
	// namespace that changes slowly relative to paging.
	all, total := s.svc.Store().Query("#", offset+resourcePage)
	if offset < len(all) {
		for _, e := range all[offset:] {
			out = append(out, resource{
				URI: TopicURI(e.Topic), Name: e.Topic, MimeType: "application/json", Size: len(e.Payload),
				Annotations: map[string]any{"lastModified": time.Unix(0, e.Time).UTC().Format(time.RFC3339)},
			})
		}
	}
	res := map[string]any{"resources": out}
	if offset+resourcePage < total {
		res["nextCursor"] = strconv.Itoa(offset + resourcePage)
	}
	return res, nil
}

func (s *Server) listResourceTemplates(sess *session) any {
	if !sess.principal.Can(config.ScopeRead) {
		return map[string]any{"resourceTemplates": []any{}}
	}
	return map[string]any{"resourceTemplates": []map[string]any{
		{"uriTemplate": topicTemplate, "name": "topic", "title": "Latest value of a topic", "mimeType": "application/json",
			"description": "The most recent message on an MQTT topic. Subscribe to be notified when it changes."},
		{"uriTemplate": valuesTemplate, "name": "values", "title": "Latest values matching a filter", "mimeType": "application/json",
			"description": "Latest values of all topics matching an MQTT filter (URL-encode # as %23 and + as %2B). Subscribe to be notified when any of them changes."},
	}}
}

func (s *Server) readResource(sess *session, params json.RawMessage) (any, *rpcError) {
	var p struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.URI == "" {
		return nil, invalidParams("uri is required")
	}
	need := config.ScopeRead
	if p.URI == uriBroker {
		need = config.ScopeAdmin
	}
	if !sess.principal.Can(need) {
		return nil, &rpcError{Code: codeInvalidRequest, Message: "API key lacks the " + need + " scope"}
	}
	var v any
	switch {
	case p.URI == uriStatus:
		v = s.svc.Status()
	case p.URI == uriBroker:
		v = s.svc.Broker()
	case p.URI == uriTree:
		t, _ := s.svc.TopicTree("", 2, 100)
		v = t
	default:
		if name, ok := topicFromURI(p.URI); ok {
			e, err := s.svc.Latest(name)
			if err != nil {
				return nil, &rpcError{Code: codeNotFound, Message: err.Error(), Data: map[string]string{"uri": p.URI}}
			}
			return map[string]any{"contents": []map[string]any{topicContents(p.URI, e)}}, nil
		}
		if filter, limit, ok := filterFromURI(p.URI); ok {
			res, total, err := s.svc.Query(filter, limit)
			if err != nil {
				return nil, invalidParams(err.Error())
			}
			v = map[string]any{"filter": filter, "total": total, "values": viewsOf(res)}
		} else {
			return nil, &rpcError{Code: codeNotFound, Message: "unknown resource", Data: map[string]string{"uri": p.URI}}
		}
	}
	b, _ := json.Marshal(v)
	return map[string]any{"contents": []map[string]any{{"uri": p.URI, "mimeType": "application/json", "text": string(b)}}}, nil
}

// topicContents returns the agent-friendly JSON view of a topic's value.
func topicContents(uri string, e *store.Entry) map[string]any {
	b, _ := json.Marshal(viewOf(e))
	return map[string]any{"uri": uri, "mimeType": "application/json", "text": string(b)}
}

// --- subscriptions ---

func (s *Server) subscribe(sess *session, params json.RawMessage) (any, *rpcError) {
	var p struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.URI == "" {
		return nil, invalidParams("uri is required")
	}
	if !sess.canNotify() {
		return nil, &rpcError{Code: codeInvalidRequest,
			Message: "subscriptions need a session: initialize first and keep the Mcp-Session-Id (or use stdio)"}
	}
	if !sess.principal.Can(config.ScopeRead) {
		return nil, &rpcError{Code: codeInvalidRequest, Message: "API key lacks the read scope"}
	}
	var filter string
	if name, ok := topicFromURI(p.URI); ok {
		if err := topic.ValidateName(name); err != nil {
			return nil, invalidParams(err.Error())
		}
		filter = name
	} else if f, _, ok := filterFromURI(p.URI); ok {
		filter = f
	} else {
		return nil, invalidParams("only mqtt-get://topic/... and mqtt-get://values?filter=... resources can be subscribed")
	}
	sess.mu.Lock()
	_, exists := sess.subs[p.URI]
	sess.mu.Unlock()
	if exists {
		return map[string]any{}, nil
	}
	n := &notifier{sess: sess, uri: p.URI, interval: s.opts.NotifyInterval}
	cancel, err := s.svc.Watch(filter, n.fire)
	if err != nil {
		return nil, invalidParams(err.Error())
	}
	sess.mu.Lock()
	if sess.closed {
		sess.mu.Unlock()
		cancel()
		return nil, &rpcError{Code: codeInvalidRequest, Message: "session closed"}
	}
	sess.subs[p.URI] = func() { cancel(); n.stop() }
	sess.mu.Unlock()
	return map[string]any{}, nil
}

func (s *Server) unsubscribe(sess *session, params json.RawMessage) (any, *rpcError) {
	var p struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.URI == "" {
		return nil, invalidParams("uri is required")
	}
	sess.mu.Lock()
	cancel := sess.subs[p.URI]
	delete(sess.subs, p.URI)
	sess.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return map[string]any{}, nil
}

// notifier sends notifications/resources/updated for one subscription, at
// most once per interval. Bursts are coalesced so the agent still hears
// about the last change.
type notifier struct {
	sess     *session
	uri      string
	interval time.Duration

	mu      sync.Mutex
	last    time.Time
	timer   *time.Timer
	stopped bool
}

func (n *notifier) fire(*store.Entry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.timer != nil {
		return
	}
	if wait := n.interval - time.Since(n.last); wait > 0 {
		n.timer = time.AfterFunc(wait, func() {
			n.mu.Lock()
			n.timer = nil
			stopped := n.stopped
			n.last = time.Now()
			n.mu.Unlock()
			if !stopped {
				n.send()
			}
		})
		return
	}
	n.last = time.Now()
	go n.send() // never block the ingest path
}

func (n *notifier) send() {
	n.sess.notify("notifications/resources/updated", map[string]any{"uri": n.uri})
}

func (n *notifier) stop() {
	n.mu.Lock()
	n.stopped = true
	if n.timer != nil {
		n.timer.Stop()
	}
	n.mu.Unlock()
}

// --- completion ---

// complete suggests topic names for resource template and prompt arguments,
// one level at a time ("home/" -> "home/kitchen/", "home/lamp/", ...).
func (s *Server) complete(sess *session, params json.RawMessage) (any, *rpcError) {
	var p struct {
		Ref struct {
			Type string `json:"type"`
			URI  string `json:"uri"`
			Name string `json:"name"`
		} `json:"ref"`
		Argument struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"argument"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, invalidParams("invalid params")
	}
	empty := map[string]any{"completion": map[string]any{"values": []string{}, "hasMore": false}}
	if !sess.principal.Can(config.ScopeRead) {
		return empty, nil
	}
	switch p.Argument.Name {
	case "topic", "filter", "prefix", "command_topic", "state_topic":
	default:
		return empty, nil
	}
	v := p.Argument.Value
	if topic.HasWildcard(v) {
		return empty, nil
	}
	filter := "#"
	if pr := parentOf(v); pr != "" {
		filter = pr + "/#"
	}
	all, _ := s.svc.Store().Query(filter, 0)
	seen := map[string]bool{}
	for _, e := range all {
		if !strings.HasPrefix(e.Topic, v) {
			continue
		}
		cand := e.Topic
		if i := strings.IndexByte(e.Topic[len(v):], '/'); i >= 0 {
			cand = e.Topic[:len(v)+i+1]
		}
		seen[cand] = true
	}
	vals := make([]string, 0, len(seen))
	for c := range seen {
		vals = append(vals, c)
	}
	sort.Strings(vals)
	total := len(vals)
	if len(vals) > 100 {
		vals = vals[:100]
	}
	return map[string]any{"completion": map[string]any{"values": vals, "total": total, "hasMore": total > len(vals)}}, nil
}
