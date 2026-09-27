package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// A minimal Model Context Protocol server (Streamable HTTP transport, JSON
// responses only) exposing the service's features as tools.

var mcpVersions = []string{"2025-06-18", "2025-03-26", "2024-11-05"}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type mcpTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	scope       string
	call        func(ctx context.Context, s *Server, args json.RawMessage) (any, error)
}

func obj(props map[string]any, required ...string) map[string]any {
	m := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		m["required"] = required
	}
	return m
}

func str(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }
func num(desc string) map[string]any { return map[string]any{"type": "integer", "description": desc} }

var (
	qosProp    = map[string]any{"type": "integer", "enum": []int{0, 1, 2}, "description": "MQTT QoS level"}
	filterProp = str("MQTT topic filter; wildcards + and # allowed. Default #")
	limitProp  = num("Maximum number of results (default 100)")
)

func entryView(e *store.Entry) json.RawMessage { return store.AppendJSON(nil, e) }

func unmarshalArgs(args json.RawMessage, v any) error {
	if len(args) == 0 || string(args) == "null" {
		return nil
	}
	if err := json.Unmarshal(args, v); err != nil {
		return badRequest(err)
	}
	return nil
}

var mcpTools = []*mcpTool{
	{
		Name:        "get_latest_value",
		Description: "Get the most recent message received on an exact MQTT topic (payload, qos, retained flag, timestamp).",
		InputSchema: obj(map[string]any{"topic": str("Exact MQTT topic name")}, "topic"),
		scope:       config.ScopeRead,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			var a struct{ Topic string }
			if err := unmarshalArgs(args, &a); err != nil {
				return nil, err
			}
			e, err := s.latest(a.Topic)
			if err != nil {
				return nil, err
			}
			return entryView(e), nil
		},
	},
	{
		Name:        "query_values",
		Description: "Get the latest values of all topics matching an MQTT filter (e.g. home/+/temperature).",
		InputSchema: obj(map[string]any{"filter": filterProp, "limit": limitProp}),
		scope:       config.ScopeRead,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			var a struct {
				Filter string
				Limit  int
			}
			if err := unmarshalArgs(args, &a); err != nil {
				return nil, err
			}
			if a.Limit <= 0 {
				a.Limit = 100
			}
			res, total, err := s.query(a.Filter, a.Limit)
			if err != nil {
				return nil, err
			}
			vals := make([]json.RawMessage, len(res))
			for i, e := range res {
				vals[i] = entryView(e)
			}
			return map[string]any{"total": total, "values": vals}, nil
		},
	},
	{
		Name:        "list_topics",
		Description: "List topic names that have a stored latest value, optionally filtered by an MQTT filter.",
		InputSchema: obj(map[string]any{"filter": filterProp, "limit": num("Maximum number of topics (default 500)")}),
		scope:       config.ScopeRead,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			var a struct {
				Filter string
				Limit  int
			}
			if err := unmarshalArgs(args, &a); err != nil {
				return nil, err
			}
			if a.Limit <= 0 {
				a.Limit = 500
			}
			res, total, err := s.query(a.Filter, a.Limit)
			if err != nil {
				return nil, err
			}
			names := make([]string, len(res))
			for i, e := range res {
				names[i] = e.Topic
			}
			return map[string]any{"total": total, "topics": names}, nil
		},
	},
	{
		Name:        "publish",
		Description: "Publish a message to an MQTT topic.",
		InputSchema: obj(map[string]any{
			"topic":    str("Topic name (no wildcards)"),
			"payload":  str("Message payload (text; use encoding=base64 for binary)"),
			"encoding": map[string]any{"type": "string", "enum": []string{"text", "base64"}},
			"qos":      qosProp,
			"retain":   map[string]any{"type": "boolean"},
		}, "topic"),
		scope: config.ScopePublish,
		call: func(ctx context.Context, s *Server, args json.RawMessage) (any, error) {
			var a struct {
				Topic, Payload, Encoding string
				QoS                      byte
				Retain                   bool
			}
			if err := unmarshalArgs(args, &a); err != nil {
				return nil, err
			}
			p, _ := json.Marshal(a.Payload)
			msg, err := publishRequest{Topic: a.Topic, Payload: p, Encoding: a.Encoding, QoS: a.QoS, Retain: a.Retain}.toMessage()
			if err != nil {
				return nil, err
			}
			if _, err := s.publish(ctx, []mqttc.Message{msg}); err != nil {
				return nil, err
			}
			return map[string]any{"ok": true}, nil
		},
	},
	{
		Name:        "get_status",
		Description: "Service status: broker connection state, message counters, number of topics and webhooks.",
		InputSchema: obj(map[string]any{}),
		scope:       config.ScopeRead,
		call: func(_ context.Context, s *Server, _ json.RawMessage) (any, error) {
			return s.status(), nil
		},
	},
	{
		Name:        "get_broker_config",
		Description: "Show the MQTT broker connection settings (secrets redacted) and connection status.",
		InputSchema: obj(map[string]any{}),
		scope:       config.ScopeAdmin,
		call: func(_ context.Context, s *Server, _ json.RawMessage) (any, error) {
			return s.brokerInfo(), nil
		},
	},
	{
		Name: "configure_broker",
		Description: "Change the MQTT broker connection. Only the fields given are changed; the service reconnects and persists the settings. " +
			"urls accepts tcp://, mqtt://, ssl://, mqtts://, ws://, wss:// URLs. Supports username/password, TLS with custom CA, " +
			"mutual TLS (tls.cert_pem + tls.key_pem), ALPN and websocket headers.",
		InputSchema: obj(map[string]any{
			"urls":             map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"client_id":        str("MQTT client id"),
			"username":         str("Username"),
			"password":         str("Password or token"),
			"protocol_version": map[string]any{"type": "integer", "enum": []int{3, 4}},
			"clean_session":    map[string]any{"type": "boolean"},
			"keepalive_sec":    num("Keep-alive in seconds"),
			"subscriptions": map[string]any{"type": "array", "items": obj(map[string]any{
				"filter": str("Topic filter"), "qos": qosProp}, "filter")},
			"tls": obj(map[string]any{
				"enabled": map[string]any{"type": "boolean"}, "ca_pem": str("CA certificate PEM"),
				"cert_pem": str("Client certificate PEM"), "key_pem": str("Client key PEM"),
				"server_name": str("TLS server name"), "insecure_skip_verify": map[string]any{"type": "boolean"},
				"alpn": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			}),
			"ws_headers":   map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
			"connections":  num("Number of parallel broker connections"),
			"shared_group": str("Shared subscription group used when connections > 1"),
		}),
		scope: config.ScopeAdmin,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			b := s.mqtt.Config() // overlay the given fields on the current config
			if err := unmarshalArgs(args, &b); err != nil {
				return nil, err
			}
			if err := s.setBroker(b); err != nil {
				return nil, err
			}
			return s.brokerInfo(), nil
		},
	},
	{
		Name:        "list_webhooks",
		Description: "List webhooks with delivery statistics.",
		InputSchema: obj(map[string]any{}),
		scope:       config.ScopeAdmin,
		call: func(_ context.Context, s *Server, _ json.RawMessage) (any, error) {
			return map[string]any{"webhooks": s.webhookList()}, nil
		},
	},
	{
		Name:        "create_webhook",
		Description: "Create a webhook that POSTs every message matching the topic filters to a URL. Use batch_size > 1 for high-volume topics.",
		InputSchema: webhookSchema(false),
		scope:       config.ScopeAdmin,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			var w config.Webhook
			if err := unmarshalArgs(args, &w); err != nil {
				return nil, err
			}
			return s.webhookPut(w, true)
		},
	},
	{
		Name:        "update_webhook",
		Description: "Update fields of an existing webhook.",
		InputSchema: webhookSchema(true),
		scope:       config.ScopeAdmin,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			var a struct{ ID string }
			if err := unmarshalArgs(args, &a); err != nil {
				return nil, err
			}
			w, err := s.state.Webhook(a.ID)
			if err != nil {
				return nil, notFound("webhook %q not found", a.ID)
			}
			if err := unmarshalArgs(args, &w); err != nil {
				return nil, err
			}
			w.ID = a.ID
			return s.webhookPut(w, false)
		},
	},
	{
		Name:        "delete_webhook",
		Description: "Delete a webhook.",
		InputSchema: obj(map[string]any{"id": str("Webhook id")}, "id"),
		scope:       config.ScopeAdmin,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			var a struct{ ID string }
			if err := unmarshalArgs(args, &a); err != nil {
				return nil, err
			}
			return map[string]any{"ok": true}, s.webhookDelete(a.ID)
		},
	},
	{
		Name:        "test_webhook",
		Description: "Send a test message to a webhook and report whether it succeeded.",
		InputSchema: obj(map[string]any{"id": str("Webhook id"), "topic": str("Optional topic"), "payload": str("Optional payload")}, "id"),
		scope:       config.ScopeAdmin,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			var a struct{ ID, Topic, Payload string }
			if err := unmarshalArgs(args, &a); err != nil {
				return nil, err
			}
			var p []byte
			if a.Payload != "" {
				p = []byte(a.Payload)
			}
			return map[string]any{"ok": true}, s.webhookTest(a.ID, a.Topic, p)
		},
	},
	{
		Name:        "create_api_key",
		Description: "Create a REST API key. Scopes: read (latest values), publish, admin (configuration). The key is shown only once.",
		InputSchema: obj(map[string]any{
			"name":   str("Descriptive name"),
			"scopes": map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"read", "publish", "admin"}}},
		}, "name"),
		scope: config.ScopeAdmin,
		call: func(_ context.Context, s *Server, args json.RawMessage) (any, error) {
			var a struct {
				Name   string
				Scopes []string
			}
			if err := unmarshalArgs(args, &a); err != nil {
				return nil, err
			}
			k, secret, err := s.state.CreateKey(a.Name, a.Scopes)
			if err != nil {
				return nil, badRequest(err)
			}
			return map[string]any{"key": secret, "info": k}, nil
		},
	},
}

func webhookSchema(update bool) map[string]any {
	props := map[string]any{
		"name":              str("Descriptive name"),
		"url":               str("Target http(s) URL"),
		"topics":            map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "MQTT topic filters"},
		"method":            map[string]any{"type": "string", "enum": []string{"POST", "PUT", "PATCH"}},
		"headers":           map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
		"secret":            str("HMAC-SHA256 signing secret (X-MQTT-Get-Signature header)"),
		"format":            map[string]any{"type": "string", "enum": []string{"json", "raw"}},
		"batch_size":        num("Messages per request (json format); 1 = no batching"),
		"batch_interval_ms": num("Maximum time to wait for a batch to fill"),
		"timeout_ms":        num("Request timeout"),
		"max_retries":       num("Retries on network errors / 5xx / 429"),
		"queue_size":        num("Per-webhook buffer; messages are dropped when full"),
		"concurrency":       num("Parallel requests"),
		"enabled":           map[string]any{"type": "boolean"},
	}
	if update {
		props["id"] = str("Webhook id")
		return obj(props, "id")
	}
	props["id"] = str("Optional id (generated if empty)")
	return obj(props, "url", "topics")
}

func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	key := keyFrom(r.Context())
	trimmed := strings.TrimSpace(string(body))
	if strings.HasPrefix(trimmed, "[") {
		var reqs []rpcRequest
		if err := json.Unmarshal(body, &reqs); err != nil {
			writeJSON(w, 200, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{-32700, "parse error"}})
			return
		}
		var out []rpcResponse
		for _, req := range reqs {
			if resp, ok := s.mcpDispatch(r.Context(), key, req); ok {
				out = append(out, resp)
			}
		}
		if len(out) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		writeJSON(w, 200, out)
		return
	}
	var req rpcRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, 200, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{-32700, "parse error"}})
		return
	}
	resp, ok := s.mcpDispatch(r.Context(), key, req)
	if !ok {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, 200, resp)
}

func allowed(key *config.APIKey, scope string) bool {
	return key == nil || state.HasScope(key, scope)
}

// mcpDispatch handles one JSON-RPC message. ok is false for notifications.
func (s *Server) mcpDispatch(ctx context.Context, key *config.APIKey, req rpcRequest) (rpcResponse, bool) {
	if len(req.ID) == 0 {
		return rpcResponse{}, false // notification (e.g. notifications/initialized)
	}
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		v := mcpVersions[0]
		for _, sv := range mcpVersions {
			if sv == p.ProtocolVersion {
				v = sv
			}
		}
		resp.Result = map[string]any{
			"protocolVersion": v,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "mqtt-get", "version": Version},
			"instructions": "mqtt-get bridges an MQTT broker to HTTP. Use get_latest_value / query_values to read the most recent " +
				"message of topics, publish to send messages, configure_broker to set up the broker connection and " +
				"create_webhook to forward messages to HTTP endpoints.",
		}
	case "ping":
		resp.Result = map[string]any{}
	case "tools/list":
		tools := make([]*mcpTool, 0, len(mcpTools))
		for _, t := range mcpTools {
			if allowed(key, t.scope) {
				tools = append(tools, t)
			}
		}
		resp.Result = map[string]any{"tools": tools}
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			resp.Error = &rpcError{-32602, "invalid params"}
			break
		}
		var tool *mcpTool
		for _, t := range mcpTools {
			if t.Name == p.Name {
				tool = t
			}
		}
		if tool == nil {
			resp.Error = &rpcError{-32602, "unknown tool: " + p.Name}
			break
		}
		if !allowed(key, tool.scope) {
			resp.Result = toolResult(nil, errors.New("API key lacks the "+tool.scope+" scope"))
			break
		}
		resp.Result = toolResult(tool.call(ctx, s, p.Arguments))
	default:
		resp.Error = &rpcError{-32601, "method not found: " + req.Method}
	}
	return resp, true
}

func toolResult(v any, err error) map[string]any {
	if err != nil {
		return map[string]any{"isError": true, "content": []map[string]any{{"type": "text", "text": err.Error()}}}
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	return map[string]any{"content": []map[string]any{{"type": "text", "text": string(b)}}}
}
