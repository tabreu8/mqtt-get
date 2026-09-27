package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/topic"
)

type call struct {
	svc       *core.Service
	principal *core.Principal
	sess      *session
}

type tool struct {
	Name        string         `json:"name"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations"`
	scope       string
	run         func(ctx context.Context, c *call, args json.RawMessage) (any, error)
}

// --- schema helpers ---

func object(props map[string]any, required ...string) map[string]any {
	m := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		m["required"] = required
	}
	return m
}

func str(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }

func integer(desc string, min, max int) map[string]any {
	return map[string]any{"type": "integer", "description": desc, "minimum": min, "maximum": max}
}

func boolean(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}

func strList(desc string) map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": desc}
}

func enum(desc string, values ...any) map[string]any {
	return map[string]any{"enum": values, "description": desc}
}

var (
	pQoS      = enum("MQTT QoS level (default 0)", 0, 1, 2)
	pFilter   = str(`MQTT topic filter; "+" matches one level, "#" all remaining levels (e.g. "home/+/temperature", "factory/#")`)
	pPayload  = map[string]any{"description": `Message payload. A string is sent as text; any other JSON value (object, number, boolean, array) is sent as its JSON encoding.`}
	pEncoding = enum(`How to interpret a string payload: "text" (default) or "base64" for binary data`, "text", "base64")
)

func annotations(title string, readOnly, destructive, idempotent, openWorld bool) map[string]any {
	a := map[string]any{"title": title, "readOnlyHint": readOnly, "openWorldHint": openWorld}
	if !readOnly {
		a["destructiveHint"] = destructive
		a["idempotentHint"] = idempotent
	}
	return a
}

// decodeArgs strictly decodes tool arguments so that mistakes (misspelled
// or invented parameters) come back as a helpful error instead of being
// silently ignored.
func decodeArgs(args json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(args)) == 0 || string(args) == "null" {
		args = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(args))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return core.Invalidf("invalid arguments: %s", strings.TrimPrefix(err.Error(), "json: "))
	}
	return nil
}

func seconds(v float64, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return time.Duration(v * float64(time.Second))
}

// --- tools ---

var tools = []*tool{
	{
		Name:        "get_status",
		Description: "Connection status of the MQTT broker, message counters and the number of known topics. Call this first if values seem missing or stale.",
		InputSchema: object(map[string]any{}),
		Annotations: annotations("Get service status", true, false, true, false),
		scope:       config.ScopeRead,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			if err := decodeArgs(args, &struct{}{}); err != nil {
				return nil, err
			}
			return c.svc.Status(), nil
		},
	},
	{
		Name: "describe_topic_tree",
		Description: "Summarize the MQTT topic namespace as a tree: each node has the number of topics below it and when it was last updated. " +
			"Start without a prefix to see the top levels, then drill down with prefix (e.g. \"factory/line1\"). Best first step to discover what data exists.",
		InputSchema: object(map[string]any{
			"prefix":       str(`Topic prefix to start from, without wildcards (e.g. "home" or "factory/line1"). Omit for the root.`),
			"depth":        integer("How many levels below prefix to show (default 2)", 1, 10),
			"max_children": integer("Maximum children listed per node (default 50)", 1, 500),
		}),
		Annotations: annotations("Describe topic tree", true, false, true, false),
		scope:       config.ScopeRead,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Prefix      string `json:"prefix"`
				Depth       int    `json:"depth"`
				MaxChildren int    `json:"max_children"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			return c.svc.TopicTree(a.Prefix, a.Depth, a.MaxChildren)
		},
	},
	{
		Name:        "list_topics",
		Description: "List topic names that have a value, optionally matching an MQTT filter, with each topic's age and payload size (no payloads).",
		InputSchema: object(map[string]any{
			"filter": pFilter,
			"limit":  integer("Maximum topics to return (default 200)", 1, 5000),
		}),
		Annotations: annotations("List topics", true, false, true, false),
		scope:       config.ScopeRead,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Filter string `json:"filter"`
				Limit  int    `json:"limit"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if a.Limit <= 0 {
				a.Limit = 200
			}
			res, total, err := c.svc.Query(a.Filter, a.Limit)
			if err != nil {
				return nil, err
			}
			type item struct {
				Topic        string  `json:"topic"`
				AgeSeconds   float64 `json:"age_seconds"`
				PayloadBytes int     `json:"payload_bytes"`
			}
			items := make([]item, len(res))
			for i, e := range res {
				v := viewOf(e)
				items[i] = item{e.Topic, v.AgeSeconds, len(e.Payload)}
			}
			return map[string]any{"total": total, "returned": len(items), "topics": items}, nil
		},
	},
	{
		Name:        "get_value",
		Description: "Get the most recent message received on one exact topic (no wildcards): payload, QoS, retained flag, timestamp and age.",
		InputSchema: object(map[string]any{
			"topic":           str(`Exact topic name, e.g. "home/kitchen/temperature"`),
			"max_age_seconds": map[string]any{"type": "number", "description": "Fail if the latest value is older than this many seconds (detects stale or offline sensors)"},
		}, "topic"),
		Annotations: annotations("Get latest value", true, false, true, false),
		scope:       config.ScopeRead,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Topic         string  `json:"topic"`
				MaxAgeSeconds float64 `json:"max_age_seconds"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if topic.HasWildcard(a.Topic) {
				return nil, core.Invalidf("topic %q contains wildcards; use get_values with filter instead", a.Topic)
			}
			e, err := c.svc.Latest(a.Topic)
			if err != nil {
				if core.KindOf(err) == core.NotFound {
					return nil, errors.New(err.Error() + suggest(c.svc, a.Topic))
				}
				return nil, err
			}
			v := viewOf(e)
			if age := time.Since(time.Unix(0, e.Time)).Seconds(); a.MaxAgeSeconds > 0 && age > a.MaxAgeSeconds {
				return nil, fmt.Errorf("latest value of %q is %.1fs old, older than max_age_seconds=%g (the publisher may be offline)", a.Topic, age, a.MaxAgeSeconds)
			}
			return v, nil
		},
	},
	{
		Name:        "get_values",
		Description: "Get the latest values of several topics at once: either a list of exact topics, or all topics matching an MQTT filter.",
		InputSchema: object(map[string]any{
			"topics": strList("Exact topic names (use this or filter)"),
			"filter": pFilter,
			"limit":  integer("Maximum values to return for a filter (default 100)", 1, 1000),
		}),
		Annotations: annotations("Get latest values", true, false, true, false),
		scope:       config.ScopeRead,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Topics []string `json:"topics"`
				Filter string   `json:"filter"`
				Limit  int      `json:"limit"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if (len(a.Topics) == 0) == (a.Filter == "") {
				return nil, core.Invalidf("give exactly one of topics or filter")
			}
			if a.Filter != "" {
				if a.Limit <= 0 {
					a.Limit = 100
				}
				res, total, err := c.svc.Query(a.Filter, a.Limit)
				if err != nil {
					return nil, err
				}
				return map[string]any{"total": total, "returned": len(res), "values": viewsOf(res)}, nil
			}
			if len(a.Topics) > 1000 {
				return nil, core.Invalidf("at most 1000 topics per call")
			}
			var vals []value
			missing := []string{}
			for _, t := range a.Topics {
				e, err := c.svc.Latest(t)
				if err != nil {
					missing = append(missing, t)
					continue
				}
				vals = append(vals, viewOf(e))
			}
			return map[string]any{"values": vals, "missing": missing}, nil
		},
	},
	{
		Name: "wait_for_message",
		Description: "Wait until a message arrives on a topic matching the filter and return it (or received=false after timeout_seconds). " +
			"Use it to observe changes and events instead of polling get_value.",
		InputSchema: object(map[string]any{
			"filter":          pFilter,
			"timeout_seconds": map[string]any{"type": "number", "description": "How long to wait (default 30, max 300)"},
			"include_current": boolean("Return the newest already-stored matching value immediately if there is one (default false: only new messages)"),
		}, "filter"),
		Annotations: annotations("Wait for a message", true, false, false, false),
		scope:       config.ScopeRead,
		run: func(ctx context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Filter         string  `json:"filter"`
				TimeoutSeconds float64 `json:"timeout_seconds"`
				IncludeCurrent bool    `json:"include_current"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			timeout := seconds(a.TimeoutSeconds, core.DefaultWaitTimeout)
			e, err := c.svc.Wait(ctx, a.Filter, timeout, a.IncludeCurrent)
			if err != nil {
				return nil, err
			}
			if e == nil {
				return map[string]any{"received": false, "waited_seconds": timeout.Seconds()}, nil
			}
			return map[string]any{"received": true, "message": viewOf(e)}, nil
		},
	},
	{
		Name: "publish",
		Description: "Publish a message to an MQTT topic. This can control real devices: confirm with the user first, and check the device's existing topics for the expected payload format. " +
			"To confirm the device reacted, use publish_and_wait instead.",
		InputSchema: object(map[string]any{
			"topic":    str("Topic name to publish to (no wildcards)"),
			"payload":  pPayload,
			"encoding": pEncoding,
			"qos":      pQoS,
			"retain":   boolean("Ask the broker to retain the message as the topic's last known value (default false)"),
		}, "topic", "payload"),
		Annotations: annotations("Publish a message", false, true, false, true),
		scope:       config.ScopePublish,
		run: func(ctx context.Context, c *call, args json.RawMessage) (any, error) {
			var a publishArgs
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			msg, err := a.message()
			if err != nil {
				return nil, err
			}
			if _, err := c.svc.Publish(ctx, []mqttc.Message{msg}); err != nil {
				return nil, err
			}
			return map[string]any{"published": true, "topic": msg.Topic, "payload_bytes": len(msg.Payload), "qos": msg.QoS, "retain": msg.Retain}, nil
		},
	},
	{
		Name: "publish_and_wait",
		Description: "Publish a command and wait for the response or state change, e.g. publish {\"state\":\"ON\"} to home/lamp/set and wait on home/lamp/state. " +
			"Returns the first message matching response_filter (messages on the command topic itself are ignored). Confirm with the user before controlling devices.",
		InputSchema: object(map[string]any{
			"topic":           str("Command topic to publish to"),
			"payload":         pPayload,
			"encoding":        pEncoding,
			"qos":             pQoS,
			"retain":          boolean("Retain the command message (default false)"),
			"response_filter": str("Topic or filter where the response/state is published"),
			"timeout_seconds": map[string]any{"type": "number", "description": "How long to wait for the response (default 10, max 300)"},
		}, "topic", "payload", "response_filter"),
		Annotations: annotations("Publish and wait for response", false, true, false, true),
		scope:       config.ScopePublish,
		run: func(ctx context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				publishArgs
				ResponseFilter string  `json:"response_filter"`
				TimeoutSeconds float64 `json:"timeout_seconds"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			msg, err := a.message()
			if err != nil {
				return nil, err
			}
			timeout := seconds(a.TimeoutSeconds, 10*time.Second)
			start := time.Now()
			e, err := c.svc.PublishAndWait(ctx, msg, a.ResponseFilter, timeout)
			if err != nil {
				return nil, err
			}
			if e == nil {
				return map[string]any{"published": true, "response_received": false, "waited_seconds": timeout.Seconds(),
					"hint": "no message on " + a.ResponseFilter + "; the device may be offline, use another response topic, or need a different payload"}, nil
			}
			return map[string]any{"published": true, "response_received": true, "response_after_ms": time.Since(start).Milliseconds(), "response": viewOf(e)}, nil
		},
	},
	// --- administration ---
	{
		Name:        "get_broker_config",
		Description: "Show the MQTT broker connection settings (secrets redacted) and connection status.",
		InputSchema: object(map[string]any{}),
		Annotations: annotations("Get broker settings", true, false, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			if err := decodeArgs(args, &struct{}{}); err != nil {
				return nil, err
			}
			return c.svc.Broker(), nil
		},
	},
	{
		Name: "configure_broker",
		Description: "Change the MQTT broker connection. Only the given fields change; the service reconnects immediately and saves the settings. " +
			"urls accept tcp://, mqtt://, ssl://, mqtts://, ws://, wss://. Auth: username/password (or a token as password), TLS with a custom CA (tls.ca_pem), mutual TLS (tls.cert_pem + tls.key_pem).",
		InputSchema: object(map[string]any{
			"urls":             strList("Broker URLs (failover order)"),
			"client_id":        str("MQTT client id"),
			"username":         str("Username"),
			"password":         str("Password or token"),
			"password_file":    str("File to read the password from on every connect"),
			"protocol_version": enum("4 = MQTT 3.1.1, 3 = MQTT 3.1", 3, 4),
			"clean_session":    boolean("Clean session (default true)"),
			"keepalive_sec":    integer("Keep-alive in seconds", 1, 3600),
			"subscriptions": map[string]any{"type": "array", "description": "Filters to subscribe to", "items": object(map[string]any{
				"filter": str("Topic filter"), "qos": pQoS}, "filter")},
			"tls": object(map[string]any{
				"enabled": boolean("Use TLS for tcp:// URLs"), "ca_pem": str("CA certificate (PEM)"), "ca_file": str("CA file path"),
				"cert_pem": str("Client certificate (PEM)"), "cert_file": str("Client certificate path"),
				"key_pem": str("Client private key (PEM)"), "key_file": str("Client key path"),
				"server_name": str("TLS server name"), "insecure_skip_verify": boolean("Skip verification (testing only)"),
				"alpn": strList("ALPN protocols"),
			}),
			"ws_headers":   map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}, "description": "WebSocket HTTP headers"},
			"connections":  integer("Parallel broker connections", 1, 64),
			"shared_group": str("Shared-subscription group spreading ingest over connections"),
		}),
		Annotations: annotations("Configure broker connection", false, true, true, true),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			b := c.svc.BrokerConfig()
			// Overlay only the given fields; unknown fields are rejected.
			if err := decodeArgs(args, &b); err != nil {
				return nil, err
			}
			if err := c.svc.SetBroker(b); err != nil {
				return nil, err
			}
			return c.svc.Broker(), nil
		},
	},
	{
		Name:        "reconnect_broker",
		Description: "Drop and re-establish the broker connection (e.g. after fixing the broker side).",
		InputSchema: object(map[string]any{}),
		Annotations: annotations("Reconnect to broker", false, false, true, true),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			if err := decodeArgs(args, &struct{}{}); err != nil {
				return nil, err
			}
			if err := c.svc.Reconnect(); err != nil {
				return nil, err
			}
			return c.svc.Broker(), nil
		},
	},
	{
		Name:        "list_webhooks",
		Description: "List webhooks with their topic filters and delivery statistics (delivered, failed, dropped, last error).",
		InputSchema: object(map[string]any{}),
		Annotations: annotations("List webhooks", true, false, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			if err := decodeArgs(args, &struct{}{}); err != nil {
				return nil, err
			}
			return map[string]any{"webhooks": c.svc.Webhooks()}, nil
		},
	},
	{
		Name:        "create_webhook",
		Description: "Create a webhook that POSTs every message matching the topic filters to a URL. For high-volume topics set batch_size > 1.",
		InputSchema: webhookSchema(false),
		Annotations: annotations("Create webhook", false, false, false, true),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var w config.Webhook
			if err := decodeArgs(args, &w); err != nil {
				return nil, err
			}
			return c.svc.PutWebhook(w, true)
		},
	},
	{
		Name:        "update_webhook",
		Description: "Change fields of an existing webhook (only the given fields change).",
		InputSchema: webhookSchema(true),
		Annotations: annotations("Update webhook", false, false, true, true),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var id struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(args, &id)
			w, err := c.svc.WebhookConfig(id.ID)
			if err != nil {
				return nil, err
			}
			if err := decodeArgs(args, &w); err != nil {
				return nil, err
			}
			w.ID = id.ID
			return c.svc.PutWebhook(w, false)
		},
	},
	{
		Name:        "delete_webhook",
		Description: "Delete a webhook.",
		InputSchema: object(map[string]any{"id": str("Webhook id")}, "id"),
		Annotations: annotations("Delete webhook", false, true, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				ID string `json:"id"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if err := c.svc.DeleteWebhook(a.ID); err != nil {
				return nil, err
			}
			return map[string]any{"deleted": a.ID}, nil
		},
	},
	{
		Name:        "test_webhook",
		Description: "Send a test message to a webhook now and report whether the endpoint accepted it.",
		InputSchema: object(map[string]any{
			"id": str("Webhook id"), "topic": str("Optional topic for the test message"), "payload": pPayload,
		}, "id"),
		Annotations: annotations("Test webhook", false, false, false, true),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				ID      string          `json:"id"`
				Topic   string          `json:"topic"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			var payload []byte
			if len(a.Payload) > 0 {
				m, err := core.PublishRequest{Topic: "x", Payload: a.Payload}.Message()
				if err != nil {
					return nil, err
				}
				payload = m.Payload
			}
			if err := c.svc.TestWebhook(a.ID, a.Topic, payload); err != nil {
				return nil, err
			}
			return map[string]any{"delivered": true}, nil
		},
	},
	{
		Name:        "list_api_keys",
		Description: "List API keys (name, prefix and scopes; never the secret).",
		InputSchema: object(map[string]any{}),
		Annotations: annotations("List API keys", true, false, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			if err := decodeArgs(args, &struct{}{}); err != nil {
				return nil, err
			}
			keys := c.svc.Keys()
			sort.SliceStable(keys, func(i, j int) bool { return keys[i].CreatedAt.Before(keys[j].CreatedAt) })
			return map[string]any{"keys": keys}, nil
		},
	},
	{
		Name:        "create_api_key",
		Description: "Create an API key for the REST API or MCP. Scopes: read (values), publish, admin (configuration). The secret is shown only once: give it to the user.",
		InputSchema: object(map[string]any{
			"name":   str("Descriptive name, e.g. \"grafana\""),
			"scopes": map[string]any{"type": "array", "items": map[string]any{"enum": []string{"read", "publish", "admin"}}, "description": "Default [\"read\"]"},
		}, "name"),
		Annotations: annotations("Create API key", false, false, false, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Name   string   `json:"name"`
				Scopes []string `json:"scopes"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			k, secret, err := c.svc.CreateKey(a.Name, a.Scopes)
			if err != nil {
				return nil, err
			}
			return map[string]any{"key": secret, "info": k, "note": "shown only once"}, nil
		},
	},
	{
		Name:        "revoke_api_key",
		Description: "Revoke an API key by id (keys from environment variables cannot be revoked).",
		InputSchema: object(map[string]any{"id": str("Key id from list_api_keys")}, "id"),
		Annotations: annotations("Revoke API key", false, true, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				ID string `json:"id"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if err := c.svc.DeleteKey(c.principal, a.ID); err != nil {
				return nil, err
			}
			return map[string]any{"revoked": a.ID}, nil
		},
	},
}

func init() {
	for _, t := range tools {
		t.Title, _ = t.Annotations["title"].(string)
	}
}

type publishArgs struct {
	Topic    string          `json:"topic"`
	Payload  json.RawMessage `json:"payload"`
	Encoding string          `json:"encoding"`
	QoS      byte            `json:"qos"`
	Retain   bool            `json:"retain"`
}

func (a publishArgs) message() (mqttc.Message, error) {
	if len(a.Payload) == 0 {
		return mqttc.Message{}, core.Invalidf("payload is required (use \"\" for an empty message)")
	}
	return core.PublishRequest{Topic: a.Topic, Payload: a.Payload, Encoding: a.Encoding, QoS: a.QoS, Retain: a.Retain}.Message()
}

func webhookSchema(update bool) map[string]any {
	props := map[string]any{
		"name":              str("Descriptive name"),
		"url":               str("Target http(s) URL"),
		"topics":            strList("MQTT topic filters to forward"),
		"method":            enum("HTTP method (default POST)", "POST", "PUT", "PATCH"),
		"headers":           map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}, "description": "Extra HTTP headers"},
		"secret":            str("HMAC-SHA256 signing secret (X-MQTT-Get-Signature header)"),
		"format":            enum(`"json" envelope (default) or "raw" payload body`, "json", "raw"),
		"batch_size":        integer("Messages per request, json format (default 1)", 1, 10000),
		"batch_interval_ms": integer("Maximum wait for a batch to fill (default 200)", 1, 60000),
		"timeout_ms":        integer("Request timeout (default 5000)", 1, 120000),
		"max_retries":       integer("Retries on network errors, 5xx and 429 (default 3)", 0, 20),
		"queue_size":        integer("Per-webhook buffer; messages are dropped when full (default 10000)", 1, 10000000),
		"concurrency":       integer("Parallel requests (default 1)", 1, 256),
		"enabled":           boolean("Enabled (default true)"),
	}
	if update {
		props["id"] = str("Webhook id")
		return object(props, "id")
	}
	props["id"] = str("Optional id (generated if omitted)")
	return object(props, "url", "topics")
}

// suggest proposes similar topics when a topic has no value, a common agent
// mistake being a slightly wrong name.
func suggest(svc *core.Service, name string) string {
	for p := parentOf(name); ; p = parentOf(p) {
		filter := "#"
		if p != "" {
			filter = p + "/#"
		}
		res, total, err := svc.Query(filter, 10)
		if err == nil && total > 0 {
			names := make([]string, len(res))
			for i, e := range res {
				names[i] = e.Topic
			}
			more := ""
			if total > len(res) {
				more = fmt.Sprintf(" (and %d more; use describe_topic_tree or list_topics)", total-len(res))
			}
			return fmt.Sprintf(". Existing topics under %q: %s%s", filter, strings.Join(names, ", "), more)
		}
		if p == "" {
			return ". No topics have been received yet; check get_status."
		}
	}
}

func (s *Server) listTools(sess *session) any {
	out := make([]*tool, 0, len(tools))
	for _, t := range tools {
		if sess.principal.Can(t.scope) {
			out = append(out, t)
		}
	}
	return map[string]any{"tools": out}
}

func (s *Server) callTool(ctx context.Context, sess *session, params json.RawMessage) (any, *rpcError) {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, invalidParams("invalid params: " + err.Error())
	}
	var t *tool
	for _, x := range tools {
		if x.Name == p.Name {
			t = x
		}
	}
	if t == nil {
		return nil, invalidParams("unknown tool: " + p.Name)
	}
	if err := core.Require(sess.principal, t.scope); err != nil {
		return toolError(err), nil
	}
	start := time.Now()
	res, err := t.run(ctx, &call{svc: s.svc, principal: sess.principal, sess: sess}, p.Arguments)
	s.log.Debug("mcp: tool call", "tool", t.Name, "principal", sess.principal.Name, "ms", time.Since(start).Milliseconds(), "err", err)
	if err != nil {
		return toolError(err), nil
	}
	return toolResult(res), nil
}

func toolError(err error) map[string]any {
	return map[string]any{"isError": true, "content": []map[string]any{{"type": "text", "text": err.Error()}}}
}

// toolResult returns the result both as text (for every client) and as
// structuredContent (for clients on protocol 2025-06-18 and later).
func toolResult(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		return toolError(err)
	}
	var structured any = json.RawMessage(b)
	if len(b) == 0 || b[0] != '{' {
		structured = map[string]any{"result": json.RawMessage(b)}
	}
	return map[string]any{
		"content":           []map[string]any{{"type": "text", "text": string(b)}},
		"structuredContent": structured,
	}
}
