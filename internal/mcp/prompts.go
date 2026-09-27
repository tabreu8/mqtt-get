package mcp

import (
	"encoding/json"
	"fmt"

	"github.com/tabreu8/mqtt-get/internal/config"
)

type promptArg struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Required    bool   `json:"required,omitempty"`
}

type prompt struct {
	Name        string      `json:"name"`
	Title       string      `json:"title"`
	Description string      `json:"description"`
	Arguments   []promptArg `json:"arguments,omitempty"`
	scope       string
	build       func(s *Server, args map[string]string) ([]map[string]any, error)
}

func userText(text string) map[string]any {
	return map[string]any{"role": "user", "content": map[string]any{"type": "text", "text": text}}
}

var prompts = []*prompt{
	{
		Name:        "explore_namespace",
		Title:       "Explore the MQTT namespace",
		Description: "Survey what data is available on the broker and summarize devices, measurements and freshness.",
		Arguments:   []promptArg{{Name: "prefix", Description: "Optional topic prefix to focus on, e.g. \"factory\""}},
		scope:       config.ScopeRead,
		build: func(_ *Server, a map[string]string) ([]map[string]any, error) {
			scope := "the whole broker"
			if a["prefix"] != "" {
				scope = fmt.Sprintf("the topics under %q", a["prefix"])
			}
			return []map[string]any{userText(fmt.Sprintf(`Explore %s and tell me what data is available.

1. Call describe_topic_tree (prefix %q, depth 2) and drill into the most interesting branches.
2. Sample representative values with get_values (use filters, don't fetch everything).
3. Summarize: the main areas / devices, what each measures (with units if visible), update frequency (age_seconds) and anything that looks stale or odd.
Do not publish anything.`, scope, a["prefix"]))}, nil
		},
	},
	{
		Name:        "monitor_topics",
		Title:       "Monitor topics for changes",
		Description: "Watch topics for a while and report the changes and anything unusual.",
		Arguments: []promptArg{
			{Name: "filter", Description: "MQTT filter to watch, e.g. \"factory/+/+/alarm\"", Required: true},
			{Name: "duration_minutes", Description: "How long to watch (default 5)"},
		},
		scope: config.ScopeRead,
		build: func(_ *Server, a map[string]string) ([]map[string]any, error) {
			if a["filter"] == "" {
				return nil, fmt.Errorf("filter is required")
			}
			d := a["duration_minutes"]
			if d == "" {
				d = "5"
			}
			return []map[string]any{userText(fmt.Sprintf(`Monitor MQTT topics matching %q for about %s minutes.

First read the current values with get_values (filter %q). Then repeatedly call wait_for_message with that filter (timeout_seconds 60) until the time is up.
Report each meaningful change as it happens (topic, old → new value), and finish with a summary: how many messages, which topics changed, and anything abnormal (alarms, out-of-range values, topics that went silent).
Do not publish anything.`, a["filter"], d, a["filter"]))}, nil
		},
	},
	{
		Name:        "control_device",
		Title:       "Control a device safely",
		Description: "Send a command to a device and confirm it took effect, checking the current state first.",
		Arguments: []promptArg{
			{Name: "command_topic", Description: "Topic that accepts commands, e.g. \"home/lamp/set\"", Required: true},
			{Name: "desired_state", Description: "What should happen, in words or as a payload, e.g. \"turn on\" or {\"state\":\"ON\"}", Required: true},
			{Name: "state_topic", Description: "Topic where the device reports its state, e.g. \"home/lamp/state\""},
		},
		scope: config.ScopePublish,
		build: func(_ *Server, a map[string]string) ([]map[string]any, error) {
			if a["command_topic"] == "" || a["desired_state"] == "" {
				return nil, fmt.Errorf("command_topic and desired_state are required")
			}
			state := a["state_topic"]
			if state == "" {
				state = "(find it: look for a sibling state/status topic with describe_topic_tree)"
			}
			return []map[string]any{userText(fmt.Sprintf(`I want to control a device: %s.
Command topic: %s
State topic: %s

1. Read the current state with get_value and recent values on the command topic to learn the payload format. Do not guess the format; if unclear, ask me.
2. If the device is already in the desired state, stop and tell me.
3. Tell me exactly which payload you will publish and wait for my confirmation.
4. Use publish_and_wait (response_filter = the state topic) and report whether the device confirmed the change.`,
				a["desired_state"], a["command_topic"], state))}, nil
		},
	},
	{
		Name:        "troubleshoot_connection",
		Title:       "Troubleshoot the broker connection",
		Description: "Diagnose why data is missing: connection state, errors, subscriptions and configuration.",
		scope:       config.ScopeAdmin,
		build: func(s *Server, _ map[string]string) ([]map[string]any, error) {
			st, _ := json.Marshal(s.svc.Status())
			br, _ := json.Marshal(s.svc.Broker())
			return []map[string]any{
				userText(`Diagnose the MQTT connection of this mqtt-get instance using the status and settings below.
Explain what is wrong (if anything) in plain words and propose the exact fix (e.g. a configure_broker call). Common causes: wrong URL scheme (tcp vs mqtts), private CA missing (tls.ca_pem), mTLS certificate required, bad credentials, ACLs rejecting the subscription filter, duplicate client_id, broker not supporting shared subscriptions.
Do not change anything without my confirmation.`),
				{"role": "user", "content": map[string]any{"type": "resource", "resource": map[string]any{
					"uri": uriStatus, "mimeType": "application/json", "text": string(st)}}},
				{"role": "user", "content": map[string]any{"type": "resource", "resource": map[string]any{
					"uri": uriBroker, "mimeType": "application/json", "text": string(br)}}},
			}, nil
		},
	},
}

func (s *Server) listPrompts(sess *session) any {
	out := make([]*prompt, 0, len(prompts))
	for _, p := range prompts {
		if sess.principal.Can(p.scope) {
			out = append(out, p)
		}
	}
	return map[string]any{"prompts": out}
}

func (s *Server) getPrompt(sess *session, params json.RawMessage) (any, *rpcError) {
	var p struct {
		Name      string            `json:"name"`
		Arguments map[string]string `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, invalidParams("invalid params")
	}
	for _, pr := range prompts {
		if pr.Name != p.Name {
			continue
		}
		if !sess.principal.Can(pr.scope) {
			return nil, &rpcError{Code: codeInvalidRequest, Message: "API key lacks the " + pr.scope + " scope"}
		}
		msgs, err := pr.build(s, p.Arguments)
		if err != nil {
			return nil, invalidParams(err.Error())
		}
		return map[string]any{"description": pr.Description, "messages": msgs}, nil
	}
	return nil, invalidParams("unknown prompt: " + p.Name)
}
