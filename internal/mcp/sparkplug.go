package mcp

import (
	"context"
	"encoding/json"

	"github.com/tabreu8/mqtt-get/internal/config"
)

var sparkplugReadTools = []*tool{
	{
		Name: "list_sparkplug_nodes",
		Description: "List the Sparkplug B edge nodes and devices seen on the broker, with online state and (with metrics=true) every metric's name, type, unit and translated topic. " +
			"Sparkplug metric values are readable like any topic (get_value, get_values, get_history, wait_for_message) under <prefix>/<group>/<edge_node>[/<device>]/<metric>; each value is {\"value\":…,\"type\":…,\"timestamp\":…,\"online\":…}. online=false means the node or device died and the value is the last one known.",
		InputSchema: object(map[string]any{
			"group":   str("Only this Sparkplug group"),
			"metrics": boolean("Include every metric (default false: counts only)"),
		}),
		Annotations: annotations("List Sparkplug nodes", true, false, true, false),
		scope:       config.ScopeRead,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Group   string `json:"group"`
				Metrics bool   `json:"metrics"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			nodes, err := c.svc.SparkplugNodes(a.Group, a.Metrics)
			if err != nil {
				return nil, err
			}
			v := c.svc.Sparkplug()
			out := map[string]any{"prefix": v.Config.Prefix, "commands_enabled": v.Config.Commands, "count": len(nodes), "nodes": nodes}
			if v.Hint != "" {
				out["hint"] = v.Hint
			}
			return out, nil
		},
	},
}

var sparkplugPublishTools = []*tool{
	{
		Name: "request_sparkplug_rebirth",
		Description: "Ask a Sparkplug B edge node to publish its birth certificates again (NCMD Node Control/Rebirth), to refresh every metric value and definition. " +
			"Harmless for the process, but the node re-sends all its metrics.",
		InputSchema: object(map[string]any{
			"group":     str("Sparkplug group id"),
			"edge_node": str("Edge node id"),
		}, "group", "edge_node"),
		Annotations: annotations("Request Sparkplug rebirth", false, false, true, true),
		scope:       config.ScopePublish,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Group string `json:"group"`
				Edge  string `json:"edge_node"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if err := c.svc.SparkplugRebirth(a.Group, a.Edge); err != nil {
				return nil, err
			}
			return map[string]any{"requested": true, "group": a.Group, "edge_node": a.Edge}, nil
		},
	},
}

var sparkplugAdminTools = []*tool{
	{
		Name: "configure_sparkplug",
		Description: "Change Sparkplug B support (only the given fields change; saved). When enabled, spBv1.0 messages are decoded into plain topics with JSON values. " +
			"rebirth lets mqtt-get request births from nodes it can't decode; commands lets publish write to Sparkplug metrics (sent as NCMD/DCMD, typed by the metric's birth), limited to commands_filter when set.",
		InputSchema: object(map[string]any{
			"enabled":              boolean("Decode Sparkplug B messages"),
			"prefix":               str(`Prefix of the translated topics (default "spb")`),
			"keep_raw":             boolean("Also keep the raw protobuf messages under spBv1.0/... (default false)"),
			"rebirth":              boolean("Request a rebirth when a node's metrics can't be decoded or messages were lost (default false)"),
			"rebirth_interval_sec": integer("Minimum seconds between rebirth requests per node (default 30)", 1, 86400),
			"reorder_timeout_ms":   integer("How long to hold a message that arrives out of sequence for the missing ones; when it expires, a rebirth is requested (with rebirth). 0 disables (default 2000)", 0, 60000),
			"commands":             boolean("Allow writes to Sparkplug metrics through publish (default false)"),
			"commands_filter":      strList("Only metrics whose translated topic matches one of these topic filters are writable (e.g. [\"spb/plant1/+/+/Setpoints/#\"]); empty: all"),
		}),
		Annotations: annotations("Configure Sparkplug B", false, true, true, true),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			cfg := c.svc.SparkplugConfig()
			if err := decodeArgs(args, &cfg); err != nil {
				return nil, err
			}
			return c.svc.SetSparkplug(cfg)
		},
	},
}
