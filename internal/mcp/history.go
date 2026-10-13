package mcp

import (
	"context"
	"encoding/json"
	"math"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/history"
	"github.com/tabreu8/mqtt-get/internal/store"
	"github.com/tabreu8/mqtt-get/internal/topic"
)

// maxHistoryPayloadBytes limits each history point shown to an agent
// (points mode returns up to 1000 of them).
const maxHistoryPayloadBytes = 2 << 10

// withExtraTools inserts the history and Sparkplug read tools after
// wait_for_message, the publish tools after publish_and_wait, and appends
// the administration tools.
func withExtraTools(list []*tool) []*tool {
	var out []*tool
	for _, t := range list {
		out = append(out, t)
		switch t.Name {
		case "wait_for_message":
			out = append(out, historyReadTools...)
			out = append(out, sparkplugReadTools...)
		case "publish_and_wait":
			out = append(out, sparkplugPublishTools...)
		}
	}
	out = append(out, historyAdminTools...)
	return append(out, sparkplugAdminTools...)
}

var historyReadTools = []*tool{
	{
		Name: "get_history",
		Description: "Past values of one exact topic, recorded by mqtt-get's history rules. Default mode \"summary\" returns compact statistics: " +
			"for numbers min/max/avg/first/last (with when the min and max happened) and about 60 time buckets; for text or status values " +
			"the distinct values with counts and the most recent changes. Use field to pick a number inside a JSON payload (e.g. \"celsius\" or \"data.temp\"). " +
			"Mode \"points\" returns the raw messages (newest limit points). Only topics matched by a history rule have history (see list_history_topics).",
		InputSchema: object(map[string]any{
			"topic":          str(`Exact topic name, e.g. "home/kitchen/temperature"`),
			"since":          str(`Start of the range: a duration before now ("15m", "6h", "7d"), an RFC 3339 timestamp, or "all". Default "1h"`),
			"until":          str("End of the range (same formats as since). Default now"),
			"mode":           enum(`"summary" (default) or "points"`, "summary", "points"),
			"field":          str(`Dot path of the value inside a JSON payload, e.g. "celsius", "data.temp", "values.0". Omit to use the whole payload`),
			"bucket_seconds": map[string]any{"type": "number", "description": "Summary bucket size in seconds. Default: chosen to give at most max_buckets buckets"},
			"max_buckets":    integer("Summary: maximum number of automatic buckets (default 60, 0 = no buckets)", 0, 1000),
			"limit":          integer("Points mode: newest points to return (default 50)", 1, 1000),
		}, "topic"),
		Annotations: annotations("Get topic history", true, false, true, false),
		scope:       config.ScopeRead,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Topic         string  `json:"topic"`
				Since         string  `json:"since"`
				Until         string  `json:"until"`
				Mode          string  `json:"mode"`
				Field         string  `json:"field"`
				BucketSeconds float64 `json:"bucket_seconds"`
				MaxBuckets    *int    `json:"max_buckets"`
				Limit         int     `json:"limit"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if topic.HasWildcard(a.Topic) {
				return nil, core.Invalidf("topic %q contains wildcards; get_history takes one exact topic (list_history_topics accepts a filter)", a.Topic)
			}
			now := time.Now()
			if a.Since == "" {
				a.Since = "1h"
			}
			var since time.Time
			if a.Since != "all" {
				var err error
				if since, err = core.ParseTime(a.Since, now); err != nil {
					return nil, err
				}
			}
			until, err := core.ParseTime(a.Until, now)
			if err != nil {
				return nil, err
			}
			rangeOut := map[string]any{"since": fmtTime(since), "until": fmtTime(until)}
			switch a.Mode {
			case "", "summary":
				q := core.SummaryQuery{Since: since, Until: until, Field: a.Field, AutoBuckets: 60,
					Bucket: time.Duration(a.BucketSeconds * float64(time.Second))}
				if a.MaxBuckets != nil {
					q.AutoBuckets = *a.MaxBuckets
				}
				sum, res, err := c.svc.HistorySummary(a.Topic, q)
				if err != nil {
					return nil, err
				}
				return map[string]any{"topic": a.Topic, "rule": res.Rule, "range": rangeOut, "recorded": recorded(res.Info), "summary": sum}, nil
			case "points":
				if a.Limit <= 0 {
					a.Limit = 50
				}
				res, err := c.svc.History(a.Topic, core.HistoryQuery{Since: since, Until: until, Limit: a.Limit})
				if err != nil {
					return nil, err
				}
				pts := make([]value, len(res.Points))
				for i, p := range res.Points {
					pts[i] = pointView(a.Topic, p)
				}
				return map[string]any{"topic": a.Topic, "rule": res.Rule, "range": rangeOut, "recorded": recorded(res.Info),
					"total": res.Total, "returned": len(pts), "points": pts}, nil
			}
			return nil, core.Invalidf(`mode must be "summary" or "points"`)
		},
	},
	{
		Name:        "list_history_topics",
		Description: "List the topics that have recorded history (optionally matching an MQTT filter), with how many points and which time span each covers.",
		InputSchema: object(map[string]any{
			"filter": pFilter,
			"limit":  integer("Maximum topics to return (default 200)", 1, 5000),
		}),
		Annotations: annotations("List topics with history", true, false, true, false),
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
			list, total, err := c.svc.HistoryTopics(a.Filter, a.Limit)
			if err != nil {
				return nil, err
			}
			items := make([]map[string]any, len(list))
			for i, t := range list {
				items[i] = recorded(t)
				items[i]["topic"] = t.Topic
				items[i]["rule"] = t.Rule
			}
			out := map[string]any{"total": total, "returned": len(items), "topics": items}
			if total == 0 && len(c.svc.HistoryRules()) == 0 {
				out["hint"] = "no history rules are configured, so no history is recorded; an admin can add one with create_history_rule"
			}
			return out, nil
		},
	},
}

var historyAdminTools = []*tool{
	{
		Name:        "list_history_rules",
		Description: "List the history rules (which topics keep a history and with which limits) with their usage: topics, points, memory and dropped messages.",
		InputSchema: object(map[string]any{}),
		Annotations: annotations("List history rules", true, false, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			if err := decodeArgs(args, &struct{}{}); err != nil {
				return nil, err
			}
			return map[string]any{"rules": c.svc.HistoryRules(), "stats": c.svc.HistoryStats()}, nil
		},
	},
	{
		Name: "create_history_rule",
		Description: "Start keeping a history of the topics matching the filters, in memory. Keep it bounded: max_points per topic (default 1000), " +
			"max_age_sec drops old points, min_interval_ms downsamples fast publishers (e.g. 60000 keeps one point per minute), on_change skips repeated values. " +
			"History only covers messages received after the rule is created.",
		InputSchema: historyRuleSchema(false),
		Annotations: annotations("Create history rule", false, false, false, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var r config.HistoryRule
			if err := decodeArgs(args, &r); err != nil {
				return nil, err
			}
			return c.svc.PutHistoryRule(r, true)
		},
	},
	{
		Name:        "update_history_rule",
		Description: "Change fields of a history rule (only the given fields change). Lower limits trim the recorded history immediately.",
		InputSchema: historyRuleSchema(true),
		Annotations: annotations("Update history rule", false, true, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var id struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(args, &id)
			r, err := c.svc.HistoryRuleConfig(id.ID)
			if err != nil {
				return nil, err
			}
			if err := decodeArgs(args, &r); err != nil {
				return nil, err
			}
			r.ID = id.ID
			return c.svc.PutHistoryRule(r, false)
		},
	},
	{
		Name:        "delete_history_rule",
		Description: "Delete a history rule. The recorded history of topics no other rule matches is deleted too.",
		InputSchema: object(map[string]any{"id": str("History rule id")}, "id"),
		Annotations: annotations("Delete history rule", false, true, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				ID string `json:"id"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if err := c.svc.DeleteHistoryRule(a.ID); err != nil {
				return nil, err
			}
			return map[string]any{"deleted": a.ID}, nil
		},
	},
	{
		Name:        "clear_history",
		Description: "Delete the recorded history of one topic, or of all topics when topic is omitted (rules are kept and recording continues).",
		InputSchema: object(map[string]any{"topic": str("Exact topic; omit to clear all history")}),
		Annotations: annotations("Clear history", false, true, true, false),
		scope:       config.ScopeAdmin,
		run: func(_ context.Context, c *call, args json.RawMessage) (any, error) {
			var a struct {
				Topic string `json:"topic"`
			}
			if err := decodeArgs(args, &a); err != nil {
				return nil, err
			}
			if err := c.svc.ForgetHistory(a.Topic); err != nil {
				return nil, err
			}
			return map[string]any{"cleared": true}, nil
		},
	},
}

func historyRuleSchema(update bool) map[string]any {
	props := map[string]any{
		"name":            str("Descriptive name"),
		"topics":          strList("MQTT topic filters to record"),
		"max_points":      integer("Points kept per topic (default 1000)", 1, config.MaxHistoryPoints),
		"max_age_sec":     integer("Drop points older than this many seconds (default: no age limit)", 0, 1<<31-1),
		"min_interval_ms": integer("Keep at most one point per interval per topic, e.g. 60000 = one per minute (default: every message)", 0, 1<<31-1),
		"on_change":       boolean("Skip messages whose payload equals the previous point (default false)"),
		"max_bytes":       integer("Memory budget for all topics of this rule, in bytes (default: only the global HISTORY_MAX_BYTES)", 0, 1<<53),
		"enabled":         boolean("Enabled (default true)"),
	}
	if update {
		props["id"] = str("History rule id")
		return object(props, "id")
	}
	props["id"] = str("Optional id (generated if omitted)")
	return object(props, "topics")
}

func fmtTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

func recorded(t core.HistoryTopic) map[string]any {
	return map[string]any{
		"points": t.Points, "oldest": t.Oldest.Format(time.RFC3339Nano), "newest": t.Newest.Format(time.RFC3339Nano),
		"span_seconds": math.Round(t.Newest.Sub(t.Oldest).Seconds()*10) / 10,
	}
}

// pointView renders a history point like a value, with a smaller payload
// limit.
func pointView(name string, p history.Point) value {
	e := store.Entry{Topic: name, Payload: p.Payload, QoS: p.QoS, Retained: p.Retained, Time: p.Time}
	return viewOfLimit(&e, maxHistoryPayloadBytes)
}
