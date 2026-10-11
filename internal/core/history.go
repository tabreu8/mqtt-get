package core

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/history"
	"github.com/tabreu8/mqtt-get/internal/state"
	"github.com/tabreu8/mqtt-get/internal/topic"
)

// History limits for one query.
const (
	DefaultHistoryLimit = 1000
	MaxHistoryLimit     = 100000
	MaxHistoryBuckets   = history.MaxBuckets
)

// HistoryRuleView is a history rule with its counters.
type HistoryRuleView struct {
	config.HistoryRule
	Stats history.RuleStats `json:"stats"`
}

// historyRules returns every rule in effect order: the environment rule
// first, then the persisted ones.
func (s *Service) historyRules() []config.HistoryRule {
	list := s.state.HistoryRules()
	if s.envHistory != nil {
		list = append([]config.HistoryRule{*s.envHistory}, list...)
	}
	return list
}

func (s *Service) applyHistoryRules() { s.hist.SetRules(s.historyRules()) }

// HistoryRules lists the history rules, in the order they are matched.
func (s *Service) HistoryRules() []HistoryRuleView {
	stats := s.hist.Rules()
	list := s.historyRules()
	out := make([]HistoryRuleView, len(list))
	for i, r := range list {
		out[i] = HistoryRuleView{HistoryRule: r, Stats: stats[r.ID]}
	}
	return out
}

// HistoryRule returns one rule.
func (s *Service) HistoryRule(id string) (HistoryRuleView, error) {
	for _, r := range s.HistoryRules() {
		if r.ID == id {
			return r, nil
		}
	}
	return HistoryRuleView{}, notFound("history rule %q not found", id)
}

func (s *Service) isEnvHistory(id string) bool {
	return s.envHistory != nil && s.envHistory.ID == id
}

func envRuleError(id string) error {
	return &Error{Kind: Conflict, Msg: fmt.Sprintf("history rule %q comes from the HISTORY_* environment variables and cannot be changed through the API", id)}
}

// HistoryRuleConfig returns a persisted rule, to build a partial update.
func (s *Service) HistoryRuleConfig(id string) (config.HistoryRule, error) {
	if s.isEnvHistory(id) {
		return config.HistoryRule{}, envRuleError(id)
	}
	r, err := s.state.HistoryRule(id)
	if err != nil {
		return config.HistoryRule{}, notFound("history rule %q not found", id)
	}
	return r, nil
}

// PutHistoryRule creates (create=true) or replaces a rule, applies it to
// the recorded topics and saves it.
func (s *Service) PutHistoryRule(r config.HistoryRule, create bool) (HistoryRuleView, error) {
	s.histMu.Lock()
	defer s.histMu.Unlock()
	if s.isEnvHistory(r.ID) {
		return HistoryRuleView{}, envRuleError(r.ID)
	}
	_, err := s.state.HistoryRule(r.ID)
	exists := err == nil
	switch {
	case create && r.ID == "":
		r.ID = state.NewID()
	case create && exists:
		return HistoryRuleView{}, &Error{Kind: Conflict, Msg: fmt.Sprintf("history rule id %q already exists", r.ID)}
	case !create && !exists:
		return HistoryRuleView{}, notFound("history rule %q not found", r.ID)
	}
	r.Env = false
	r.Normalize()
	if err := r.Validate(); err != nil {
		return HistoryRuleView{}, invalid(err)
	}
	if err := s.state.PutHistoryRule(r); err != nil {
		return HistoryRuleView{}, err
	}
	s.applyHistoryRules()
	return s.HistoryRule(r.ID)
}

// DeleteHistoryRule removes a rule; topics no other rule matches lose
// their history.
func (s *Service) DeleteHistoryRule(id string) error {
	s.histMu.Lock()
	defer s.histMu.Unlock()
	if s.isEnvHistory(id) {
		return envRuleError(id)
	}
	if err := s.state.DeleteHistoryRule(id); err != nil {
		return notFound("history rule %q not found", id)
	}
	s.applyHistoryRules()
	return nil
}

// HistoryTopic describes the recorded history of one topic.
type HistoryTopic struct {
	Topic  string    `json:"topic"`
	Rule   string    `json:"rule"`
	Points int       `json:"points"`
	Bytes  int64     `json:"bytes"`
	Oldest time.Time `json:"oldest"`
	Newest time.Time `json:"newest"`
}

func historyTopic(in history.Info) HistoryTopic {
	return HistoryTopic{Topic: in.Topic, Rule: in.Rule, Points: in.Points, Bytes: in.Bytes,
		Oldest: time.Unix(0, in.Oldest).UTC(), Newest: time.Unix(0, in.Newest).UTC()}
}

// HistoryTopics lists the topics with recorded history matching filter
// ("" = all), sorted by topic, plus the total number of matches.
func (s *Service) HistoryTopics(filter string, limit int) ([]HistoryTopic, int, error) {
	if filter == "" {
		filter = "#"
	}
	if err := topic.ValidateFilter(filter); err != nil {
		return nil, 0, invalid(fmt.Errorf("filter %q: %w", filter, err))
	}
	if limit <= 0 {
		limit = 1000
	}
	list, total := s.hist.List(filter, min(limit, MaxHistoryLimit))
	out := make([]HistoryTopic, len(list))
	for i, in := range list {
		out[i] = historyTopic(in)
	}
	return out, total, nil
}

// HistoryQuery selects points of one topic.
type HistoryQuery struct {
	Since, Until time.Time // zero = unbounded
	Limit        int       // newest points to return (default 1000)
}

// HistoryResult is a range of points of one topic, oldest first.
type HistoryResult struct {
	Topic  string
	Rule   string
	Total  int // points in the range (Points may hold fewer: Limit)
	Points []history.Point
	Info   HistoryTopic // the whole recorded history of the topic
}

// History returns recorded points of an exact topic.
func (s *Service) History(name string, q HistoryQuery) (*HistoryResult, error) {
	if err := topic.ValidateName(name); err != nil {
		return nil, invalid(fmt.Errorf("topic %q: %w", name, err))
	}
	if !q.Until.IsZero() && q.Until.Before(q.Since) {
		return nil, invalid(errors.New("until is before since"))
	}
	if q.Limit <= 0 {
		q.Limit = DefaultHistoryLimit
	}
	q.Limit = min(q.Limit, MaxHistoryLimit)
	pts, total, info, ok := s.hist.Get(name, unixNano(q.Since), unixNano(q.Until), q.Limit)
	if !ok {
		if rule := s.hist.RuleFor(name); rule != "" {
			return nil, notFound("no history recorded yet for %q (history rule %q applies to it; points are recorded as messages arrive)", name, rule)
		}
		return nil, notFound("topic %q has no history: no history rule matches it (create one with REST POST /api/v1/history-rules or the MCP tool create_history_rule)", name)
	}
	return &HistoryResult{Topic: name, Rule: info.Rule, Total: total, Points: pts, Info: historyTopic(info)}, nil
}

// SummaryQuery selects and aggregates points of one topic.
type SummaryQuery struct {
	Since, Until time.Time // zero = unbounded
	Field        string    // JSON path of the value ("" = whole payload)
	// Bucket > 0 sets the bucket size. Otherwise AutoBuckets > 0 picks a
	// round size giving at most that many buckets over the range; with
	// both 0 there are no buckets.
	Bucket      time.Duration
	AutoBuckets int
}

// HistorySummary aggregates the points of a topic: see history.Summarize.
func (s *Service) HistorySummary(name string, q SummaryQuery) (*history.Summary, *HistoryResult, error) {
	res, err := s.History(name, HistoryQuery{Since: q.Since, Until: q.Until, Limit: MaxHistoryLimit})
	if err != nil {
		return nil, nil, err
	}
	bucket := q.Bucket
	if n := len(res.Points); bucket <= 0 && q.AutoBuckets > 0 && n > 0 {
		from, to := res.Points[0].Time, res.Points[n-1].Time
		if !q.Since.IsZero() {
			from = q.Since.UnixNano()
		}
		if !q.Until.IsZero() {
			to = q.Until.UnixNano()
		}
		bucket = history.AutoBucket(time.Duration(to-from), min(q.AutoBuckets, MaxHistoryBuckets))
	}
	if bucket > 0 && bucket < time.Millisecond {
		return nil, nil, Invalidf("bucket must be at least 1ms")
	}
	if n := len(res.Points); bucket > 0 && n > 0 {
		span := time.Duration(res.Points[n-1].Time - res.Points[0].Time)
		if span/bucket > MaxHistoryBuckets {
			return nil, nil, Invalidf("bucket %s gives more than %d buckets over the %s range; use a larger bucket", bucket, MaxHistoryBuckets, span.Round(time.Second))
		}
	}
	sum := history.Summarize(res.Points, strings.TrimSpace(q.Field), max(bucket, 0))
	return &sum, res, nil
}

// ForgetHistory removes the history of one topic ("" = all topics).
func (s *Service) ForgetHistory(name string) error {
	if name == "" {
		s.hist.Clear()
		return nil
	}
	if !s.hist.Delete(name) {
		return notFound("topic %q has no history", name)
	}
	return nil
}

// HistoryStats returns the global history counters.
func (s *Service) HistoryStats() history.Stats { return s.hist.Stats() }

func unixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// ParseTime parses a point in time for history queries: an RFC 3339
// timestamp, a Unix timestamp in seconds, or a duration meaning that long
// before now ("90s", "15m", "24h", "7d"). "" is the zero time (unbounded).
func ParseTime(v string, now time.Time) (time.Time, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, nil
	}
	d := strings.TrimPrefix(v, "-")
	if days, ok := strings.CutSuffix(d, "d"); ok {
		if n, err := strconv.ParseFloat(days, 64); err == nil && n >= 0 {
			return now.Add(-time.Duration(n * 24 * float64(time.Hour))), nil
		}
	}
	if d, err := time.ParseDuration(d); err == nil {
		return now.Add(-d), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t, nil
	}
	if sec, err := strconv.ParseFloat(v, 64); err == nil && isNumber(v) {
		return time.Unix(0, int64(sec*float64(time.Second))), nil
	}
	return time.Time{}, Invalidf("invalid time %q: use RFC 3339 (2026-01-02T15:04:05Z), Unix seconds, or a duration before now (15m, 24h, 7d)", v)
}

func isNumber(v string) bool {
	for i, c := range v {
		if (c < '0' || c > '9') && c != '.' && !(i == 0 && c == '-') {
			return false
		}
	}
	return v != ""
}
