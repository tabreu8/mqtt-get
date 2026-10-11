package httpapi

import (
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/history"
	"github.com/tabreu8/mqtt-get/internal/store"
)

func (s *Server) historyRoutes(mux *http.ServeMux) {
	read, admin := config.ScopeRead, config.ScopeAdmin
	mux.HandleFunc("GET /api/v1/history", s.auth(read, s.handleHistoryList))
	mux.HandleFunc("GET /api/v1/history/{topic...}", s.auth(read, s.handleHistory))
	mux.HandleFunc("DELETE /api/v1/history", s.auth(admin, s.handleHistoryClear))
	mux.HandleFunc("DELETE /api/v1/history/{topic...}", s.auth(admin, s.handleHistoryDelete))

	mux.HandleFunc("GET /api/v1/history-rules", s.auth(admin, s.handleHistoryRuleList))
	mux.HandleFunc("POST /api/v1/history-rules", s.auth(admin, s.handleHistoryRuleCreate))
	mux.HandleFunc("GET /api/v1/history-rules/{id}", s.auth(admin, s.handleHistoryRuleGet))
	mux.HandleFunc("PUT /api/v1/history-rules/{id}", s.auth(admin, s.handleHistoryRuleUpdate))
	mux.HandleFunc("DELETE /api/v1/history-rules/{id}", s.auth(admin, s.handleHistoryRuleDelete))
}

// handleHistoryList lists the topics with history (?filter=, ?limit=), or
// returns one topic's history with ?topic=.
func (s *Server) handleHistoryList(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("topic") != "" {
		s.handleHistory(w, r)
		return
	}
	list, total, err := s.svc.HistoryTopics(r.URL.Query().Get("filter"), intParam(r, "limit", 1000))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"total": total, "count": len(list), "topics": list, "stats": s.svc.HistoryStats()})
}

// handleHistory returns the points of one topic in a time range, or with
// bucket=, summary=true or field= an aggregate of them.
//
//	since, until  RFC 3339, Unix seconds, or a duration before now (15m, 24h, 7d)
//	limit         newest points returned (default 1000)
//	field         JSON path of the value to aggregate ("celsius", "data.temp")
//	bucket        time bucket size (5m, 1h) or "auto"
//	format        "json" (default) or "csv"
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	now := time.Now()
	since, err := core.ParseTime(q.Get("since"), now)
	if err != nil {
		writeError(w, badRequest(fmt.Errorf("since: %w", err)))
		return
	}
	until, err := core.ParseTime(q.Get("until"), now)
	if err != nil {
		writeError(w, badRequest(fmt.Errorf("until: %w", err)))
		return
	}
	name := topicParam(r)
	field := q.Get("field")
	summary, _ := strconv.ParseBool(q.Get("summary"))
	if b := q.Get("bucket"); b != "" || summary || field != "" {
		sq := core.SummaryQuery{Since: since, Until: until, Field: field}
		switch b {
		case "":
		case "auto":
			sq.AutoBuckets = 60
		default:
			if sq.Bucket, err = time.ParseDuration(b); err != nil || sq.Bucket <= 0 {
				writeError(w, badRequest(fmt.Errorf(`bucket: use a positive duration (30s, 5m, 1h) or "auto"`)))
				return
			}
		}
		sum, res, err := s.svc.HistorySummary(name, sq)
		if err != nil {
			writeError(w, err)
			return
		}
		if q.Get("format") == "csv" {
			writeBucketsCSV(w, sum)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, 200, struct {
			Topic string `json:"topic"`
			Rule  string `json:"rule"`
			*history.Summary
		}{res.Topic, res.Rule, sum})
		return
	}

	res, err := s.svc.History(name, core.HistoryQuery{Since: since, Until: until, Limit: intParam(r, "limit", core.DefaultHistoryLimit)})
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if q.Get("format") == "csv" {
		writePointsCSV(w, res.Points)
		return
	}
	size := 128
	for _, p := range res.Points {
		size += len(p.Payload) + 80
	}
	buf := make([]byte, 0, size)
	buf = append(buf, `{"topic":`...)
	buf = store.AppendString(buf, res.Topic)
	buf = append(buf, `,"rule":`...)
	buf = store.AppendString(buf, res.Rule)
	buf = append(buf, `,"total":`...)
	buf = strconv.AppendInt(buf, int64(res.Total), 10)
	buf = append(buf, `,"count":`...)
	buf = strconv.AppendInt(buf, int64(len(res.Points)), 10)
	buf = append(buf, `,"points":[`...)
	for i, p := range res.Points {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, `{"payload":`...)
		buf = store.AppendPayload(buf, p.Payload)
		buf = append(buf, `,"qos":`...)
		buf = strconv.AppendUint(buf, uint64(p.QoS), 10)
		buf = append(buf, `,"retained":`...)
		buf = strconv.AppendBool(buf, p.Retained)
		buf = append(buf, `,"timestamp":"`...)
		buf = time.Unix(0, p.Time).UTC().AppendFormat(buf, time.RFC3339Nano)
		buf = append(buf, `"}`...)
	}
	buf = append(buf, "]}\n"...)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(buf)
}

func writePointsCSV(w http.ResponseWriter, pts []history.Point) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"timestamp", "payload", "qos", "retained"})
	for _, p := range pts {
		payload := string(p.Payload)
		if store.Encoding(p.Payload) == store.EncBase64 {
			payload = "base64:" + base64.StdEncoding.EncodeToString(p.Payload)
		}
		_ = cw.Write([]string{time.Unix(0, p.Time).UTC().Format(time.RFC3339Nano), payload,
			strconv.Itoa(int(p.QoS)), strconv.FormatBool(p.Retained)})
	}
	cw.Flush()
}

func writeBucketsCSV(w http.ResponseWriter, sum *history.Summary) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"start", "count", "min", "max", "avg", "first", "last"})
	f := func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
	for _, b := range sum.Buckets {
		_ = cw.Write([]string{b.Start.Format(time.RFC3339), strconv.Itoa(b.Count), f(b.Min), f(b.Max), f(b.Avg), f(b.First), f(b.Last)})
	}
	cw.Flush()
}

func (s *Server) handleHistoryDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.ForgetHistory(topicParam(r)); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHistoryClear(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("topic") != "" {
		s.handleHistoryDelete(w, r)
		return
	}
	_ = s.svc.ForgetHistory("")
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleHistoryRuleList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"rules": s.svc.HistoryRules(), "stats": s.svc.HistoryStats()})
}

func (s *Server) handleHistoryRuleGet(w http.ResponseWriter, r *http.Request) {
	v, err := s.svc.HistoryRule(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, v)
}

func (s *Server) handleHistoryRuleCreate(w http.ResponseWriter, r *http.Request) {
	var hr config.HistoryRule
	if err := s.decode(w, r, &hr); err != nil {
		writeError(w, err)
		return
	}
	v, err := s.svc.PutHistoryRule(hr, true)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 201, v)
}

func (s *Server) handleHistoryRuleUpdate(w http.ResponseWriter, r *http.Request) {
	var hr config.HistoryRule
	if err := s.decode(w, r, &hr); err != nil {
		writeError(w, err)
		return
	}
	hr.ID = r.PathValue("id")
	v, err := s.svc.PutHistoryRule(hr, false)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, v)
}

func (s *Server) handleHistoryRuleDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.DeleteHistoryRule(r.PathValue("id")); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
