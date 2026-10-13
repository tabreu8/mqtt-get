package httpapi

import (
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
)

// handleMetrics writes Prometheus text-format metrics.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	st := s.svc.MQTTStatus()
	var b strings.Builder
	metric := func(name, typ, help string, v any) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, v)
	}
	bool01 := func(v bool) int {
		if v {
			return 1
		}
		return 0
	}
	metric("mqttget_mqtt_connected", "gauge", "1 if at least one broker connection is up.", bool01(st.Connected))
	metric("mqttget_mqtt_connections_up", "gauge", "Number of broker connections up.", st.ConnectedCount)
	metric("mqttget_messages_received_total", "counter", "MQTT messages received.", st.MessagesReceived)
	metric("mqttget_bytes_received_total", "counter", "MQTT payload bytes received.", st.BytesReceived)
	metric("mqttget_messages_published_total", "counter", "MQTT messages published via the API.", st.MessagesSent)
	metric("mqttget_publish_errors_total", "counter", "Failed publish attempts.", st.PublishErrors)
	metric("mqttget_topics", "gauge", "Distinct topics with a stored latest value.", s.svc.Store().Len())
	metric("mqttget_topics_dropped_total", "counter", "Messages not stored because MAX_TOPICS was reached.", s.svc.Store().Dropped())
	metric("mqttget_active_watchers", "gauge", "Active waits and MCP resource subscriptions.", s.svc.Status().Watchers)
	hs := s.svc.HistoryStats()
	metric("mqttget_history_topics", "gauge", "Topics with recorded history.", hs.Topics)
	metric("mqttget_history_points", "gauge", "History points kept in memory.", hs.Points)
	metric("mqttget_history_bytes", "gauge", "Approximate memory used by history.", hs.Bytes)
	metric("mqttget_history_max_bytes", "gauge", "HISTORY_MAX_BYTES (0 = unlimited).", hs.MaxBytes)
	metric("mqttget_history_dropped_total", "counter", "Messages not recorded because a history byte budget was exhausted.", hs.Dropped)
	if sp := s.svc.Sparkplug().Stats; sp != nil {
		metric("mqttget_sparkplug_messages_total", "counter", "Sparkplug B messages processed.", sp.Messages)
		metric("mqttget_sparkplug_metrics_total", "counter", "Sparkplug metric values translated to topics.", sp.Metrics)
		metric("mqttget_sparkplug_malformed_total", "counter", "Sparkplug payloads that could not be decoded.", sp.Malformed)
		metric("mqttget_sparkplug_dropped_total", "counter", "Sparkplug messages or metrics dropped by the memory limits.", sp.Dropped)
		metric("mqttget_sparkplug_unknown_metrics_total", "counter", "Sparkplug metrics dropped: alias without a birth certificate.", sp.Unknown)
		metric("mqttget_sparkplug_seq_errors_total", "counter", "Sparkplug sequence number gaps (lost messages).", sp.SeqErrors)
		metric("mqttget_sparkplug_rebirth_requests_total", "counter", "Rebirth requests sent to edge nodes.", sp.RebirthRequests)
		metric("mqttget_sparkplug_commands_total", "counter", "NCMD/DCMD writes sent.", sp.Commands)
		metric("mqttget_sparkplug_nodes", "gauge", "Sparkplug edge nodes seen.", sp.Nodes)
		metric("mqttget_sparkplug_nodes_online", "gauge", "Sparkplug edge nodes online.", sp.NodesOnline)
	}
	metric("mqttget_goroutines", "gauge", "Number of goroutines.", runtime.NumGoroutine())
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	metric("mqttget_heap_bytes", "gauge", "Heap bytes in use.", ms.HeapInuse)

	stats := s.svc.WebhookStats()
	ids := make([]string, 0, len(stats))
	for id := range stats {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	series := []struct {
		name, typ, help string
		get             func(id string) any
	}{
		{"mqttget_webhook_delivered_total", "counter", "Messages delivered to the webhook.", func(id string) any { return stats[id].Delivered }},
		{"mqttget_webhook_failed_total", "counter", "Messages that failed delivery after retries.", func(id string) any { return stats[id].Failed }},
		{"mqttget_webhook_dropped_total", "counter", "Messages dropped because the webhook queue was full.", func(id string) any { return stats[id].Dropped }},
		{"mqttget_webhook_requests_total", "counter", "HTTP requests made to the webhook.", func(id string) any { return stats[id].Requests }},
		{"mqttget_webhook_queue_length", "gauge", "Messages waiting in the webhook queue.", func(id string) any { return stats[id].Queued }},
	}
	for _, m := range series {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", m.name, m.help, m.name, m.typ)
		for _, id := range ids {
			fmt.Fprintf(&b, "%s{webhook=%q} %v\n", m.name, id, m.get(id))
		}
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}
