package api

import (
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
)

// handleMetrics writes Prometheus text-format metrics.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	st := s.mqtt.Status()
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
	metric("mqttget_topics", "gauge", "Distinct topics with a stored latest value.", s.store.Len())
	metric("mqttget_topics_dropped_total", "counter", "Messages not stored because MAX_TOPICS was reached.", s.store.Dropped())
	metric("mqttget_goroutines", "gauge", "Number of goroutines.", runtime.NumGoroutine())
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	metric("mqttget_heap_bytes", "gauge", "Heap bytes in use.", ms.HeapInuse)

	stats := s.hooks.AllStats()
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
