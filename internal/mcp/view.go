package mcp

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tabreu8/mqtt-get/internal/store"
)

// maxPayloadBytes limits how much of one payload is shown to an agent, to
// protect its context window. The full value is always available via REST.
const maxPayloadBytes = 16 << 10

// value is the agent-facing representation of a stored message.
type value struct {
	Topic        string  `json:"topic"`
	Payload      any     `json:"payload"`
	Encoding     string  `json:"encoding"`
	QoS          byte    `json:"qos"`
	Retained     bool    `json:"retained"`
	Timestamp    string  `json:"timestamp"`
	AgeSeconds   float64 `json:"age_seconds"`
	PayloadBytes int     `json:"payload_bytes"`
	Truncated    bool    `json:"truncated,omitempty"`
	// MQTT 5 only.
	Properties       map[string]any `json:"properties,omitempty"`
	ExpiresInSeconds *float64       `json:"expires_in_seconds,omitempty"`
}

func viewOf(e *store.Entry) value {
	v := value{
		Topic:        e.Topic,
		QoS:          e.QoS,
		Retained:     e.Retained,
		Timestamp:    time.Unix(0, e.Time).UTC().Format(time.RFC3339Nano),
		AgeSeconds:   math.Round(time.Since(time.Unix(0, e.Time)).Seconds()*10) / 10,
		PayloadBytes: len(e.Payload),
		Encoding:     store.Encoding(e.Payload),
	}
	if pr := e.Props; !pr.Empty() {
		v.Properties = map[string]any{}
		if pr.ContentType != "" {
			v.Properties["content_type"] = pr.ContentType
		}
		if pr.ResponseTopic != "" {
			v.Properties["response_topic"] = pr.ResponseTopic
		}
		if len(pr.CorrelationData) > 0 {
			if utf8.Valid(pr.CorrelationData) {
				v.Properties["correlation_data"] = string(pr.CorrelationData)
			} else {
				v.Properties["correlation_data_base64"] = base64.StdEncoding.EncodeToString(pr.CorrelationData)
			}
		}
		if len(pr.UserProperties) > 0 {
			v.Properties["user_properties"] = pr.UserProperties
		}
		if pr.MessageExpiry > 0 {
			v.Properties["message_expiry_sec"] = pr.MessageExpiry
			left := math.Round(time.Until(e.ExpiresAt()).Seconds()*10) / 10
			v.ExpiresInSeconds = &left
		}
		if pr.PayloadUTF8 {
			v.Properties["payload_format"] = "utf8"
		}
	}
	p := e.Payload
	if len(p) > maxPayloadBytes {
		v.Truncated = true
		p = p[:maxPayloadBytes]
		if v.Encoding == store.EncJSON {
			// Partial JSON is not JSON: show it as text.
			v.Encoding = store.EncUTF8
		}
		if v.Encoding == store.EncUTF8 {
			for len(p) > 0 && !utf8.Valid(p) {
				p = p[:len(p)-1]
			}
		}
	}
	switch v.Encoding {
	case store.EncJSON:
		v.Payload = json.RawMessage(p)
	case store.EncUTF8:
		v.Payload = string(p)
	default:
		v.Payload = base64.StdEncoding.EncodeToString(p)
	}
	return v
}

func viewsOf(es []*store.Entry) []value {
	out := make([]value, len(es))
	for i, e := range es {
		out[i] = viewOf(e)
	}
	return out
}

// parentOf returns the parent level of a topic ("" for top level).
func parentOf(t string) string {
	if i := strings.LastIndexByte(t, '/'); i >= 0 {
		return t[:i]
	}
	return ""
}
