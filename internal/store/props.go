package store

import (
	"encoding/base64"
	"strconv"
	"time"
	"unicode/utf8"
)

// Props are the MQTT 5 properties of a message that matter to consumers.
// Messages without properties (all MQTT 3.1.1 traffic) carry a nil *Props,
// so they cost nothing.
type Props struct {
	ContentType     string         // MIME type of the payload
	ResponseTopic   string         // where the receiver should reply (request/response)
	CorrelationData []byte         // opaque id linking a response to its request
	UserProperties  []UserProperty // application key/value pairs (keys may repeat)
	MessageExpiry   uint32         // seconds; 0 = never expires
	PayloadUTF8     bool           // payload format indicator: payload is UTF-8 text
}

// UserProperty is one MQTT 5 user property.
type UserProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Empty reports whether no property is set.
func (p *Props) Empty() bool {
	return p == nil || (p.ContentType == "" && p.ResponseTopic == "" && len(p.CorrelationData) == 0 &&
		len(p.UserProperties) == 0 && p.MessageExpiry == 0 && !p.PayloadUTF8)
}

// ExpiresAt returns when a message received at receivedNanos expires
// (zero time if it never does).
func (e *Entry) ExpiresAt() time.Time {
	if e.Props == nil || e.Props.MessageExpiry == 0 {
		return time.Time{}
	}
	return time.Unix(0, e.Time).Add(time.Duration(e.Props.MessageExpiry) * time.Second)
}

// Expired reports whether the message's MQTT 5 expiry interval has passed.
func (e *Entry) Expired(now time.Time) bool {
	at := e.ExpiresAt()
	return !at.IsZero() && now.After(at)
}

// appendProps appends `,"properties":{...}` when there are properties.
func appendProps(buf []byte, p *Props) []byte {
	if p.Empty() {
		return buf
	}
	buf = append(buf, `,"properties":{`...)
	first := true
	field := func(name string) {
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = append(buf, '"')
		buf = append(buf, name...)
		buf = append(buf, `":`...)
	}
	if p.ContentType != "" {
		field("content_type")
		buf = AppendString(buf, p.ContentType)
	}
	if p.ResponseTopic != "" {
		field("response_topic")
		buf = AppendString(buf, p.ResponseTopic)
	}
	if len(p.CorrelationData) > 0 {
		if utf8.Valid(p.CorrelationData) {
			field("correlation_data")
			buf = AppendString(buf, string(p.CorrelationData))
		} else {
			field("correlation_data_base64")
			buf = append(buf, '"')
			buf = base64.StdEncoding.AppendEncode(buf, p.CorrelationData)
			buf = append(buf, '"')
		}
	}
	if len(p.UserProperties) > 0 {
		field("user_properties")
		buf = append(buf, '[')
		for i, up := range p.UserProperties {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, `{"key":`...)
			buf = AppendString(buf, up.Key)
			buf = append(buf, `,"value":`...)
			buf = AppendString(buf, up.Value)
			buf = append(buf, '}')
		}
		buf = append(buf, ']')
	}
	if p.MessageExpiry > 0 {
		field("message_expiry_sec")
		buf = strconv.AppendUint(buf, uint64(p.MessageExpiry), 10)
	}
	if p.PayloadUTF8 {
		field("payload_format")
		buf = append(buf, `"utf8"`...)
	}
	return append(buf, '}')
}

// Header names used for properties on raw HTTP bodies (REST and webhooks).
const (
	HeaderContentType     = "X-MQTT-Content-Type"
	HeaderResponseTopic   = "X-MQTT-Response-Topic"
	HeaderCorrelationData = "X-MQTT-Correlation-Data" // base64
	HeaderMessageExpiry   = "X-MQTT-Message-Expiry"   // seconds
	HeaderUserProperty    = "X-MQTT-User-Property"    // "key=value", repeatable
	HeaderPayloadFormat   = "X-MQTT-Payload-Format"   // "utf8"
)

// WriteHeaders renders properties as HTTP headers with add(key, value).
func (p *Props) WriteHeaders(add func(k, v string)) {
	if p.Empty() {
		return
	}
	if p.ContentType != "" {
		add(HeaderContentType, p.ContentType)
	}
	if p.ResponseTopic != "" {
		add(HeaderResponseTopic, p.ResponseTopic)
	}
	if len(p.CorrelationData) > 0 {
		add(HeaderCorrelationData, base64.StdEncoding.EncodeToString(p.CorrelationData))
	}
	if p.MessageExpiry > 0 {
		add(HeaderMessageExpiry, strconv.FormatUint(uint64(p.MessageExpiry), 10))
	}
	if p.PayloadUTF8 {
		add(HeaderPayloadFormat, "utf8")
	}
	for _, u := range p.UserProperties {
		add(HeaderUserProperty, u.Key+"="+u.Value)
	}
}
