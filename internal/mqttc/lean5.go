package mqttc

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/tabreu8/mqtt-get/internal/store"
)

// MQTT 5 support for the lean client: property encoding and decoding.
// Messages without properties (the common case) skip all of this; a message
// with properties costs one extra allocation for its store.Props, whose
// strings are zero-copy views into the packet buffer.

// MQTT 5 property identifiers used here.
const (
	propPayloadFormat     = 0x01
	propMessageExpiry     = 0x02
	propContentType       = 0x03
	propResponseTopic     = 0x08
	propCorrelationData   = 0x09
	propSessionExpiry     = 0x11
	propWillDelay         = 0x18
	propAuthMethod        = 0x15
	propAuthData          = 0x16
	propAssignedClientID  = 0x12
	propServerKeepAlive   = 0x13
	propReasonString      = 0x1F
	propReceiveMaximum    = 0x21
	propTopicAliasMaximum = 0x22
	propTopicAlias        = 0x23
	propMaximumQoS        = 0x24
	propRetainAvailable   = 0x25
	propUserProperty      = 0x26
	propMaximumPacketSize = 0x27
)

const pAuth = 15

var errMalformedProps = errors.New("malformed properties")

// uvarint decodes an MQTT variable byte integer from b.
func uvarint(b []byte) (n, size int, err error) {
	mult := 1
	for i := 0; i < 4 && i < len(b); i++ {
		n += int(b[i]&0x7F) * mult
		if b[i]&0x80 == 0 {
			return n, i + 1, nil
		}
		mult *= 128
	}
	return 0, 0, errMalformedProps
}

// splitProps reads a property-length prefix at b[0:] and returns the
// properties and the rest.
func splitProps(b []byte) (props, rest []byte, err error) {
	n, k, err := uvarint(b)
	if err != nil || k+n > len(b) {
		return nil, nil, errMalformedProps
	}
	return b[k : k+n], b[k+n:], nil
}

// forEachProp calls fn for every property in b. For string and binary
// properties v is the content without its length prefix; for user
// properties v is the encoded key/value pair (see pair).
func forEachProp(b []byte, fn func(id byte, v []byte)) error {
	for len(b) > 0 {
		id := b[0]
		b = b[1:]
		var n, skip int
		switch id {
		case 0x01, 0x17, 0x19, 0x24, 0x25, 0x28, 0x29, 0x2A:
			n = 1
		case 0x13, 0x21, 0x22, 0x23:
			n = 2
		case 0x02, 0x11, 0x18, 0x27:
			n = 4
		case 0x0B: // subscription identifier
			_, k, err := uvarint(b)
			if err != nil {
				return err
			}
			n = k
		case 0x03, 0x08, 0x09, 0x12, 0x15, 0x16, 0x1A, 0x1C, 0x1F:
			if len(b) < 2 {
				return errMalformedProps
			}
			n, skip = 2+int(binary.BigEndian.Uint16(b)), 2
		case propUserProperty:
			if len(b) < 2 {
				return errMalformedProps
			}
			k := 2 + int(binary.BigEndian.Uint16(b))
			if len(b) < k+2 {
				return errMalformedProps
			}
			n = k + 2 + int(binary.BigEndian.Uint16(b[k:]))
		default:
			return errMalformedProps
		}
		if n > len(b) {
			return errMalformedProps
		}
		fn(id, b[skip:n])
		b = b[n:]
	}
	return nil
}

// pair decodes a user property value from forEachProp.
func pair(v []byte) (key, val []byte) {
	k := int(binary.BigEndian.Uint16(v))
	return v[2 : 2+k], v[4+k:]
}

// view returns a string sharing b's memory; b must never be modified.
func view(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// publishProps decodes the properties of an inbound PUBLISH. Strings are
// views into b (the packet buffer, never modified).
func publishProps(b []byte) (props *store.Props, alias uint16, err error) {
	var p store.Props
	err = forEachProp(b, func(id byte, v []byte) {
		switch id {
		case propPayloadFormat:
			p.PayloadUTF8 = v[0] == 1
		case propMessageExpiry:
			p.MessageExpiry = binary.BigEndian.Uint32(v)
		case propContentType:
			p.ContentType = view(v)
		case propResponseTopic:
			p.ResponseTopic = view(v)
		case propCorrelationData:
			p.CorrelationData = v[:len(v):len(v)]
		case propUserProperty:
			k, val := pair(v)
			p.UserProperties = append(p.UserProperties, store.UserProperty{Key: view(k), Value: view(val)})
		case propTopicAlias:
			alias = binary.BigEndian.Uint16(v)
		}
	})
	if err != nil || p.Empty() {
		return nil, alias, err
	}
	return &p, alias, nil
}

// reasonString returns the Reason String property, if any.
func reasonString(props []byte) string {
	var s string
	_ = forEachProp(props, func(id byte, v []byte) {
		if id == propReasonString {
			s = string(v)
		}
	})
	return s
}

// ackReason decodes a v5 PUBACK/PUBREC/PUBREL/PUBCOMP body after the packet
// id: the reason code (0 when omitted) and the reason string.
func ackReason(rest []byte) (code byte, reason string) {
	if len(rest) == 0 {
		return 0, ""
	}
	code = rest[0]
	if code >= 0x80 && len(rest) > 1 {
		if props, _, err := splitProps(rest[1:]); err == nil {
			reason = reasonString(props)
		}
	}
	return code, reason
}

// appendPublishProps appends the property section of an outbound PUBLISH.
func appendPublishProps(b []byte, p *store.Props) []byte {
	if p.Empty() {
		return append(b, 0)
	}
	var pb []byte
	if p.PayloadUTF8 {
		pb = append(pb, propPayloadFormat, 1)
	}
	if p.MessageExpiry > 0 {
		pb = binary.BigEndian.AppendUint32(append(pb, propMessageExpiry), p.MessageExpiry)
	}
	if p.ContentType != "" {
		pb = appendStr(append(pb, propContentType), p.ContentType)
	}
	if p.ResponseTopic != "" {
		pb = appendStr(append(pb, propResponseTopic), p.ResponseTopic)
	}
	if len(p.CorrelationData) > 0 {
		pb = binary.BigEndian.AppendUint16(append(pb, propCorrelationData), uint16(len(p.CorrelationData)))
		pb = append(pb, p.CorrelationData...)
	}
	for _, u := range p.UserProperties {
		pb = appendStr(appendStr(append(pb, propUserProperty), u.Key), u.Value)
	}
	return append(appendVarint(b, len(pb)), pb...)
}

// authData returns the Authentication Data property, if any.
func authData(props []byte) []byte {
	var d []byte
	_ = forEachProp(props, func(id byte, v []byte) {
		if id == propAuthData {
			d = v
		}
	})
	return d
}

// authPacket builds an MQTT 5 AUTH packet continuing the exchange.
func authPacket(method string, data []byte) []byte {
	pb := appendStr([]byte{propAuthMethod}, method)
	pb = binary.BigEndian.AppendUint16(append(pb, propAuthData), uint16(len(data)))
	pb = append(pb, data...)
	return packet(pAuth<<4, append(appendVarint([]byte{0x18}, len(pb)), pb...))
}

// connack holds what mqtt-get uses from a v5 CONNACK.
type connack struct {
	reason           string
	serverKeepAlive  int // -1 = not set
	receiveMaximum   int
	maximumQoS       byte
	retainAvailable  bool
	maximumPacket    int // 0 = unlimited
	assignedClientID string
	authData         []byte
}

func parseConnack(props []byte) (connack, error) {
	c := connack{serverKeepAlive: -1, receiveMaximum: 65535, maximumQoS: 2, retainAvailable: true}
	err := forEachProp(props, func(id byte, v []byte) {
		switch id {
		case propReasonString:
			c.reason = string(v)
		case propServerKeepAlive:
			c.serverKeepAlive = int(binary.BigEndian.Uint16(v))
		case propReceiveMaximum:
			if n := int(binary.BigEndian.Uint16(v)); n > 0 {
				c.receiveMaximum = n
			}
		case propMaximumQoS:
			c.maximumQoS = v[0]
		case propRetainAvailable:
			c.retainAvailable = v[0] == 1
		case propMaximumPacketSize:
			c.maximumPacket = int(binary.BigEndian.Uint32(v))
		case propAssignedClientID:
			c.assignedClientID = string(v)
		case propAuthData:
			c.authData = append([]byte(nil), v...)
		}
	})
	return c, err
}
