package store

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"
	"unicode/utf8"
)

// Payload encodings used in JSON output.
const (
	EncJSON   = "json"   // payload is valid JSON and embedded as-is
	EncUTF8   = "utf8"   // payload is a UTF-8 string
	EncBase64 = "base64" // binary payload, base64 encoded
)

// Encoding returns how a payload is represented in JSON output.
func Encoding(p []byte) string {
	if len(p) > 0 && json.Valid(p) {
		return EncJSON
	}
	if utf8.Valid(p) {
		return EncUTF8
	}
	return EncBase64
}

// AppendJSON appends the JSON representation of e to buf:
//
//	{"topic":"a/b","payload":...,"encoding":"json","qos":0,"retained":false,"timestamp":"..."}
func AppendJSON(buf []byte, e *Entry) []byte {
	buf = append(buf, `{"topic":`...)
	buf = AppendString(buf, e.Topic)
	buf = append(buf, `,"payload":`...)
	enc := Encoding(e.Payload)
	switch enc {
	case EncJSON:
		buf = append(buf, e.Payload...)
	case EncUTF8:
		buf = AppendString(buf, string(e.Payload))
	default:
		buf = append(buf, '"')
		buf = base64.StdEncoding.AppendEncode(buf, e.Payload)
		buf = append(buf, '"')
	}
	buf = append(buf, `,"encoding":"`...)
	buf = append(buf, enc...)
	buf = append(buf, `","qos":`...)
	buf = strconv.AppendUint(buf, uint64(e.QoS), 10)
	buf = append(buf, `,"retained":`...)
	buf = strconv.AppendBool(buf, e.Retained)
	buf = append(buf, `,"timestamp":"`...)
	buf = time.Unix(0, e.Time).UTC().AppendFormat(buf, time.RFC3339Nano)
	buf = append(buf, '"')
	buf = appendProps(buf, e.Props)
	return append(buf, '}')
}

const hexDigits = "0123456789abcdef"

// AppendString appends s as a JSON string literal.
func AppendString(buf []byte, s string) []byte {
	buf = append(buf, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' && c != '<' && c != '>' && c != '&' {
				i++
				continue
			}
			buf = append(buf, s[start:i]...)
			switch c {
			case '"', '\\':
				buf = append(buf, '\\', c)
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\t':
				buf = append(buf, '\\', 't')
			default:
				buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			buf = append(buf, s[start:i]...)
			buf = append(buf, `�`...)
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			buf = append(buf, s[start:i]...)
			buf = append(buf, '\\', 'u', '2', '0', '2', hexDigits[r&0xf])
			i += size
			start = i
			continue
		}
		i += size
	}
	buf = append(buf, s[start:]...)
	return append(buf, '"')
}
