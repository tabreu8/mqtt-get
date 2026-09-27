// Package topic implements MQTT topic name / topic filter validation and
// matching, including an immutable trie used to fan messages out to many
// subscribers without scanning every filter.
package topic

import (
	"errors"
	"strings"
)

var (
	errEmpty    = errors.New("topic must not be empty")
	errTooLong  = errors.New("topic exceeds 65535 bytes")
	errWildcard = errors.New("topic name must not contain wildcards (+ or #)")
	errHash     = errors.New("'#' must be the last level of a filter and occupy the whole level")
	errPlus     = errors.New("'+' must occupy a whole level")
	errNull     = errors.New("topic must not contain NUL characters")
)

// ValidateName checks that s is a valid topic name to publish to.
func ValidateName(s string) error {
	if s == "" {
		return errEmpty
	}
	if len(s) > 65535 {
		return errTooLong
	}
	if strings.ContainsAny(s, "+#") {
		return errWildcard
	}
	if strings.IndexByte(s, 0) >= 0 {
		return errNull
	}
	return nil
}

// ValidateFilter checks that s is a valid subscription filter.
func ValidateFilter(s string) error {
	if s == "" {
		return errEmpty
	}
	if len(s) > 65535 {
		return errTooLong
	}
	if strings.IndexByte(s, 0) >= 0 {
		return errNull
	}
	rest := s
	for {
		level, r, more := cut(rest)
		if strings.IndexByte(level, '#') >= 0 && (level != "#" || more) {
			return errHash
		}
		if strings.IndexByte(level, '+') >= 0 && level != "+" {
			return errPlus
		}
		if !more {
			return nil
		}
		rest = r
	}
}

// HasWildcard reports whether s contains a wildcard character.
func HasWildcard(s string) bool { return strings.ContainsAny(s, "+#") }

// Match reports whether the topic name matches the (valid) filter. It does
// not allocate.
func Match(filter, name string) bool {
	// Wildcards in the first level never match topics starting with '$'.
	if len(name) > 0 && name[0] == '$' && len(filter) > 0 && (filter[0] == '+' || filter[0] == '#') {
		return false
	}
	for {
		fl, frest, fmore := cut(filter)
		if fl == "#" {
			return true
		}
		nl, nrest, nmore := cut(name)
		if fl != "+" && fl != nl {
			return false
		}
		if !fmore {
			return !nmore
		}
		if !nmore {
			// "a/#" matches "a".
			return frest == "#"
		}
		filter, name = frest, nrest
	}
}

func cut(s string) (level, rest string, more bool) {
	i := strings.IndexByte(s, '/')
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+1:], true
}
