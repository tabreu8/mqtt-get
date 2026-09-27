package core

import (
	"errors"
	"fmt"
)

// Kind classifies errors so each interface can map them (HTTP status codes,
// MCP tool errors, JSON-RPC errors).
type Kind int

// Error kinds.
const (
	Internal        Kind = iota
	Invalid              // bad input
	NotFound             // unknown topic, webhook, key, ...
	Conflict             // already exists
	Unauthenticated      // missing or invalid API key
	Forbidden            // key lacks a scope
	Unavailable          // not connected to the broker
	Upstream             // broker or webhook endpoint returned an error
	Busy                 // too many concurrent operations
)

// Error is an error with a Kind.
type Error struct {
	Kind Kind
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

// KindOf returns the Kind of err (Internal if it has none).
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return Internal
}

func invalid(err error) error { return &Error{Kind: Invalid, Msg: err.Error()} }

func notFound(format string, a ...any) error {
	return &Error{Kind: NotFound, Msg: fmt.Sprintf(format, a...)}
}

// Invalidf returns an Invalid error.
func Invalidf(format string, a ...any) error {
	return &Error{Kind: Invalid, Msg: fmt.Sprintf(format, a...)}
}
