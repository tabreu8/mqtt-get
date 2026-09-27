package topic

import "strings"

// Trie maps topic filters to values. It is built once and then read
// concurrently without locks; rebuild it (copy-on-write) when filters change.
type Trie[T comparable] struct {
	root node[T]
	size int
}

type node[T comparable] struct {
	children map[string]*node[T]
	values   []T
}

// NewTrie returns an empty trie.
func NewTrie[T comparable]() *Trie[T] { return &Trie[T]{} }

// Insert adds value v under filter. Filters are assumed to be valid.
func (t *Trie[T]) Insert(filter string, v T) {
	n := &t.root
	for _, level := range strings.Split(filter, "/") {
		if n.children == nil {
			n.children = make(map[string]*node[T])
		}
		c := n.children[level]
		if c == nil {
			c = &node[T]{}
			n.children[level] = c
		}
		n = c
	}
	n.values = append(n.values, v)
	t.size++
}

// Len returns the number of inserted (filter, value) pairs.
func (t *Trie[T]) Len() int { return t.size }

// Match appends every value whose filter matches the topic name to buf and
// returns it. Each value is appended at most once.
func (t *Trie[T]) Match(name string, buf []T) []T {
	if t == nil || t.size == 0 {
		return buf
	}
	start := len(buf)
	dollar := len(name) > 0 && name[0] == '$'
	buf = t.root.match(name, !dollar, buf)
	return dedupe(buf, start)
}

// match matches name (the remaining levels) against the children of n.
// wild reports whether wildcards may match the current level.
func (n *node[T]) match(name string, wild bool, buf []T) []T {
	if n.children == nil {
		return buf
	}
	if wild {
		if h := n.children["#"]; h != nil {
			buf = append(buf, h.values...)
		}
	}
	level, rest, more := cut(name)
	if c := n.children[level]; c != nil {
		buf = c.next(rest, more, buf)
	}
	if wild && level != "+" {
		if c := n.children["+"]; c != nil {
			buf = c.next(rest, more, buf)
		}
	}
	return buf
}

func (n *node[T]) next(rest string, more bool, buf []T) []T {
	if !more {
		buf = append(buf, n.values...)
		// "a/#" also matches "a".
		if h := n.children["#"]; h != nil {
			buf = append(buf, h.values...)
		}
		return buf
	}
	return n.match(rest, true, buf)
}

func dedupe[T comparable](buf []T, start int) []T {
	out := buf[:start]
	for i := start; i < len(buf); i++ {
		v := buf[i]
		dup := false
		for j := start; j < len(out); j++ {
			if out[j] == v {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, v)
		}
	}
	return out
}
