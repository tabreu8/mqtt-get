// Package store keeps the most recent message received for every topic.
//
// It is a sharded map so that many ingest goroutines and HTTP readers can
// operate concurrently with minimal lock contention. Entries are immutable
// once stored and are shared (not copied) with webhook dispatchers.
package store

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/tabreu8/mqtt-get/internal/topic"
)

const shardCount = 256

// Entry is one received MQTT message. Treat it as read-only.
type Entry struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retained bool
	Time     int64 // unix nanoseconds when received
}

type shard struct {
	mu sync.RWMutex
	m  map[string]*Entry
	_  [32]byte // reduce false sharing between adjacent shards
}

// Store is a concurrent latest-value-per-topic store.
type Store struct {
	shards    [shardCount]shard
	count     atomic.Int64
	dropped   atomic.Uint64
	maxTopics int64
}

// New creates a store. maxTopics <= 0 means unlimited.
func New(maxTopics int) *Store {
	s := &Store{maxTopics: int64(maxTopics)}
	for i := range s.shards {
		s.shards[i].m = make(map[string]*Entry)
	}
	return s
}

func (s *Store) shard(key string) *shard {
	// FNV-1a, inlined to avoid allocations.
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return &s.shards[h%shardCount]
}

// Set stores e as the latest value of e.Topic. It returns false if the value
// was rejected because the topic limit was reached.
func (s *Store) Set(e *Entry) bool {
	sh := s.shard(e.Topic)
	sh.mu.Lock()
	old, ok := sh.m[e.Topic]
	if !ok {
		if s.maxTopics > 0 && s.count.Load() >= s.maxTopics {
			sh.mu.Unlock()
			s.dropped.Add(1)
			return false
		}
		s.count.Add(1)
	} else if old.Time > e.Time {
		// Out-of-order delivery (possible with several shared-subscription
		// connections): keep the newer value.
		sh.mu.Unlock()
		return true
	}
	sh.m[e.Topic] = e
	sh.mu.Unlock()
	return true
}

// Get returns the latest entry for an exact topic name.
func (s *Store) Get(name string) (*Entry, bool) {
	sh := s.shard(name)
	sh.mu.RLock()
	e, ok := sh.m[name]
	sh.mu.RUnlock()
	return e, ok
}

// Delete removes a topic. It reports whether it existed.
func (s *Store) Delete(name string) bool {
	sh := s.shard(name)
	sh.mu.Lock()
	_, ok := sh.m[name]
	if ok {
		delete(sh.m, name)
		s.count.Add(-1)
	}
	sh.mu.Unlock()
	return ok
}

// Clear removes all topics.
func (s *Store) Clear() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		s.count.Add(-int64(len(sh.m)))
		sh.m = make(map[string]*Entry)
		sh.mu.Unlock()
	}
}

// Len returns the number of stored topics.
func (s *Store) Len() int { return int(s.count.Load()) }

// Dropped returns how many messages were rejected by the topic limit.
func (s *Store) Dropped() uint64 { return s.dropped.Load() }

// Query returns entries whose topic matches filter (all when filter is "" or
// "#"), sorted by topic, at most limit entries (0 = unlimited). It also
// returns the total number of matches.
func (s *Store) Query(filter string, limit int) ([]*Entry, int) {
	all := filter == "" || filter == "#"
	var out []*Entry
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for name, e := range sh.m {
			if all || topic.Match(filter, name) {
				out = append(out, e)
			}
		}
		sh.mu.RUnlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	total := len(out)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, total
}
