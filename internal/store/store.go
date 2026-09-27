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
	"time"
	"unsafe"

	"github.com/tabreu8/mqtt-get/internal/topic"
)

const shardCount = 256

// Entry is one received MQTT message. Treat it as read-only.
type Entry struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retained bool
	Time     int64  // unix nanoseconds when received
	Props    *Props // MQTT 5 properties (nil for MQTT 3.1.1 or none)
}

// slot holds the latest value of one topic. It is allocated once per topic
// and then updated in place under the shard lock, so updating an existing
// topic allocates nothing. The topic is only kept as the map key: storing it
// again would retain a second copy of the name for every updated topic.
//
// The payload is kept as pointer + length instead of a slice header, which
// brings a slot from the 48-byte to the 32-byte allocation class.
type slot struct {
	payload  *byte // first byte of the payload (keeps the whole array alive)
	n        uint32
	qos      byte
	retained bool
	time     int64
	props    *Props
}

func newSlot(e *Entry) slot {
	return slot{payload: unsafe.SliceData(e.Payload), n: uint32(len(e.Payload)), qos: e.QoS, retained: e.Retained, time: e.Time, props: e.Props}
}

func (sl *slot) bytes() []byte {
	if sl.payload == nil {
		return nil
	}
	return unsafe.Slice(sl.payload, sl.n)
}

func (sl *slot) value(topic string) Entry {
	return Entry{Topic: topic, Payload: sl.bytes(), QoS: sl.qos, Retained: sl.retained, Time: sl.time, Props: sl.props}
}

func (sl *slot) entry(topic string) *Entry {
	e := sl.value(topic)
	return &e
}

type shard struct {
	mu sync.RWMutex
	m  map[string]*slot
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
		s.shards[i].m = make(map[string]*slot)
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
	} else if old.time > e.Time {
		// Out-of-order delivery (possible with several shared-subscription
		// connections): keep the newer value.
		sh.mu.Unlock()
		return true
	}
	if ok {
		// In place: no allocation, and the caller's copy of the topic name
		// is not retained. Fields are written one by one so pointer writes
		// (which cost a GC write barrier while the collector runs) happen
		// only when needed: props is nil for all MQTT 3.1.1 traffic.
		old.payload = unsafe.SliceData(e.Payload)
		old.n = uint32(len(e.Payload))
		old.qos, old.retained, old.time = e.QoS, e.Retained, e.Time
		if old.props != e.Props {
			old.props = e.Props
		}
	} else {
		sl := newSlot(e)
		sh.m[e.Topic] = &sl
	}
	sh.mu.Unlock()
	return true
}

// Get returns the latest entry for an exact topic name.
func (s *Store) Get(name string) (*Entry, bool) {
	sh := s.shard(name)
	sh.mu.RLock()
	sl, ok := sh.m[name]
	if !ok {
		sh.mu.RUnlock()
		return nil, false
	}
	e := sl.entry(name) // copy while holding the lock
	sh.mu.RUnlock()
	return e, true
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
		sh.m = make(map[string]*slot)
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
	now := time.Now()
	var vals []Entry // one backing array instead of one allocation per result
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for name, sl := range sh.m {
			if all || topic.Match(filter, name) {
				if v := sl.value(name); !v.Expired(now) { // MQTT 5 message expiry
					vals = append(vals, v)
				}
			}
		}
		sh.mu.RUnlock()
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i].Topic < vals[j].Topic })
	total := len(vals)
	if limit > 0 && len(vals) > limit {
		vals = vals[:limit]
	}
	out := make([]*Entry, len(vals))
	for i := range vals {
		out[i] = &vals[i]
	}
	return out, total
}
