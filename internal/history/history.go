// Package history keeps a bounded, in-memory history of the messages on
// topics matched by history rules.
//
// Each topic matched by a rule gets a ring buffer of points. A point holds
// the received payload as-is (the same buffer the latest-value store keeps,
// never copied) plus its timestamp, so recording a message allocates nothing
// once the ring has reached its size. Memory is bounded by every rule's
// max_points per topic, an optional per-rule byte budget and a global byte
// budget; when a budget is exhausted, a topic makes room by dropping its own
// oldest points, so one noisy topic never wipes out the others.
//
// Topics that match no rule cost one atomic load (no rules at all) or one
// lock-free trie match per message.
package history

import (
	"bytes"
	"hash/maphash"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
	"github.com/tabreu8/mqtt-get/internal/topic"
)

const shardCount = 64

// Memory accounting (approximate): a point costs its payload, its topic
// name (the MQTT client allocates both in one block, which the point keeps
// alive) and pointOverhead; a topic costs seriesOverhead plus its name.
const (
	pointOverhead  = int64(unsafe.Sizeof(point{}))
	seriesOverhead = 160
)

// Point is one recorded message.
type Point struct {
	Payload  []byte
	Time     int64 // unix nanoseconds when received
	QoS      byte
	Retained bool
}

// point is the compact stored form of a Point (pointer + length instead of
// a slice header: 24 bytes).
type point struct {
	payload  *byte
	n        uint32
	qos      byte
	retained bool
	time     int64
}

func (p *point) bytes() []byte {
	if p.payload == nil {
		return nil
	}
	return unsafe.Slice(p.payload, p.n)
}

func (p *point) export() Point {
	return Point{Payload: p.bytes(), Time: p.time, QoS: p.qos, Retained: p.retained}
}

// rule is the runtime form of a config.HistoryRule.
type rule struct {
	cfg         config.HistoryRule
	order       int
	maxPoints   int
	maxAge      int64 // ns, 0 = none
	minInterval int64 // ns, 0 = none
	maxBytes    int64 // 0 = none

	topics  atomic.Int64
	points  counter
	bytes   counter
	dropped atomic.Uint64
}

func newRule(c config.HistoryRule, order int) *rule {
	c.Normalize()
	return &rule{
		points: counter{fold: pointsFold}, bytes: counter{fold: foldFor(c.MaxBytes)},
		cfg: c, order: order, maxPoints: c.MaxPoints,
		maxAge:      int64(c.MaxAgeSec) * int64(time.Second),
		minInterval: int64(c.MinIntervalMs) * int64(time.Millisecond),
		maxBytes:    c.MaxBytes,
	}
}

// rules is an immutable rule set, swapped atomically on changes.
type rules struct {
	list []*rule
	trie *topic.Trie[*rule]
}

var matchPool = sync.Pool{New: func() any { b := make([]*rule, 0, 4); return &b }}

// match returns the first rule (in configuration order) matching name.
func (rs *rules) match(name string) *rule {
	bp := matchPool.Get().(*[]*rule)
	defer matchPool.Put(bp)
	m := rs.trie.Match(name, (*bp)[:0])
	*bp = m[:0]
	var best *rule
	for _, r := range m {
		if best == nil || r.order < best.order {
			best = r
		}
	}
	return best
}

// series is the ring buffer of one topic.
type series struct {
	mu      sync.Mutex
	rule    *rule
	buf     []point
	head    int
	n       int
	bytes   int64 // points only
	nameLen int64 // topic name length, accounted in every point
	dead    bool  // removed from the map: ignore late writes
	stripe  uint8 // counter stripe its changes go to
}

func (s *series) at(i int) *point { return &s.buf[(s.head+i)%len(s.buf)] }

type shard struct {
	mu sync.RWMutex
	m  map[string]*series
	_  [32]byte
}

// Options configure a History.
type Options struct {
	// MaxBytes is the global memory budget (0 = unlimited).
	MaxBytes int64
	// Now is the clock (tests); default time.Now.
	Now func() time.Time
}

// History is the set of recorded topics. All methods are safe for
// concurrent use.
type History struct {
	shards   [shardCount]shard
	rules    atomic.Pointer[rules]
	rulesMu  sync.Mutex // serializes SetRules
	maxBytes int64
	now      func() time.Time

	topics  atomic.Int64
	points  counter
	bytes   counter
	dropped atomic.Uint64
}

// counter is a total updated from many cores. Changes go to one of its
// stripes and are folded into the total once they exceed fold, so
// concurrent writers rarely touch the same cache line. Approx misses at
// most counterStripes*fold; Load is exact.
type counter struct {
	total   atomic.Int64
	fold    int64
	stripes [counterStripes]struct {
		v atomic.Int64
		_ [56]byte
	}
}

const (
	counterStripes = 64
	pointsFold     = 256
)

// foldFor is the fold of a byte counter with budget max: a budget can be
// exceeded by at most 1% (64 stripes × max/6400), and by 1 MiB at most.
func foldFor(max int64) int64 {
	if max <= 0 {
		return 16 << 10
	}
	return min(16<<10, max/6400)
}

// Add adds d to the total (rare changes).
func (c *counter) Add(d int64) { c.total.Add(d) }

// AddStriped adds d through a stripe (frequent changes).
func (c *counter) AddStriped(stripe uint8, d int64) {
	st := &c.stripes[stripe%counterStripes]
	if v := st.v.Add(d); v > c.fold || v < -c.fold {
		c.total.Add(st.v.Swap(0))
	}
}

// Approx returns the total without the changes still in the stripes.
func (c *counter) Approx() int64 { return c.total.Load() }

// Load returns the exact total.
func (c *counter) Load() int64 {
	n := c.total.Load()
	for i := range c.stripes {
		n += c.stripes[i].v.Load()
	}
	return n
}

// New creates an empty history with no rules.
func New(o Options) *History {
	h := &History{maxBytes: o.MaxBytes, now: o.Now, points: counter{fold: pointsFold}, bytes: counter{fold: foldFor(o.MaxBytes)}}
	if h.now == nil {
		h.now = time.Now
	}
	for i := range h.shards {
		h.shards[i].m = map[string]*series{}
	}
	h.rules.Store(&rules{trie: topic.NewTrie[*rule]()})
	return h
}

var shardSeed = maphash.MakeSeed()

func (h *History) shard(key string) *shard {
	return &h.shards[maphash.String(shardSeed, key)%shardCount]
}

// Add records e if a rule matches its topic. It never blocks on other topics.
func (h *History) Add(e *store.Entry) { h.AddRef(e, nil) }

// Ref caches the series of a topic for AddRef. The zero value is ready to
// use. Use one Ref for one topic only.
type Ref struct{ s atomic.Pointer[series] }

// AddRef is Add through ref (nil: none), which caches e.Topic's series so
// that recording it again skips looking it up.
func (h *History) AddRef(e *store.Entry, ref *Ref) {
	rs := h.rules.Load()
	if rs.trie.Len() == 0 {
		return
	}
	if ref != nil {
		if s := ref.s.Load(); s != nil && h.add(s, e) {
			return
		}
	}
	// Recorded topics skip the trie: their series knows its rule.
	sh := h.shard(e.Topic)
	sh.mu.RLock()
	s := sh.m[e.Topic]
	sh.mu.RUnlock()
	if s == nil {
		if rs.match(e.Topic) == nil {
			return
		}
		if s = h.create(sh, e.Topic); s == nil {
			return
		}
	}
	if ref != nil {
		ref.s.Store(s)
	}
	h.add(s, e)
}

// create adds the series of a new topic, using the current rule set (which
// SetRules may have just replaced).
func (h *History) create(sh *shard, name string) *series {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if s := sh.m[name]; s != nil {
		return s
	}
	r := h.rules.Load().match(name)
	if r == nil {
		return nil
	}
	cost := seriesOverhead + int64(len(name))
	if h.over(r, cost) {
		r.dropped.Add(1)
		h.dropped.Add(1)
		return nil
	}
	s := &series{rule: r, nameLen: int64(len(name)), stripe: uint8(maphash.String(shardSeed, name) >> 56)}
	sh.m[strings.Clone(name)] = s
	h.topics.Add(1)
	h.bytes.Add(cost)
	r.topics.Add(1)
	r.bytes.Add(cost)
	return s
}

func (h *History) over(r *rule, size int64) bool {
	return (h.maxBytes > 0 && h.bytes.Approx()+size > h.maxBytes) ||
		(r.maxBytes > 0 && r.bytes.Approx()+size > r.maxBytes)
}

// add records e in s. It returns false when s was removed (the topic
// must be looked up again).
func (h *History) add(s *series, e *store.Entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return false
	}
	r := s.rule
	if s.n > 0 {
		last := s.at(s.n - 1)
		// Time going backwards (out-of-order delivery) is not downsampled.
		if r.minInterval > 0 && e.Time >= last.time && e.Time-last.time < r.minInterval {
			return true
		}
		if r.cfg.OnChange && bytes.Equal(last.bytes(), e.Payload) {
			return true
		}
	}
	// Changes are accounted once at the end: in the steady state (a full
	// ring replacing a point of the same size) that touches no shared
	// counter at all.
	var dPoints, dBytes int64
	if r.maxAge > 0 {
		dPoints, dBytes = s.trimOlder(e.Time - r.maxAge)
	}
	size := int64(len(e.Payload)) + int64(len(e.Topic)) + pointOverhead
	if h.over(r, size+dBytes) {
		// Make room from this topic's own oldest points, so the total does
		// not grow while over budget.
		for s.n > 0 && -dBytes < size {
			dBytes -= s.pop()
			dPoints--
		}
		if h.over(r, size+dBytes) {
			h.account(s, r, dPoints, dBytes)
			r.dropped.Add(1)
			h.dropped.Add(1)
			return true
		}
	}
	if s.n == r.maxPoints {
		dBytes -= s.pop()
		dPoints--
	}
	if s.n == len(s.buf) {
		s.grow(r.maxPoints)
	}
	*s.at(s.n) = point{payload: unsafe.SliceData(e.Payload), n: uint32(len(e.Payload)), qos: e.QoS, retained: e.Retained, time: e.Time}
	s.n++
	s.bytes += size
	h.account(s, r, dPoints+1, dBytes+size)
	return true
}

func (h *History) account(s *series, r *rule, points, size int64) {
	if points != 0 {
		h.points.AddStriped(s.stripe, points)
		r.points.AddStriped(s.stripe, points)
	}
	if size != 0 {
		h.bytes.AddStriped(s.stripe, size)
		r.bytes.AddStriped(s.stripe, size)
	}
}

// grow enlarges a full ring (doubling, up to max points).
func (s *series) grow(max int) {
	c := 2 * len(s.buf)
	if c < 8 {
		c = 8
	}
	if c > max {
		c = max
	}
	s.resize(c)
}

// resize copies the points (oldest first) into a buffer of c slots; c must
// be >= s.n.
func (s *series) resize(c int) {
	nb := make([]point, c)
	for i := 0; i < s.n; i++ {
		nb[i] = *s.at(i)
	}
	s.buf, s.head = nb, 0
}

// pop drops the oldest point and returns its size. The caller accounts
// for it (see dropOlder and add).
func (s *series) pop() int64 {
	p := s.at(0)
	size := int64(p.n) + pointOverhead + s.nameLen
	*p = point{} // release the payload
	s.head = (s.head + 1) % len(s.buf)
	s.n--
	s.bytes -= size
	return size
}

// trimOlder drops points received before cutoff (unix ns) and returns the
// (negative) change in points and bytes, for the caller to account.
func (s *series) trimOlder(cutoff int64) (dPoints, dBytes int64) {
	for s.n > 0 && s.at(0).time < cutoff {
		dBytes -= s.pop()
		dPoints--
	}
	return dPoints, dBytes
}

// dropOlder is trimOlder with accounting.
func (h *History) dropOlder(s *series, cutoff int64) {
	dp, db := s.trimOlder(cutoff)
	h.account(s, s.rule, dp, db)
}

// dropOldest drops points until at most keep remain, with accounting.
func (h *History) dropOldest(s *series, keep int) {
	var dp, db int64
	for s.n > keep {
		db -= s.pop()
		dp--
	}
	h.account(s, s.rule, dp, db)
}

// RuleStats are per-rule counters.
type RuleStats struct {
	Topics  int64  `json:"topics"`
	Points  int64  `json:"points"`
	Bytes   int64  `json:"bytes"`
	Dropped uint64 `json:"dropped"`
}

// SetRules replaces the rule set. Disabled rules are ignored. Recorded
// topics are re-evaluated: a topic now matched by another rule moves to it
// (trimmed to its limits) and a topic no rule matches is forgotten.
func (h *History) SetRules(list []config.HistoryRule) {
	h.rulesMu.Lock()
	defer h.rulesMu.Unlock()
	rs := &rules{trie: topic.NewTrie[*rule]()}
	for _, c := range list {
		if !c.IsEnabled() {
			continue
		}
		r := newRule(c, len(rs.list))
		rs.list = append(rs.list, r)
		for _, f := range c.Topics {
			rs.trie.Insert(f, r)
		}
	}
	h.rules.Store(rs)
	for i := range h.shards {
		sh := &h.shards[i]
		sh.mu.Lock()
		for name, s := range sh.m {
			s.mu.Lock()
			if r := rs.match(name); r != nil {
				h.moveLocked(s, r)
			} else {
				h.removeLocked(sh, name, s)
			}
			s.mu.Unlock()
		}
		sh.mu.Unlock()
	}
}

// moveLocked assigns series s to rule r and applies r's point and age limits.
func (h *History) moveLocked(s *series, r *rule) {
	if old := s.rule; old != r {
		cost := s.bytes + seriesOverhead + s.nameLen
		old.topics.Add(-1)
		old.points.Add(-int64(s.n))
		old.bytes.Add(-cost)
		r.topics.Add(1)
		r.points.Add(int64(s.n))
		r.bytes.Add(cost)
		s.rule = r
	}
	h.dropOldest(s, r.maxPoints)
	if len(s.buf) > r.maxPoints {
		s.resize(r.maxPoints)
	}
	if r.maxAge > 0 {
		h.dropOlder(s, h.now().UnixNano()-r.maxAge)
	}
}

// removeLocked deletes a series (shard and series locks held).
func (h *History) removeLocked(sh *shard, name string, s *series) {
	h.dropOldest(s, 0)
	cost := seriesOverhead + int64(len(name))
	h.topics.Add(-1)
	h.bytes.Add(-cost)
	s.rule.topics.Add(-1)
	s.rule.bytes.Add(-cost)
	s.dead = true
	delete(sh.m, name)
}

// Sweep drops points older than their rule's max age and forgets topics
// left without points. Call it periodically: topics that stop publishing
// are otherwise only trimmed when read.
func (h *History) Sweep() {
	now := h.now().UnixNano()
	for i := range h.shards {
		sh := &h.shards[i]
		sh.mu.Lock()
		for name, s := range sh.m {
			s.mu.Lock()
			if s.rule.maxAge > 0 {
				h.dropOlder(s, now-s.rule.maxAge)
			}
			if s.n == 0 {
				h.removeLocked(sh, name, s)
			}
			s.mu.Unlock()
		}
		sh.mu.Unlock()
	}
}

// Rules returns the counters of every active rule, by rule id.
func (h *History) Rules() map[string]RuleStats {
	rs := h.rules.Load()
	out := make(map[string]RuleStats, len(rs.list))
	for _, r := range rs.list {
		out[r.cfg.ID] = RuleStats{Topics: r.topics.Load(), Points: r.points.Load(), Bytes: r.bytes.Load(), Dropped: r.dropped.Load()}
	}
	return out
}

// RuleFor returns the id of the rule that applies to topic name ("" if none).
func (h *History) RuleFor(name string) string {
	if r := h.rules.Load().match(name); r != nil {
		return r.cfg.ID
	}
	return ""
}

// Stats are global counters.
type Stats struct {
	Rules    int    `json:"rules"`
	Topics   int64  `json:"topics"`
	Points   int64  `json:"points"`
	Bytes    int64  `json:"bytes"`
	MaxBytes int64  `json:"max_bytes"`
	Dropped  uint64 `json:"dropped"`
}

// Stats returns global counters.
func (h *History) Stats() Stats {
	return Stats{
		Rules: len(h.rules.Load().list), Topics: h.topics.Load(), Points: h.points.Load(),
		Bytes: h.bytes.Load(), MaxBytes: h.maxBytes, Dropped: h.dropped.Load(),
	}
}

// Info describes the recorded history of one topic.
type Info struct {
	Topic  string `json:"topic"`
	Rule   string `json:"rule"`
	Points int    `json:"points"`
	Bytes  int64  `json:"bytes"`
	Oldest int64  `json:"-"`
	Newest int64  `json:"-"`
}

func (s *series) info(name string, cutoff int64) Info {
	in := Info{Topic: name, Rule: s.rule.cfg.ID, Bytes: s.bytes}
	for i := 0; i < s.n; i++ {
		p := s.at(i)
		if p.time < cutoff {
			continue
		}
		if in.Points == 0 {
			in.Oldest = p.time
		}
		in.Points++
		in.Newest = p.time
	}
	return in
}

// cutoff returns the oldest time still within the rule's max age.
func (h *History) cutoff(r *rule) int64 {
	if r.maxAge == 0 {
		return 0
	}
	return h.now().UnixNano() - r.maxAge
}

// Get returns the points of topic name received in [since, until] (zero
// values: unbounded), oldest first. With limit > 0 only the newest limit
// points are returned; total counts all points in the range. ok is false
// when the topic has no history.
func (h *History) Get(name string, since, until int64, limit int) (pts []Point, total int, info Info, ok bool) {
	sh := h.shard(name)
	sh.mu.RLock()
	s := sh.m[name]
	sh.mu.RUnlock()
	if s == nil {
		return nil, 0, Info{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return nil, 0, Info{}, false
	}
	if c := h.cutoff(s.rule); c > since {
		since = c
	}
	info = s.info(name, since)
	lo, hi := -1, -1
	for i := 0; i < s.n; i++ {
		t := s.at(i).time
		if t < since || (until != 0 && t > until) {
			continue
		}
		if lo < 0 {
			lo = i
		}
		hi = i
		total++
	}
	if total == 0 {
		return []Point{}, 0, info, true
	}
	skip := 0
	if limit > 0 && total > limit {
		skip = total - limit
	}
	pts = make([]Point, 0, total-skip)
	for i := lo; i <= hi; i++ {
		p := s.at(i)
		if p.time < since || (until != 0 && p.time > until) {
			continue
		}
		if skip > 0 {
			skip--
			continue
		}
		pts = append(pts, p.export())
	}
	return pts, total, info, true
}

// List returns the recorded topics matching filter ("" or "#" = all), sorted
// by topic, at most limit (0 = unlimited), plus the number of matches.
func (h *History) List(filter string, limit int) ([]Info, int) {
	all := filter == "" || filter == "#"
	var out []Info
	for i := range h.shards {
		sh := &h.shards[i]
		sh.mu.RLock()
		for name, s := range sh.m {
			if !all && !topic.Match(filter, name) {
				continue
			}
			s.mu.Lock()
			in := s.info(name, h.cutoff(s.rule))
			s.mu.Unlock()
			if in.Points > 0 {
				out = append(out, in)
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

// Delete forgets the history of one topic. It reports whether it existed.
func (h *History) Delete(name string) bool {
	sh := h.shard(name)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	s := sh.m[name]
	if s == nil {
		return false
	}
	s.mu.Lock()
	h.removeLocked(sh, name, s)
	s.mu.Unlock()
	return true
}

// Clear forgets all history (rules are kept).
func (h *History) Clear() {
	for i := range h.shards {
		sh := &h.shards[i]
		sh.mu.Lock()
		for name, s := range sh.m {
			s.mu.Lock()
			h.removeLocked(sh, name, s)
			s.mu.Unlock()
		}
		sh.mu.Unlock()
	}
}
