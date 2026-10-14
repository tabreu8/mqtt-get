// Package sparkplug translates Sparkplug B traffic into plain MQTT topics
// with JSON values, and writes to those topics into NCMD/DCMD commands. It
// is mqtt-get's Sparkplug host logic (births, deaths, aliases, sequence
// numbers, rebirth requests); the payload codec is the public package
// github.com/tabreu8/mqtt-get/sparkplug.
package sparkplug

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
	"github.com/tabreu8/mqtt-get/internal/topic"
	spb "github.com/tabreu8/mqtt-get/sparkplug"
)

// Namespace is the Sparkplug B topic namespace.
const Namespace = "spBv1.0"

// Message types.
const (
	NBIRTH = "NBIRTH"
	NDEATH = "NDEATH"
	DBIRTH = "DBIRTH"
	DDEATH = "DDEATH"
	NDATA  = "NDATA"
	DDATA  = "DDATA"
	NCMD   = "NCMD"
	DCMD   = "DCMD"
)

// Memory guards against floods of made-up node names or metric names
// (anyone who can publish to the broker can send Sparkplug messages).
const (
	MaxNodes          = 100_000
	MaxMetricsPerNode = 100_000
)

// RebirthMetric is the node control metric that asks an edge node to
// publish its births again.
const RebirthMetric = "Node Control/Rebirth"

// metric is what a birth certificate tells about one metric.
type metric struct {
	name     string
	device   string // "" for node metrics
	datatype spb.DataType
	// kind is the protobuf field the edge node used for the value in its
	// birth (implementations differ for some types, e.g. UInt32 in
	// int_value or long_value): writes use the same one.
	kind     spb.Kind
	alias    uint64
	hasAlias bool
	topic    string // translated topic
	unit     string
	unitJSON string    // `,"unit":"…"`, built on first use
	ref      store.Ref // where topic is stored
	// last value, to re-publish it marked offline on death
	value   []byte // JSON
	ts      uint64 // metric timestamp (ms)
	recv    int64  // receive time (ns), the entry time
	qos     byte
	hasLast bool
}

// aliasTable maps aliases to metrics: small aliases (edge nodes number
// them from 0 or 1) index a slice, which is faster than a map; others use a
// map.
type aliasTable struct {
	small []*metric
	big   map[uint64]*metric
}

const maxSmallAlias = 1 << 16

func (a *aliasTable) get(alias uint64) *metric {
	if alias < uint64(len(a.small)) {
		return a.small[alias]
	}
	if a.big != nil && alias >= maxSmallAlias {
		return a.big[alias]
	}
	return nil
}

// set records m for alias (nil removes it).
func (a *aliasTable) set(alias uint64, m *metric) {
	if alias < maxSmallAlias {
		if alias >= uint64(len(a.small)) {
			if m == nil {
				return
			}
			a.small = append(a.small, make([]*metric, int(alias)+1-len(a.small))...)
		}
		a.small[alias] = m
		return
	}
	if m == nil {
		delete(a.big, alias)
		return
	}
	if a.big == nil {
		a.big = map[uint64]*metric{}
	}
	a.big[alias] = m
}

type device struct {
	id      string
	online  bool
	birthAt time.Time
	deathAt time.Time
	metrics map[string]*metric // by name
}

type node struct {
	mu        sync.Mutex
	shard     uint32 // index into Translator.shards and .ctr
	next      *node  // next node with the same hash
	group     string
	edge      string
	online    bool
	born      bool   // a birth has been seen (aliases and types are known)
	restored  bool   // the definitions came from disk, not from a birth yet
	birthTS   uint64 // payload timestamp of the last NBIRTH
	bdSeq     int64  // -1 = unknown
	seq       int    // last seq, -1 = unknown
	birthAt   time.Time
	deathAt   time.Time
	lastMsg   time.Time
	metrics   map[string]*metric // node metrics by name
	aliases   aliasTable         // node and device metrics
	devices   map[string]*device
	lastDev   *device // the device of the last device message
	rebirthAt time.Time

	seqErrors, unknown, rebirths, staleDeaths uint64

	// Messages held until the sequence gap before them is filled (by seq),
	// since pendingSince; behind counts messages dropped in a row as late.
	pending      map[int]pending
	pendingSince time.Time
	behind       int

	// problems counts protocol violations (payloads failing validation,
	// NBIRTH seq other than 0, alias conflicts); lastProblem describes
	// the latest.
	problems      uint64
	lastProblem   string
	lastProblemAt time.Time
}

// Publisher sends a message to the broker (rebirth requests). It must not
// block the caller for long: the translator calls it from the ingest path.
type Publisher func(topic string, payload []byte)

// Stats are translator counters.
type Stats struct {
	Messages        uint64 `json:"messages"`         // Sparkplug messages processed
	Metrics         uint64 `json:"metrics"`          // metric values translated
	Malformed       uint64 `json:"malformed"`        // payloads that couldn't be decoded
	Dropped         uint64 `json:"dropped"`          // messages or metrics over the memory limits
	Problems        uint64 `json:"problems"`         // protocol violations (see NodeInfo.LastProblem)
	Unknown         uint64 `json:"unknown_metrics"`  // aliases with no birth seen
	SeqErrors       uint64 `json:"seq_errors"`       // out-of-order sequence numbers
	RebirthRequests uint64 `json:"rebirth_requests"` // NCMD rebirth requests sent
	Commands        uint64 `json:"commands"`         // NCMD/DCMD writes sent
	Nodes           int    `json:"nodes"`
	NodesOnline     int    `json:"nodes_online"`
}

// Translator turns Sparkplug B messages into plain topics with JSON values
// and translates writes to those topics into NCMD/DCMD commands.
type Translator struct {
	cfg     atomic.Pointer[config.Sparkplug]
	prefix  string // fixed for the translator's life
	publish Publisher
	now     func() time.Time // nil: the real clock (tests set it)

	// Nodes are spread over shards, so that ingest workers looking up
	// different nodes don't contend on one lock.
	shards    [nodeShards]nodeShard
	nodeCount atomic.Int64
	// ctr counts messages and values per shard: one counter shared by
	// all ingest workers would bounce between their cores.
	ctr [nodeShards]shardCounters
	// payloads recycles decoded payloads, scratch the buffers values are
	// written in.
	payloads, scratch sync.Pool

	mu      sync.RWMutex       // byTopic and metric registration
	byTopic map[string]*metric // translated topic -> metric (commands)

	malformed, dropped, problems, unknown, seqErrors, rebirths, commands atomic.Uint64
	pendingNodes                                                         atomic.Int64 // nodes holding messages
	dirty                                                                atomic.Bool  // definitions changed since the last Save
}

// New creates a translator. cfg must be normalized and valid.
func New(cfg config.Sparkplug, publish Publisher) *Translator {
	t := &Translator{prefix: cfg.Prefix, publish: publish, byTopic: map[string]*metric{}}
	for i := range t.shards {
		t.shards[i].m = map[uint64]*node{}
	}
	t.payloads.New = func() any { return new(spb.Payload) }
	t.scratch.New = func() any { b := make([]byte, 0, 1024); return &b }
	t.cfg.Store(&cfg)
	return t
}

const nodeShards = 64

type nodeShard struct {
	mu sync.RWMutex
	m  map[uint64]*node // by nodeHash; colliding nodes chain through next
	_  [40]byte         // keep shards on separate cache lines
}

type shardCounters struct {
	messages, metrics atomic.Uint64
	_                 [48]byte
}

// nodeHash identifies an edge node (the runtime's string hash); the low
// bits pick its shard.
func nodeHash(group, edge string) uint64 {
	return maphash.String(nodeSeed, group)*31 + maphash.String(nodeSeed, edge)
}

// find returns node group/edge of chain n.
func (n *node) find(group, edge string) *node {
	for ; n != nil; n = n.next {
		if n.group == group && n.edge == edge {
			return n
		}
	}
	return nil
}

var nodeSeed = maphash.MakeSeed()

// Route returns a number identifying the edge node of a node or device
// message (NBIRTH … DCMD), the same for every message of the node, so that
// a caller can spread nodes over workers while keeping each node's
// messages in order. ok is false for other topics (STATE, …).
func Route(topic string) (h uint32, ok bool) {
	group, _, edge, _, ok := parseTopic(topic)
	if !ok {
		return 0, false
	}
	return uint32(nodeHash(group, edge) >> 32), true
}

// lookup returns a known node, or nil.
func (t *Translator) lookup(group, edge string) *node {
	h := nodeHash(group, edge)
	sh := &t.shards[h%nodeShards]
	sh.mu.RLock()
	n := sh.m[h].find(group, edge)
	sh.mu.RUnlock()
	return n
}

// allNodes returns every node (a snapshot of the list).
func (t *Translator) allNodes() []*node {
	list := make([]*node, 0, t.nodeCount.Load())
	for i := range t.shards {
		sh := &t.shards[i]
		sh.mu.RLock()
		for _, n := range sh.m {
			for ; n != nil; n = n.next {
				list = append(list, n)
			}
		}
		sh.mu.RUnlock()
	}
	return list
}

// clock returns the current time (tests set now).
func (t *Translator) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// clockAt is clock, but uses the receive time ns (unix nanoseconds) when
// it is known.
func (t *Translator) clockAt(ns int64) time.Time {
	if t.now != nil || ns == 0 {
		return t.clock()
	}
	return time.Unix(0, ns)
}

// Config returns the translator's settings.
func (t *Translator) Config() config.Sparkplug { return *t.cfg.Load() }

// SetConfig changes the settings, keeping what the translator knows about
// the edge nodes. It returns false (and changes nothing) when the prefix
// differs: that needs a new translator.
func (t *Translator) SetConfig(cfg config.Sparkplug) bool {
	if cfg.Prefix != t.prefix {
		return false
	}
	t.cfg.Store(&cfg)
	return true
}

// IsSparkplug reports whether name is in the Sparkplug B namespace.
func IsSparkplug(name string) bool {
	return len(name) > len(Namespace) && name[len(Namespace)] == '/' && name[:len(Namespace)] == Namespace
}

// parseTopic splits spBv1.0/group/TYPE/edge[/device] (without allocating:
// the parts are substrings of name).
func parseTopic(name string) (group, typ, edge, dev string, ok bool) {
	if !IsSparkplug(name) {
		return "", "", "", "", false
	}
	rest := name[len(Namespace)+1:]
	var parts [4]string
	n := 0
	for n < 4 {
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			parts[n] = rest
			n++
			rest = ""
			break
		}
		if n == 3 {
			return "", "", "", "", false // more than 5 levels
		}
		parts[n] = rest[:i]
		rest = rest[i+1:]
		n++
	}
	if rest != "" || n < 3 {
		return "", "", "", "", false
	}
	group, typ, edge = parts[0], parts[1], parts[2]
	if n == 4 {
		dev = parts[3]
	}
	if group == "" || edge == "" || (n == 4 && dev == "") {
		return "", "", "", "", false
	}
	switch typ {
	case NBIRTH, NDEATH, NDATA, NCMD:
		return group, typ, edge, "", n == 3
	case DBIRTH, DDEATH, DDATA, DCMD:
		return group, typ, edge, dev, n == 4
	}
	return "", "", "", "", false
}

// sanitize makes a metric name usable in a topic name.
func sanitize(s string) string {
	if !strings.ContainsAny(s, "+#\x00") {
		return s
	}
	return strings.Map(func(r rune) rune {
		if r == '+' || r == '#' || r == 0 {
			return '_'
		}
		return r
	}, s)
}

func (t *Translator) metricTopic(group, edge, dev, name string) string {
	var b strings.Builder
	b.WriteString(t.prefix)
	b.WriteByte('/')
	b.WriteString(group)
	b.WriteByte('/')
	b.WriteString(edge)
	if dev != "" {
		b.WriteByte('/')
		b.WriteString(dev)
	}
	b.WriteByte('/')
	b.WriteString(sanitize(name))
	return b.String()
}

// Process translates one Sparkplug message. handled is false for topics the
// translator does not handle (STATE and anything outside the namespace),
// which are then stored as-is. The returned entries are the translated
// metric values.
func (t *Translator) Process(e store.Entry) (out []store.Entry, handled bool) {
	tr, handled := t.ProcessAppend(e, nil)
	return entries(tr), handled
}

// Translated is a translated metric value: the entry to store, and the
// reference to store it through (store.SetRef).
type Translated struct {
	Entry store.Entry
	Ref   *store.Ref
}

func entries(tr []Translated) []store.Entry {
	if tr == nil {
		return nil
	}
	out := make([]store.Entry, len(tr))
	for i := range tr {
		out[i] = tr[i].Entry
	}
	return out
}

// ProcessAppend is Process appending the translated values to out, so
// that a caller translating many messages can reuse one slice.
func (t *Translator) ProcessAppend(e store.Entry, out []Translated) ([]Translated, bool) {
	group, typ, edge, dev, ok := parseTopic(e.Topic)
	if !ok {
		return out, false
	}
	h := nodeHash(group, edge)
	t.ctr[h%nodeShards].messages.Add(1)
	if typ == NCMD || typ == DCMD {
		return out, true // commands from other hosts are not state
	}
	p := t.payloads.Get().(*spb.Payload)
	if err := spb.DecodeInto(p, e.Payload); err != nil {
		t.payloads.Put(p)
		t.malformed.Add(1)
		return out, true
	}
	n := t.nodeHashed(h, group, edge)
	if n == nil {
		t.payloads.Put(p)
		t.dropped.Add(1)
		return out, true
	}
	em := emit{out: slices.Grow(out, len(p.Metrics))}
	before := len(out)
	scratch := t.scratch.Get().(*[]byte)
	em.block = (*scratch)[:0]
	// The receive time stands for now: it was taken once per socket read,
	// and reading the clock again costs (a lot, on some VMs).
	now := t.clockAt(e.Time)
	n.mu.Lock()
	n.lastMsg = now
	if err := p.Validate(); err != nil {
		// Still translated (leniently): the problem is reported per node.
		t.problem(n, now, "%s: %v", typ, err)
	}
	switch typ {
	case NBIRTH:
		if bd, ok := bdSeqOf(p); ok && n.online && n.born && !n.restored && n.bdSeq == bd && n.seq == 0 && p.HasSeq && p.Seq == 0 && p.HasTimestamp && p.Timestamp == n.birthTS {
			// The same birth again, with nothing in between: a duplicate
			// delivery (overlapping subscriptions, QoS 1 resend).
			break
		}
		if p.HasSeq && p.Seq != 0 {
			t.problem(n, now, "NBIRTH with seq %d (must be 0)", p.Seq)
		}
		t.nodeBirth(&em, n, p, &e, now)
		// Messages of the new session that overtook the birth (possible
		// with shared subscriptions) follow it; older ones are dropped.
		t.drain(&em, n, now)
		t.clearPending(n)
	case NDEATH:
		// Messages held for reordering are older than the death.
		t.flushPending(&em, n, now)
		t.nodeDeath(&em, n, p, now)
	default:
		t.sequenced(&em, n, typ, dev, p, &e, now)
	}
	n.mu.Unlock()
	if !em.keep && cap(p.Metrics) <= 1024 {
		t.payloads.Put(p)
	}
	if cap(em.block) <= 64<<10 {
		*scratch = em.block[:0]
		t.scratch.Put(scratch)
	}
	t.ctr[n.shard].metrics.Add(uint64(len(em.out) - before))
	return em.out, true
}

// emit collects the values translated from one message.
type emit struct {
	out   []Translated
	block []byte // scratch space to write a value in
	keep  bool   // the payload is held for reordering: don't recycle it
	// the last timestamp formatted, reused by the message's other values
	tsMs  uint64
	tsLen int
	ts    [40]byte
}

// appendTime writes ms as an RFC 3339 JSON string, reusing the previous
// formatting when the time is the same.
func (em *emit) appendTime(buf []byte, ms uint64) []byte {
	if em.tsLen == 0 || em.tsMs != ms {
		em.tsLen = copy(em.ts[:], appendTime(em.ts[:0], int64(ms)))
		em.tsMs = ms
	}
	return append(buf, em.ts[:em.tsLen]...)
}

// handle translates a DBIRTH, DDEATH, NDATA or DDATA message (lock held).
func (t *Translator) handle(em *emit, n *node, typ, dev string, p *spb.Payload, e *store.Entry, now time.Time) {
	rebirth := t.checkSeq(n, p)
	if n.restored && (typ == NDATA || typ == DDATA) && t.checkRestored(n, dev, p) {
		// Back after a restart: the node is evidently online. A rebirth
		// confirms the definitions and refreshes every value.
		n.online = true
		if d := n.devices[dev]; d != nil {
			d.online = true
		}
		rebirth = true
	}
	switch typ {
	case DBIRTH:
		t.deviceBirth(em, n, dev, p, e, now)
	case DDEATH:
		// The device is offline as of the DDEATH's own timestamp.
		at := now
		if p.HasTimestamp {
			at = time.UnixMilli(int64(p.Timestamp))
		}
		t.deviceDeath(em, n, dev, at)
	case NDATA, DDATA:
		unknown := t.data(em, n, dev, p, e)
		rebirth = rebirth || unknown || !n.born
	}
	if rebirth {
		t.requestRebirthLocked(n, now, false)
	}
}

// pending is a message held until the ones before it arrive.
type pending struct {
	typ, dev string
	p        *spb.Payload
	e        store.Entry
}

// sequenced handles a message that carries a sequence number. With a
// reorder timeout, a message that arrives ahead of its turn (a gap in the
// sequence) is held until the missing ones arrive, or until the timeout
// expires (see Flush).
func (t *Translator) sequenced(em *emit, n *node, typ, dev string, p *spb.Payload, e *store.Entry, now time.Time) {
	if p.HasSeq && n.seq >= 0 && int(p.Seq%256) == n.seq {
		return // the last message again: a duplicate delivery
	}
	timeout := t.reorderTimeout()
	if !p.HasSeq || timeout <= 0 || n.seq < 0 {
		t.handle(em, n, typ, dev, p, e, now)
		return
	}
	seq := int(p.Seq % 256)
	d := (seq - (n.seq+1)%256 + 256) % 256
	switch {
	case d == 0: // in order
		n.behind = 0
		t.handle(em, n, typ, dev, p, e, now)
		t.drain(em, n, now)
		return
	case d < 128: // ahead: hold it
		if n.pending == nil {
			n.pending = map[int]pending{}
		}
		if len(n.pending) == 0 {
			n.pendingSince = now
			t.pendingNodes.Add(1)
		}
		n.pending[seq] = pending{typ: typ, dev: dev, p: p, e: *e}
		em.keep = true
		return
	}
	// Behind: a duplicate, or a message that arrived after its reorder
	// timeout expired. Its values are older than the ones already
	// translated, so it is dropped. Several in a row mean the node
	// restarted its sequence without a birth we saw: follow it.
	n.seqErrors++
	t.seqErrors.Add(1)
	if n.behind++; n.behind < 3 {
		t.requestRebirthLocked(n, now, false)
		return
	}
	n.behind = 0
	t.flushPending(em, n, now)
	n.seq = (seq + 255) % 256
	t.handle(em, n, typ, dev, p, e, now)
}

func (t *Translator) reorderTimeout() time.Duration {
	if ms := t.cfg.Load().ReorderTimeoutMs; ms != nil {
		return time.Duration(*ms) * time.Millisecond
	}
	return 0
}

// drain handles the held messages that are now in order.
func (t *Translator) drain(em *emit, n *node, now time.Time) {
	for len(n.pending) > 0 {
		next := (n.seq + 1) % 256
		pm, ok := n.pending[next]
		if !ok {
			break
		}
		delete(n.pending, next)
		if len(n.pending) == 0 {
			t.pendingNodes.Add(-1)
		}
		t.handle(em, n, pm.typ, pm.dev, pm.p, &pm.e, now)
	}
}

// flushPending handles every held message in sequence order, as when the
// reorder timeout expires: the first one reveals the gap (counted, and a
// rebirth is requested when enabled).
func (t *Translator) flushPending(em *emit, n *node, now time.Time) {
	if len(n.pending) == 0 {
		return
	}
	base := (n.seq + 1) % 256
	seqs := make([]int, 0, len(n.pending))
	for s := range n.pending {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return (seqs[i]-base+256)%256 < (seqs[j]-base+256)%256 })
	list := n.pending
	n.pending = nil
	t.pendingNodes.Add(-1)
	for _, s := range seqs {
		pm := list[s]
		t.handle(em, n, pm.typ, pm.dev, pm.p, &pm.e, now)
	}
}

func (t *Translator) clearPending(n *node) {
	if len(n.pending) > 0 {
		n.pending = nil
		t.pendingNodes.Add(-1)
	}
}

// Flush handles the messages held for reordering whose timeout has
// expired, and returns their translated values. Call it periodically
// (every 100 ms or so) when a reorder timeout is set.
func (t *Translator) Flush() []store.Entry { return entries(t.FlushTranslated()) }

// FlushTranslated is Flush returning the store references too.
func (t *Translator) FlushTranslated() []Translated {
	if t.pendingNodes.Load() == 0 {
		return nil
	}
	timeout := t.reorderTimeout()
	now := t.clock()
	var em emit
	for _, n := range t.allNodes() {
		n.mu.Lock()
		if len(n.pending) > 0 && now.Sub(n.pendingSince) >= timeout {
			before := len(em.out)
			t.flushPending(&em, n, now)
			t.ctr[n.shard].metrics.Add(uint64(len(em.out) - before))
		}
		n.mu.Unlock()
	}
	return em.out
}

// problem records a protocol violation of node n (lock held).
func (t *Translator) problem(n *node, now time.Time, format string, a ...any) {
	msg := fmt.Sprintf(format, a...)
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i] + " (and more)"
	}
	if len(msg) > 300 {
		msg = msg[:300] + "…"
	}
	n.problems++
	n.lastProblem, n.lastProblemAt = msg, now
	t.problems.Add(1)
}

// node returns the node group/edge, creating it (nil when MaxNodes are
// known).
func (t *Translator) node(group, edge string) *node {
	return t.nodeHashed(nodeHash(group, edge), group, edge)
}

func (t *Translator) nodeHashed(h uint64, group, edge string) *node {
	sh := &t.shards[h%nodeShards]
	sh.mu.RLock()
	n := sh.m[h].find(group, edge)
	sh.mu.RUnlock()
	if n != nil {
		return n
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if n = sh.m[h].find(group, edge); n == nil {
		if t.nodeCount.Load() >= MaxNodes {
			return nil
		}
		// The names may be views into a message buffer: keep copies.
		n = &node{shard: uint32(h % nodeShards), group: strings.Clone(group), edge: strings.Clone(edge), bdSeq: -1, seq: -1,
			metrics: map[string]*metric{}, devices: map[string]*device{}, next: sh.m[h]}
		sh.m[h] = n
		t.nodeCount.Add(1)
	}
	return n
}

// checkSeq verifies the message sequence number (0-255, one counter per
// edge node across all its messages except NDEATH). A gap means messages
// were lost: values may be stale, so a rebirth is wanted.
func (t *Translator) checkSeq(n *node, p *spb.Payload) (gap bool) {
	if !p.HasSeq {
		return false
	}
	seq := int(p.Seq % 256)
	if n.seq >= 0 && seq != (n.seq+1)%256 {
		n.seqErrors++
		t.seqErrors.Add(1)
		gap = true
	}
	n.seq = seq
	return gap
}

func bdSeqOf(p *spb.Payload) (int64, bool) {
	for i := range p.Metrics {
		if m := &p.Metrics[i]; m.Name == "bdSeq" && (m.Value.Kind == spb.KindLong || m.Value.Kind == spb.KindInt) {
			return int64(m.Value.Uint), true
		}
	}
	return 0, false
}

// register forgets the old metric definitions in defs (and their aliases
// and topics) and records the ones of a birth certificate.
func (t *Translator) register(n *node, devID string, defs map[string]*metric, p *spb.Payload) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dirty.Store(true)
	for name, m := range defs {
		if m.hasAlias && n.aliases.get(m.alias) == m {
			n.aliases.set(m.alias, nil)
		}
		if t.byTopic[m.topic] == m {
			delete(t.byTopic, m.topic)
		}
		delete(defs, name)
	}
	for i := range p.Metrics {
		pm := &p.Metrics[i]
		if pm.Name == "" {
			continue
		}
		if pm.Value.Kind == spb.KindTemplate && pm.Value.Template != nil && pm.Value.Template.IsDefinition {
			continue // a template type (e.g. Ignition's "_types_/…"), not a value
		}
		if len(defs) >= MaxMetricsPerNode {
			t.dropped.Add(1)
			break
		}
		m := &metric{name: pm.Name, device: devID, datatype: pm.Datatype, kind: pm.Value.Kind, alias: pm.Alias, hasAlias: pm.HasAlias,
			topic: t.metricTopic(n.group, n.edge, devID, pm.Name)}
		if m.datatype == spb.TypeUnknown {
			m.datatype = inferType(pm.Value)
		}
		for _, k := range []string{"engUnit", "engUnits", "unit", "units"} {
			if u, ok := property(pm.Properties, k); ok {
				m.unit = u
				break
			}
		}
		if topic.ValidateName(m.topic) != nil {
			continue
		}
		defs[pm.Name] = m
		if m.hasAlias {
			if other := n.aliases.get(m.alias); other != nil && other != m {
				// Aliases must be unique across a node and its devices:
				// the later birth wins.
				t.problem(n, t.clock(), "alias %d used by %s and %s", m.alias, describe(other), describe(m))
			}
			n.aliases.set(m.alias, m)
		}
		t.byTopic[m.topic] = m
	}
}

func (t *Translator) nodeBirth(em *emit, n *node, p *spb.Payload, e *store.Entry, now time.Time) {
	// A node birth invalidates every device: they publish new births.
	for id, d := range n.devices {
		t.register(n, id, d.metrics, &spb.Payload{})
		delete(n.devices, id)
	}
	n.lastDev = nil
	t.register(n, "", n.metrics, p)
	n.online, n.born, n.restored, n.birthAt, n.birthTS = true, true, false, now, p.Timestamp
	n.bdSeq = -1
	if bd, ok := bdSeqOf(p); ok {
		n.bdSeq = bd
	}
	n.seq = -1
	if p.HasSeq {
		n.seq = int(p.Seq % 256)
	}
	t.values(em, n.metrics, p, e)
}

func (t *Translator) nodeDeath(em *emit, n *node, p *spb.Payload, now time.Time) {
	// A death left behind by an older session (its will, delivered late)
	// has an older bdSeq: ignore it. Deaths carrying a newer bdSeq are
	// accepted: they can't come from an older session, and some edge nodes
	// send the next session's bdSeq on a clean disconnect.
	if bd, ok := bdSeqOf(p); ok && n.bdSeq >= 0 && staleBdSeq(bd, n.bdSeq) {
		n.staleDeaths++
		return
	}
	if !n.online {
		return
	}
	n.online, n.deathAt = false, now
	t.offline(em, n.metrics)
	for _, d := range n.devices {
		if d.online {
			d.online, d.deathAt = false, now
			t.offline(em, d.metrics)
		}
	}
}

func describe(m *metric) string {
	if m.device == "" {
		return fmt.Sprintf("node metric %q", m.name)
	}
	return fmt.Sprintf("%q of device %s", m.name, m.device)
}

// staleBdSeq reports whether death bdSeq d is older than birth bdSeq b
// (bdSeq counts 0-255 and wraps).
func staleBdSeq(d, b int64) bool {
	diff := (b - d) % 256
	if diff < 0 {
		diff += 256
	}
	return diff > 0 && diff < 128
}

func (t *Translator) deviceBirth(em *emit, n *node, devID string, p *spb.Payload, e *store.Entry, now time.Time) {
	d := n.devices[devID]
	if d == nil {
		d = &device{id: strings.Clone(devID), metrics: map[string]*metric{}}
		n.devices[d.id] = d
	}
	t.register(n, d.id, d.metrics, p)
	d.online, d.birthAt = true, now
	t.values(em, d.metrics, p, e)
}

func (t *Translator) deviceDeath(em *emit, n *node, devID string, now time.Time) {
	d := n.devices[devID]
	if d == nil || !d.online {
		return
	}
	d.online, d.deathAt = false, now
	t.offline(em, d.metrics)
}

// data translates NDATA/DDATA. unknown reports metrics that couldn't be
// resolved (alias without a birth).
func (t *Translator) data(em *emit, n *node, devID string, p *spb.Payload, e *store.Entry) (unknown bool) {
	var defs map[string]*metric
	if devID == "" {
		defs = n.metrics
	} else {
		d := n.lastDev
		if d == nil || d.id != devID {
			if d = n.devices[devID]; d == nil {
				d = &device{id: strings.Clone(devID), metrics: map[string]*metric{}, online: true}
				n.devices[d.id] = d
			}
			n.lastDev = d
		}
		devID, defs = d.id, d.metrics
	}
	for i := range p.Metrics {
		pm := &p.Metrics[i]
		var m *metric
		switch {
		case pm.Name != "":
			m = defs[pm.Name]
			if m == nil && len(defs) >= MaxMetricsPerNode {
				t.dropped.Add(1)
				continue
			}
			if m == nil {
				// No birth seen: translate it anyway, typed by its value.
				m = &metric{name: pm.Name, device: devID, datatype: pm.Datatype,
					topic: t.metricTopic(n.group, n.edge, devID, pm.Name)}
				if m.datatype == spb.TypeUnknown {
					m.datatype = inferType(pm.Value)
				}
				if topic.ValidateName(m.topic) != nil {
					continue
				}
				defs[pm.Name] = m
			}
		case pm.HasAlias:
			m = n.aliases.get(pm.Alias)
			if m != nil && m.device != devID {
				m = nil
			}
		}
		if m == nil {
			n.unknown++
			t.unknown.Add(1)
			unknown = true
			continue
		}
		t.value(em, m, pm, e, true)
	}
	return unknown
}

// values translates the metrics of a birth certificate.
func (t *Translator) values(em *emit, defs map[string]*metric, p *spb.Payload, e *store.Entry) {
	for i := range p.Metrics {
		pm := &p.Metrics[i]
		if m := defs[pm.Name]; m != nil {
			t.value(em, m, pm, e, true)
		}
	}
}

// value translates one metric value:
//
//	{"value":72.4,"type":"Float","timestamp":"…","unit":"°C","online":true}
func (t *Translator) value(em *emit, m *metric, pm *spb.Metric, e *store.Entry, online bool) {
	dt := pm.Datatype
	if dt == spb.TypeUnknown {
		dt = m.datatype
	}
	ts := pm.Timestamp
	if !pm.HasTimestamp {
		ts = uint64(time.Unix(0, e.Time).UnixMilli())
	}
	recv := e.Time
	if pm.IsHistorical {
		// Backfilled data: keep its own time, so it never replaces a newer
		// value and lands at the right place in history.
		recv = time.UnixMilli(int64(ts)).UnixNano()
	}
	// Written in a reused scratch buffer, then copied into a payload of
	// exactly its size: one small allocation per value, and nothing else
	// kept alive by it.
	buf := append(em.block[:0], `{"value":`...)
	vstart := len(buf)
	if pm.IsNull {
		buf = append(buf, "null"...)
	} else {
		buf = AppendValue(buf, dt, pm.Value)
	}
	vend := len(buf)
	buf = t.envelopeTail(em, buf, m, dt, ts, online, pm)
	em.block = buf
	payload := make([]byte, len(buf))
	copy(payload, buf)
	if !pm.IsHistorical && !pm.IsTransient {
		// Shared, never modified (entries are immutable).
		m.value, m.ts, m.recv, m.qos, m.hasLast = payload[vstart:vend:vend], ts, recv, e.QoS, true
	}
	// Transient values are streamed (watchers, webhooks) but not stored.
	em.out = append(em.out, Translated{store.Entry{Topic: m.topic, Payload: payload, QoS: e.QoS, Time: recv, Transient: pm.IsTransient}, &m.ref})
}

// envelopeTail completes {"value":<value> with the metadata (pm is the
// metric as received, nil when republishing the last value):
//
//	,"type":"Float","timestamp":"…","unit":"°C","online":true}
func (t *Translator) envelopeTail(em *emit, buf []byte, m *metric, dt spb.DataType, ts uint64, online bool, pm *spb.Metric) []byte {
	buf = append(buf, typeTag(dt)...)
	buf = em.appendTime(buf, ts)
	if m.unit != "" {
		if m.unitJSON == "" {
			m.unitJSON = string(store.AppendString([]byte(`,"unit":`), m.unit))
		}
		buf = append(buf, m.unitJSON...)
	}
	if pm != nil {
		if pm.IsHistorical {
			buf = append(buf, `,"historical":true`...)
		}
		if pm.IsTransient {
			buf = append(buf, `,"transient":true`...)
		}
		if pm.Metadata != nil && hasMetadata(pm.Metadata) {
			buf = append(buf, `,"metadata":`...)
			buf = appendMetadata(buf, pm.Metadata)
		}
		if pm.Properties != nil && len(pm.Properties.Keys) > 0 {
			buf = append(buf, `,"properties":`...)
			buf = AppendProperties(buf, pm.Properties)
		}
	}
	if online {
		return append(buf, `,"online":true}`...)
	}
	return append(buf, `,"online":false}`...)
}

// typeTags are `,"type":"<name>","timestamp":` for every data type.
var typeTags = func() (tags [64]string) {
	for i := range tags {
		tags[i] = `,"type":"` + spb.DataType(i).String() + `","timestamp":`
	}
	return
}()

func typeTag(dt spb.DataType) string {
	if int(dt) < len(typeTags) {
		return typeTags[dt]
	}
	return `,"type":"` + dt.String() + `","timestamp":`
}

func hasMetadata(md *spb.MetaData) bool {
	return md.IsMultiPart || md.ContentType != "" || md.Size != 0 || md.Seq != 0 || md.FileName != "" ||
		md.FileType != "" || md.MD5 != "" || md.Description != ""
}

// appendMetadata writes the metadata of a value (files and multi-part
// values) as a JSON object, leaving out empty fields.
func appendMetadata(buf []byte, md *spb.MetaData) []byte {
	buf = append(buf, '{')
	start := len(buf)
	str := func(k, v string) {
		if v != "" {
			if len(buf) > start {
				buf = append(buf, ',')
			}
			buf = append(append(append(buf, '"'), k...), `":`...)
			buf = store.AppendString(buf, v)
		}
	}
	num := func(k string, v uint64) {
		if len(buf) > start {
			buf = append(buf, ',')
		}
		buf = append(append(append(buf, '"'), k...), `":`...)
		buf = strconv.AppendUint(buf, v, 10)
	}
	str("file_name", md.FileName)
	str("file_type", md.FileType)
	str("content_type", md.ContentType)
	if md.Size > 0 {
		num("size", md.Size)
	}
	str("md5", md.MD5)
	str("description", md.Description)
	if md.IsMultiPart {
		if len(buf) > start {
			buf = append(buf, ',')
		}
		buf = append(buf, `"is_multi_part":true`...)
		num("seq", md.Seq)
	} else if md.Seq > 0 {
		num("seq", md.Seq)
	}
	return append(buf, '}')
}

// offline re-publishes the last value of every metric marked online:false,
// keeping its time so the age still tells when it was last updated.
func (t *Translator) offline(em *emit, defs map[string]*metric) {
	need := 0
	for _, m := range defs {
		if m.hasLast {
			need += len(m.value) + 96 + len(m.unit)*2
		}
	}
	// One block for all of them: they are all replaced at the next birth.
	block := make([]byte, 0, need)
	for _, m := range defs {
		if !m.hasLast {
			continue
		}
		start := len(block)
		block = append(block, `{"value":`...)
		block = append(block, m.value...)
		block = t.envelopeTail(em, block, m, m.datatype, m.ts, false, nil)
		em.out = append(em.out, Translated{store.Entry{Topic: m.topic, Payload: block[start:len(block):len(block)], QoS: m.qos, Time: m.recv}, &m.ref})
	}
}

// property returns the string value of property key (case-insensitive).
func property(ps *spb.PropertySet, key string) (string, bool) {
	if ps == nil {
		return "", false
	}
	for i, k := range ps.Keys {
		if strings.EqualFold(k, key) && i < len(ps.Values) && ps.Values[i].Value.Kind == spb.KindString {
			return ps.Values[i].Value.Str, true
		}
	}
	return "", false
}

// --- rebirth requests ---

// RebirthPayload is the NCMD payload asking a node to publish its births.
func RebirthPayload(now time.Time) []byte {
	ms := uint64(now.UnixMilli())
	return spb.Encode(&spb.Payload{Timestamp: ms, HasTimestamp: true, Metrics: []spb.Metric{{
		Name: RebirthMetric, Timestamp: ms, HasTimestamp: true, Datatype: spb.TypeBoolean,
		Value: spb.Value{Kind: spb.KindBool, Bool: true},
	}}})
}

// StateTopic returns spBv1.0/STATE/<hostID>, the topic of a primary host
// application's STATE.
func StateTopic(hostID string) string { return Namespace + "/STATE/" + hostID }

// StatePayload returns a STATE payload: {"online":true,"timestamp":1760000000000}.
func StatePayload(online bool, ts int64) []byte {
	b := strconv.AppendBool([]byte(`{"online":`), online)
	b = append(b, `,"timestamp":`...)
	b = strconv.AppendInt(b, ts, 10)
	return append(b, '}')
}

// ParseState decodes a STATE payload.
func ParseState(b []byte) (online bool, ts int64, ok bool) {
	var st struct {
		Online    *bool  `json:"online"`
		Timestamp *int64 `json:"timestamp"`
	}
	if json.Unmarshal(b, &st) != nil || st.Online == nil || st.Timestamp == nil {
		return false, 0, false
	}
	return *st.Online, *st.Timestamp, true
}

// CommandTopic returns spBv1.0/group/NCMD/edge or .../DCMD/edge/device.
func CommandTopic(group, edge, dev string) string {
	if dev == "" {
		return Namespace + "/" + group + "/" + NCMD + "/" + edge
	}
	return Namespace + "/" + group + "/" + DCMD + "/" + edge + "/" + dev
}

// requestRebirthLocked sends a rebirth request when automatic requests are
// enabled (or force), at most once per interval per node.
func (t *Translator) requestRebirthLocked(n *node, now time.Time, force bool) bool {
	cfg := t.cfg.Load()
	if (!cfg.Rebirth && !force) || t.publish == nil {
		return false
	}
	if !n.rebirthAt.IsZero() && now.Sub(n.rebirthAt) < time.Duration(cfg.RebirthIntervalSec)*time.Second {
		return false
	}
	n.rebirthAt = now
	n.rebirths++
	t.rebirths.Add(1)
	t.publish(CommandTopic(n.group, n.edge, ""), RebirthPayload(now))
	return true
}

// ErrUnknownNode is returned for nodes that haven't been seen.
var ErrUnknownNode = errors.New("unknown Sparkplug edge node")

// RequestRebirth asks a node to publish its births now (ignoring the
// automatic-request setting, still rate limited). It reports whether a
// request was sent.
func (t *Translator) RequestRebirth(group, edge string) (bool, error) {
	if t.publish == nil {
		return false, errors.New("not connected")
	}
	n := t.lookup(group, edge)
	if n == nil {
		return false, ErrUnknownNode
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return t.requestRebirthLocked(n, t.clock(), true), nil
}

// --- commands ---

// ErrNotMetric is returned when writing to a topic that isn't a known
// Sparkplug metric.
var ErrNotMetric = errors.New("not a known Sparkplug metric")

// ErrNotWritable is returned when writing to a metric that commands_filter
// doesn't allow.
var ErrNotWritable = errors.New("writing to this Sparkplug metric is not allowed")

// writable reports whether commands may write to the metric topic name.
func (t *Translator) writable(name string) bool {
	cfg := t.cfg.Load()
	if !cfg.Commands {
		return false
	}
	if len(cfg.CommandsFilter) == 0 {
		return true
	}
	for _, f := range cfg.CommandsFilter {
		if topic.Match(f, name) {
			return true
		}
	}
	return false
}

// IsMetricTopic reports whether name is the translated topic of a metric.
func (t *Translator) IsMetricTopic(name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.byTopic[name] != nil
}

// InPrefix reports whether name is under the translated topic prefix.
func (t *Translator) InPrefix(name string) bool {
	return strings.HasPrefix(name, t.prefix+"/")
}

// Command converts a write to a translated metric topic into an NCMD or
// DCMD message (topic and Sparkplug payload), typed by the metric's birth.
func (t *Translator) Command(name string, payload []byte) (cmdTopic string, cmd []byte, err error) {
	if !t.cfg.Load().Commands {
		return "", nil, errors.New("writing to Sparkplug metrics is disabled (enable sparkplug commands)")
	}
	t.mu.RLock()
	m := t.byTopic[name]
	t.mu.RUnlock()
	if m == nil {
		return "", nil, fmt.Errorf("%w: %q (no birth certificate seen for it; the edge node must publish its births first)", ErrNotMetric, name)
	}
	if !t.writable(name) {
		return "", nil, fmt.Errorf("%w: %q is not in sparkplug commands_filter", ErrNotWritable, name)
	}
	group, edge := t.nodeOf(m)
	v, err := ValueFromJSON(m.datatype, payload)
	if err != nil {
		return "", nil, fmt.Errorf("%s (%s metric %q): %w", name, m.datatype.String(), m.name, err)
	}
	v = sameKind(v, m.datatype, m.kind)
	ms := uint64(t.clock().UnixMilli())
	cmd = spb.Encode(&spb.Payload{Timestamp: ms, HasTimestamp: true, Metrics: []spb.Metric{{
		Name: m.name, Timestamp: ms, HasTimestamp: true, Datatype: m.datatype, Value: v,
	}}})
	t.commands.Add(1)
	return CommandTopic(group, edge, m.device), cmd, nil
}

// sameKind re-encodes v in the protobuf field the edge node used for the
// metric in its birth, when both fields can hold the value: integers in
// int_value or long_value, floating point in float_value or double_value.
func sameKind(v spb.Value, dt spb.DataType, birth spb.Kind) spb.Value {
	switch {
	case v.Kind == spb.KindInt && birth == spb.KindLong:
		if dt == spb.TypeInt8 || dt == spb.TypeInt16 || dt == spb.TypeInt32 {
			v.Uint = uint64(int64(int32(uint32(v.Uint)))) // sign-extend to 64 bits
		}
		v.Kind = spb.KindLong
	case v.Kind == spb.KindLong && birth == spb.KindInt && v.Uint <= 1<<32-1:
		v.Kind = spb.KindInt
	case v.Kind == spb.KindFloat && birth == spb.KindDouble:
		v.Kind = spb.KindDouble
	case v.Kind == spb.KindDouble && birth == spb.KindFloat:
		v.Kind, v.Float = spb.KindFloat, float64(float32(v.Float))
	}
	return v
}

// nodeOf finds the node of a metric (from its translated topic).
func (t *Translator) nodeOf(m *metric) (group, edge string) {
	rest := strings.TrimPrefix(m.topic, t.prefix+"/")
	group, rest, _ = strings.Cut(rest, "/")
	edge, _, _ = strings.Cut(rest, "/")
	return group, edge
}

// --- inspection ---

// MetricInfo describes a metric known from a birth certificate.
type MetricInfo struct {
	Name  string  `json:"name"`
	Topic string  `json:"topic"`
	Type  string  `json:"type"`
	Alias *uint64 `json:"alias,omitempty"`
	Unit  string  `json:"unit,omitempty"`
	// Writable: publishing to Topic sends a command (commands enabled and
	// allowed by commands_filter).
	Writable bool `json:"writable,omitempty"`
}

// DeviceInfo describes a device.
type DeviceInfo struct {
	ID      string       `json:"id"`
	Online  bool         `json:"online"`
	BirthAt *time.Time   `json:"birth_at,omitempty"`
	DeathAt *time.Time   `json:"death_at,omitempty"`
	Metrics []MetricInfo `json:"metrics,omitempty"`
	Count   int          `json:"metric_count"`
}

// NodeInfo describes an edge node.
type NodeInfo struct {
	Group           string       `json:"group"`
	Edge            string       `json:"edge_node"`
	Online          bool         `json:"online"`
	BirthSeen       bool         `json:"birth_seen"`
	Restored        bool         `json:"restored,omitempty"` // definitions restored from disk, no birth since
	BdSeq           *int64       `json:"bd_seq,omitempty"`
	BirthAt         *time.Time   `json:"birth_at,omitempty"`
	DeathAt         *time.Time   `json:"death_at,omitempty"`
	LastMessage     time.Time    `json:"last_message"`
	SeqErrors       uint64       `json:"seq_errors"`
	Unknown         uint64       `json:"unknown_metrics"`
	RebirthRequests uint64       `json:"rebirth_requests"`
	StaleDeaths     uint64       `json:"stale_deaths,omitempty"`
	Problems        uint64       `json:"problems"`
	LastProblem     string       `json:"last_problem,omitempty"`
	LastProblemAt   *time.Time   `json:"last_problem_at,omitempty"`
	Metrics         []MetricInfo `json:"metrics,omitempty"`
	Count           int          `json:"metric_count"`
	Devices         []DeviceInfo `json:"devices"`
}

func tp(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func (t *Translator) metricInfos(defs map[string]*metric) []MetricInfo {
	out := make([]MetricInfo, 0, len(defs))
	for _, m := range defs {
		mi := MetricInfo{Name: m.name, Topic: m.topic, Type: m.datatype.String(), Unit: m.unit, Writable: t.writable(m.topic)}
		if m.hasAlias {
			a := m.alias
			mi.Alias = &a
		}
		out = append(out, mi)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Nodes lists the known edge nodes (optionally of one group), sorted. With
// metrics, every metric definition is included.
func (t *Translator) Nodes(group string, metrics bool) []NodeInfo {
	list := t.allNodes()
	out := make([]NodeInfo, 0, len(list))
	for _, n := range list {
		if group != "" && n.group != group {
			continue
		}
		n.mu.Lock()
		ni := NodeInfo{Group: n.group, Edge: n.edge, Online: n.online, BirthSeen: n.born, Restored: n.restored, BirthAt: tp(n.birthAt),
			DeathAt: tp(n.deathAt), LastMessage: n.lastMsg.UTC(), SeqErrors: n.seqErrors, Unknown: n.unknown,
			RebirthRequests: n.rebirths, StaleDeaths: n.staleDeaths, Problems: n.problems,
			LastProblem: n.lastProblem, LastProblemAt: tp(n.lastProblemAt), Count: len(n.metrics), Devices: []DeviceInfo{}}
		if n.bdSeq >= 0 {
			b := n.bdSeq
			ni.BdSeq = &b
		}
		if metrics {
			ni.Metrics = t.metricInfos(n.metrics)
		}
		for _, d := range n.devices {
			di := DeviceInfo{ID: d.id, Online: d.online && n.online, BirthAt: tp(d.birthAt), DeathAt: tp(d.deathAt), Count: len(d.metrics)}
			if metrics {
				di.Metrics = t.metricInfos(d.metrics)
			}
			ni.Devices = append(ni.Devices, di)
		}
		n.mu.Unlock()
		sort.Slice(ni.Devices, func(i, j int) bool { return ni.Devices[i].ID < ni.Devices[j].ID })
		out = append(out, ni)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].Edge < out[j].Edge
	})
	return out
}

// Stats returns counters.
func (t *Translator) Stats() Stats {
	st := Stats{Malformed: t.malformed.Load(), Dropped: t.dropped.Load(), Problems: t.problems.Load(),
		Unknown: t.unknown.Load(), SeqErrors: t.seqErrors.Load(), RebirthRequests: t.rebirths.Load(), Commands: t.commands.Load()}
	for i := range t.ctr {
		st.Messages += t.ctr[i].messages.Load()
		st.Metrics += t.ctr[i].metrics.Load()
	}
	list := t.allNodes()
	st.Nodes = len(list)
	for _, n := range list {
		n.mu.Lock()
		if n.online {
			st.NodesOnline++
		}
		n.mu.Unlock()
	}
	return st
}
