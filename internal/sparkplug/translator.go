// Package sparkplug translates Sparkplug B traffic into plain MQTT topics
// with JSON values, and writes to those topics into NCMD/DCMD commands. It
// is mqtt-get's Sparkplug host logic (births, deaths, aliases, sequence
// numbers, rebirth requests); the payload codec is the public package
// github.com/tabreu8/mqtt-get/sparkplug.
package sparkplug

import (
	"errors"
	"fmt"
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
	// last value, to re-publish it marked offline on death
	value   []byte // JSON
	ts      uint64 // metric timestamp (ms)
	recv    int64  // receive time (ns), the entry time
	qos     byte
	hasLast bool
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
	group     string
	edge      string
	online    bool
	born      bool  // a birth has been seen (aliases and types are known)
	bdSeq     int64 // -1 = unknown
	seq       int   // last seq, -1 = unknown
	birthAt   time.Time
	deathAt   time.Time
	lastMsg   time.Time
	metrics   map[string]*metric // node metrics by name
	aliases   map[uint64]*metric // node and device metrics
	devices   map[string]*device
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
	now     func() time.Time

	mu      sync.RWMutex
	nodes   map[string]*node   // group + "/" + edge
	byTopic map[string]*metric // translated topic -> metric (commands)

	messages, metrics, malformed, dropped, problems, unknown, seqErrors, rebirths, commands atomic.Uint64
	pendingNodes                                                                            atomic.Int64 // nodes holding messages
}

// New creates a translator. cfg must be normalized and valid.
func New(cfg config.Sparkplug, publish Publisher) *Translator {
	t := &Translator{prefix: cfg.Prefix, publish: publish, now: time.Now,
		nodes: map[string]*node{}, byTopic: map[string]*metric{}}
	t.cfg.Store(&cfg)
	return t
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

// parseTopic splits spBv1.0/group/TYPE/edge[/device].
func parseTopic(name string) (group, typ, edge, dev string, ok bool) {
	parts := strings.Split(name, "/")
	if len(parts) < 4 || len(parts) > 5 || parts[0] != Namespace {
		return "", "", "", "", false
	}
	group, typ, edge = parts[1], parts[2], parts[3]
	if len(parts) == 5 {
		dev = parts[4]
	}
	if group == "" || edge == "" || (len(parts) == 5 && dev == "") {
		return "", "", "", "", false
	}
	switch typ {
	case NBIRTH, NDEATH, NDATA, NCMD:
		return group, typ, edge, "", len(parts) == 4
	case DBIRTH, DDEATH, DDATA, DCMD:
		return group, typ, edge, dev, len(parts) == 5
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
	group, typ, edge, dev, ok := parseTopic(e.Topic)
	if !ok {
		return nil, false
	}
	t.messages.Add(1)
	if typ == NCMD || typ == DCMD {
		return nil, true // commands from other hosts are not state
	}
	p, err := spb.Decode(e.Payload)
	if err != nil {
		t.malformed.Add(1)
		return nil, true
	}
	n := t.node(group, edge)
	if n == nil {
		t.dropped.Add(1)
		return nil, true
	}
	now := t.now()
	n.mu.Lock()
	n.lastMsg = now
	if err := p.Validate(); err != nil {
		// Still translated (leniently): the problem is reported per node.
		t.problem(n, now, "%s: %v", typ, err)
	}
	switch typ {
	case NBIRTH:
		if p.HasSeq && p.Seq != 0 {
			t.problem(n, now, "NBIRTH with seq %d (must be 0)", p.Seq)
		}
		out = t.nodeBirth(n, p, &e, now)
		// Messages of the new session that overtook the birth (possible
		// with shared subscriptions) follow it; older ones are dropped.
		out = t.drain(n, out, now)
		t.clearPending(n)
	case NDEATH:
		// Messages held for reordering are older than the death.
		out = t.flushPending(n, nil, now)
		out = append(out, t.nodeDeath(n, p, now)...)
	default:
		out = t.sequenced(n, typ, dev, p, &e, now)
	}
	n.mu.Unlock()
	t.metrics.Add(uint64(len(out)))
	return out, true
}

// handle translates a DBIRTH, DDEATH, NDATA or DDATA message (lock held).
func (t *Translator) handle(n *node, typ, dev string, p *spb.Payload, e *store.Entry, now time.Time) (out []store.Entry) {
	rebirth := t.checkSeq(n, p)
	switch typ {
	case DBIRTH:
		out = t.deviceBirth(n, dev, p, e, now)
	case DDEATH:
		out = t.deviceDeath(n, dev, now)
	case NDATA, DDATA:
		var unknown bool
		out, unknown = t.data(n, dev, p, e)
		rebirth = rebirth || unknown || !n.born
	}
	if rebirth {
		t.requestRebirthLocked(n, now, false)
	}
	return out
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
func (t *Translator) sequenced(n *node, typ, dev string, p *spb.Payload, e *store.Entry, now time.Time) []store.Entry {
	timeout := t.reorderTimeout()
	if !p.HasSeq || timeout <= 0 || n.seq < 0 {
		return t.handle(n, typ, dev, p, e, now)
	}
	seq := int(p.Seq % 256)
	d := (seq - (n.seq+1)%256 + 256) % 256
	switch {
	case d == 0: // in order
		n.behind = 0
		return t.drain(n, t.handle(n, typ, dev, p, e, now), now)
	case d < 128: // ahead: hold it
		if n.pending == nil {
			n.pending = map[int]pending{}
		}
		if len(n.pending) == 0 {
			n.pendingSince = now
			t.pendingNodes.Add(1)
		}
		n.pending[seq] = pending{typ: typ, dev: dev, p: p, e: *e}
		return nil
	}
	// Behind: a duplicate, or a message that arrived after its reorder
	// timeout expired. Its values are older than the ones already
	// translated, so it is dropped. Several in a row mean the node
	// restarted its sequence without a birth we saw: follow it.
	n.seqErrors++
	t.seqErrors.Add(1)
	if n.behind++; n.behind < 3 {
		t.requestRebirthLocked(n, now, false)
		return nil
	}
	n.behind = 0
	out := t.flushPending(n, nil, now)
	n.seq = (seq + 255) % 256
	return append(out, t.handle(n, typ, dev, p, e, now)...)
}

func (t *Translator) reorderTimeout() time.Duration {
	if ms := t.cfg.Load().ReorderTimeoutMs; ms != nil {
		return time.Duration(*ms) * time.Millisecond
	}
	return 0
}

// drain handles the held messages that are now in order.
func (t *Translator) drain(n *node, out []store.Entry, now time.Time) []store.Entry {
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
		out = append(out, t.handle(n, pm.typ, pm.dev, pm.p, &pm.e, now)...)
	}
	return out
}

// flushPending handles every held message in sequence order, as when the
// reorder timeout expires: the first one reveals the gap (counted, and a
// rebirth is requested when enabled).
func (t *Translator) flushPending(n *node, out []store.Entry, now time.Time) []store.Entry {
	if len(n.pending) == 0 {
		return out
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
		out = append(out, t.handle(n, pm.typ, pm.dev, pm.p, &pm.e, now)...)
	}
	return out
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
func (t *Translator) Flush() []store.Entry {
	if t.pendingNodes.Load() == 0 {
		return nil
	}
	timeout := t.reorderTimeout()
	now := t.now()
	t.mu.RLock()
	list := make([]*node, 0, 8)
	for _, n := range t.nodes {
		list = append(list, n)
	}
	t.mu.RUnlock()
	var out []store.Entry
	for _, n := range list {
		n.mu.Lock()
		if len(n.pending) > 0 && now.Sub(n.pendingSince) >= timeout {
			before := len(out)
			out = t.flushPending(n, out, now)
			t.metrics.Add(uint64(len(out) - before))
		}
		n.mu.Unlock()
	}
	return out
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

func (t *Translator) node(group, edge string) *node {
	key := group + "/" + edge
	t.mu.RLock()
	n := t.nodes[key]
	t.mu.RUnlock()
	if n != nil {
		return n
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if n = t.nodes[key]; n == nil {
		if len(t.nodes) >= MaxNodes {
			return nil
		}
		n = &node{group: group, edge: edge, bdSeq: -1, seq: -1, metrics: map[string]*metric{},
			aliases: map[uint64]*metric{}, devices: map[string]*device{}}
		t.nodes[key] = n
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
	for name, m := range defs {
		if m.hasAlias && n.aliases[m.alias] == m {
			delete(n.aliases, m.alias)
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
			if other := n.aliases[m.alias]; other != nil && other != m {
				// Aliases must be unique across a node and its devices:
				// the later birth wins.
				t.problem(n, t.now(), "alias %d used by %s and %s", m.alias, describe(other), describe(m))
			}
			n.aliases[m.alias] = m
		}
		t.byTopic[m.topic] = m
	}
}

func (t *Translator) nodeBirth(n *node, p *spb.Payload, e *store.Entry, now time.Time) []store.Entry {
	// A node birth invalidates every device: they publish new births.
	for id, d := range n.devices {
		t.register(n, id, d.metrics, &spb.Payload{})
		delete(n.devices, id)
	}
	t.register(n, "", n.metrics, p)
	n.online, n.born, n.birthAt = true, true, now
	n.bdSeq = -1
	if bd, ok := bdSeqOf(p); ok {
		n.bdSeq = bd
	}
	n.seq = -1
	if p.HasSeq {
		n.seq = int(p.Seq % 256)
	}
	return t.values(n, "", n.metrics, p, e)
}

func (t *Translator) nodeDeath(n *node, p *spb.Payload, now time.Time) []store.Entry {
	// A death left behind by an older session (its will, delivered late)
	// has an older bdSeq: ignore it. Deaths carrying a newer bdSeq are
	// accepted: they can't come from an older session, and some edge nodes
	// send the next session's bdSeq on a clean disconnect.
	if bd, ok := bdSeqOf(p); ok && n.bdSeq >= 0 && staleBdSeq(bd, n.bdSeq) {
		n.staleDeaths++
		return nil
	}
	if !n.online {
		return nil
	}
	n.online, n.deathAt = false, now
	out := t.offline(n.metrics, nil)
	for _, d := range n.devices {
		if d.online {
			d.online, d.deathAt = false, now
			out = t.offline(d.metrics, out)
		}
	}
	return out
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

func (t *Translator) deviceBirth(n *node, devID string, p *spb.Payload, e *store.Entry, now time.Time) []store.Entry {
	d := n.devices[devID]
	if d == nil {
		d = &device{id: devID, metrics: map[string]*metric{}}
		n.devices[devID] = d
	}
	t.register(n, devID, d.metrics, p)
	d.online, d.birthAt = true, now
	return t.values(n, devID, d.metrics, p, e)
}

func (t *Translator) deviceDeath(n *node, devID string, now time.Time) []store.Entry {
	d := n.devices[devID]
	if d == nil || !d.online {
		return nil
	}
	d.online, d.deathAt = false, now
	return t.offline(d.metrics, nil)
}

// data translates NDATA/DDATA. unknown reports metrics that couldn't be
// resolved (alias without a birth).
func (t *Translator) data(n *node, devID string, p *spb.Payload, e *store.Entry) (out []store.Entry, unknown bool) {
	var defs map[string]*metric
	if devID == "" {
		defs = n.metrics
	} else {
		d := n.devices[devID]
		if d == nil {
			d = &device{id: devID, metrics: map[string]*metric{}, online: true}
			n.devices[devID] = d
		}
		defs = d.metrics
	}
	out = make([]store.Entry, 0, len(p.Metrics))
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
			m = n.aliases[pm.Alias]
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
		if ent, ok := t.value(m, pm, e, true); ok {
			out = append(out, ent)
		}
	}
	return out, unknown
}

// values translates the metrics of a birth certificate.
func (t *Translator) values(n *node, devID string, defs map[string]*metric, p *spb.Payload, e *store.Entry) []store.Entry {
	out := make([]store.Entry, 0, len(p.Metrics))
	for i := range p.Metrics {
		pm := &p.Metrics[i]
		m := defs[pm.Name]
		if m == nil {
			continue
		}
		if ent, ok := t.value(m, pm, e, true); ok {
			out = append(out, ent)
		}
	}
	return out
}

// value builds the translated entry of one metric value:
//
//	{"value":72.4,"type":"Float","timestamp":"…","unit":"°C","online":true}
func (t *Translator) value(m *metric, pm *spb.Metric, e *store.Entry, online bool) (store.Entry, bool) {
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
	buf := make([]byte, 0, 128)
	buf = append(buf, `{"value":`...)
	start := len(buf)
	if pm.IsNull {
		buf = append(buf, "null"...)
	} else {
		buf = AppendValue(buf, dt, pm.Value)
	}
	val := buf[start:len(buf):len(buf)] // shared, never modified: entries are immutable
	buf = t.envelopeTail(buf, m, dt, ts, online, pm)
	if !pm.IsHistorical && !pm.IsTransient {
		m.value, m.ts, m.recv, m.qos, m.hasLast = val, ts, recv, e.QoS, true
	}
	// Transient values are streamed (watchers, webhooks) but not stored.
	return store.Entry{Topic: m.topic, Payload: buf, QoS: e.QoS, Time: recv, Transient: pm.IsTransient}, true
}

// envelopeTail completes {"value":<value> with the metadata (pm is the
// metric as received, nil when republishing the last value):
//
//	,"type":"Float","timestamp":"…","unit":"°C","online":true}
func (t *Translator) envelopeTail(buf []byte, m *metric, dt spb.DataType, ts uint64, online bool, pm *spb.Metric) []byte {
	buf = append(buf, `,"type":"`...)
	buf = append(buf, dt.String()...)
	buf = append(buf, `","timestamp":`...)
	buf = appendTime(buf, int64(ts))
	if m.unit != "" {
		buf = append(buf, `,"unit":`...)
		buf = store.AppendString(buf, m.unit)
	}
	if pm != nil && pm.IsHistorical {
		buf = append(buf, `,"historical":true`...)
	}
	if pm != nil && pm.IsTransient {
		buf = append(buf, `,"transient":true`...)
	}
	if pm != nil && pm.Properties != nil && len(pm.Properties.Keys) > 0 {
		buf = append(buf, `,"properties":`...)
		buf = AppendProperties(buf, pm.Properties)
	}
	buf = append(buf, `,"online":`...)
	buf = strconv.AppendBool(buf, online)
	return append(buf, '}')
}

// offline re-publishes the last value of every metric marked online:false,
// keeping its time so the age still tells when it was last updated.
func (t *Translator) offline(defs map[string]*metric, out []store.Entry) []store.Entry {
	for _, m := range defs {
		if !m.hasLast {
			continue
		}
		buf := append(make([]byte, 0, len(m.value)+96), `{"value":`...)
		buf = append(buf, m.value...)
		buf = t.envelopeTail(buf, m, m.datatype, m.ts, false, nil)
		out = append(out, store.Entry{Topic: m.topic, Payload: buf, QoS: m.qos, Time: m.recv})
	}
	return out
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
	t.mu.RLock()
	n := t.nodes[group+"/"+edge]
	t.mu.RUnlock()
	if n == nil {
		return false, ErrUnknownNode
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return t.requestRebirthLocked(n, t.now(), true), nil
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
	ms := uint64(t.now().UnixMilli())
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
	t.mu.RLock()
	list := make([]*node, 0, len(t.nodes))
	for _, n := range t.nodes {
		if group == "" || n.group == group {
			list = append(list, n)
		}
	}
	t.mu.RUnlock()
	out := make([]NodeInfo, 0, len(list))
	for _, n := range list {
		n.mu.Lock()
		ni := NodeInfo{Group: n.group, Edge: n.edge, Online: n.online, BirthSeen: n.born, BirthAt: tp(n.birthAt),
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
	st := Stats{Messages: t.messages.Load(), Metrics: t.metrics.Load(), Malformed: t.malformed.Load(), Dropped: t.dropped.Load(), Problems: t.problems.Load(),
		Unknown: t.unknown.Load(), SeqErrors: t.seqErrors.Load(), RebirthRequests: t.rebirths.Load(), Commands: t.commands.Load()}
	t.mu.RLock()
	list := make([]*node, 0, len(t.nodes))
	for _, n := range t.nodes {
		list = append(list, n)
	}
	t.mu.RUnlock()
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
