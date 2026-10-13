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
	datatype uint32
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
	cfg     config.Sparkplug
	publish Publisher
	now     func() time.Time

	mu      sync.RWMutex
	nodes   map[string]*node   // group + "/" + edge
	byTopic map[string]*metric // translated topic -> metric (commands)

	messages, metrics, malformed, dropped, unknown, seqErrors, rebirths, commands atomic.Uint64
}

// New creates a translator. cfg must be normalized and valid.
func New(cfg config.Sparkplug, publish Publisher) *Translator {
	return &Translator{cfg: cfg, publish: publish, now: time.Now,
		nodes: map[string]*node{}, byTopic: map[string]*metric{}}
}

// Config returns the translator's settings.
func (t *Translator) Config() config.Sparkplug { return t.cfg }

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
	b.WriteString(t.cfg.Prefix)
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
	p, err := Decode(e.Payload)
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
	var rebirth bool
	switch typ {
	case NBIRTH:
		out = t.nodeBirth(n, p, &e, now)
	case NDEATH:
		out = t.nodeDeath(n, p, now)
	case DBIRTH:
		rebirth = t.checkSeq(n, p)
		out = t.deviceBirth(n, dev, p, &e, now)
	case DDEATH:
		rebirth = t.checkSeq(n, p)
		out = t.deviceDeath(n, dev, now)
	case NDATA, DDATA:
		rebirth = t.checkSeq(n, p)
		var unknown bool
		out, unknown = t.data(n, dev, p, &e)
		rebirth = rebirth || unknown || !n.born
	}
	if rebirth {
		t.requestRebirthLocked(n, now, false)
	}
	n.mu.Unlock()
	t.metrics.Add(uint64(len(out)))
	return out, true
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
func (t *Translator) checkSeq(n *node, p *Payload) (gap bool) {
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

func bdSeqOf(p *Payload) (int64, bool) {
	for i := range p.Metrics {
		if m := &p.Metrics[i]; m.Name == "bdSeq" && (m.Value.Kind == KindLong || m.Value.Kind == KindInt) {
			return int64(m.Value.U), true
		}
	}
	return 0, false
}

// register forgets the old metric definitions in defs (and their aliases
// and topics) and records the ones of a birth certificate.
func (t *Translator) register(n *node, devID string, defs map[string]*metric, p *Payload) {
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
		if pm.Value.Kind == KindTemplate && pm.Value.Template != nil && pm.Value.Template.IsDefinition {
			continue // a template type (e.g. Ignition's "_types_/…"), not a value
		}
		if len(defs) >= MaxMetricsPerNode {
			t.dropped.Add(1)
			break
		}
		m := &metric{name: pm.Name, device: devID, datatype: pm.Datatype, alias: pm.Alias, hasAlias: pm.HasAlias,
			topic: t.metricTopic(n.group, n.edge, devID, pm.Name)}
		if m.datatype == TypeUnknown {
			m.datatype = inferType(pm.Value)
		}
		for _, k := range []string{"engUnit", "engUnits", "unit", "units"} {
			if u, ok := pm.Properties.Property(k); ok {
				m.unit = u
				break
			}
		}
		if topic.ValidateName(m.topic) != nil {
			continue
		}
		defs[pm.Name] = m
		if m.hasAlias {
			n.aliases[m.alias] = m
		}
		t.byTopic[m.topic] = m
	}
}

func (t *Translator) nodeBirth(n *node, p *Payload, e *store.Entry, now time.Time) []store.Entry {
	// A node birth invalidates every device: they publish new births.
	for id, d := range n.devices {
		t.register(n, id, d.metrics, &Payload{})
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

func (t *Translator) nodeDeath(n *node, p *Payload, now time.Time) []store.Entry {
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

// staleBdSeq reports whether death bdSeq d is older than birth bdSeq b
// (bdSeq counts 0-255 and wraps).
func staleBdSeq(d, b int64) bool {
	diff := (b - d) % 256
	if diff < 0 {
		diff += 256
	}
	return diff > 0 && diff < 128
}

func (t *Translator) deviceBirth(n *node, devID string, p *Payload, e *store.Entry, now time.Time) []store.Entry {
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
func (t *Translator) data(n *node, devID string, p *Payload, e *store.Entry) (out []store.Entry, unknown bool) {
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
				if m.datatype == TypeUnknown {
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
func (t *Translator) values(n *node, devID string, defs map[string]*metric, p *Payload, e *store.Entry) []store.Entry {
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
func (t *Translator) value(m *metric, pm *Metric, e *store.Entry, online bool) (store.Entry, bool) {
	dt := pm.Datatype
	if dt == TypeUnknown {
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
	buf = t.envelopeTail(buf, m, dt, ts, online, pm.IsHistorical, pm.Properties)
	if !pm.IsHistorical {
		m.value, m.ts, m.recv, m.qos, m.hasLast = val, ts, recv, e.QoS, true
	}
	return store.Entry{Topic: m.topic, Payload: buf, QoS: e.QoS, Time: recv}, true
}

// envelopeTail completes {"value":<value> with the metadata:
//
//	,"type":"Float","timestamp":"…","unit":"°C","online":true}
func (t *Translator) envelopeTail(buf []byte, m *metric, dt uint32, ts uint64, online, historical bool, props *PropertySet) []byte {
	buf = append(buf, `,"type":"`...)
	buf = append(buf, TypeName(dt)...)
	buf = append(buf, `","timestamp":`...)
	buf = appendTime(buf, int64(ts))
	if m.unit != "" {
		buf = append(buf, `,"unit":`...)
		buf = store.AppendString(buf, m.unit)
	}
	if historical {
		buf = append(buf, `,"historical":true`...)
	}
	if props != nil && len(props.Keys) > 0 {
		buf = append(buf, `,"properties":`...)
		buf = AppendProperties(buf, props)
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
		buf = t.envelopeTail(buf, m, m.datatype, m.ts, false, false, nil)
		out = append(out, store.Entry{Topic: m.topic, Payload: buf, QoS: m.qos, Time: m.recv})
	}
	return out
}

// --- rebirth requests ---

// RebirthPayload is the NCMD payload asking a node to publish its births.
func RebirthPayload(now time.Time) []byte {
	ms := uint64(now.UnixMilli())
	return Encode(&Payload{Timestamp: ms, HasTimestamp: true, Metrics: []Metric{{
		Name: RebirthMetric, Timestamp: ms, HasTimestamp: true, Datatype: TypeBoolean,
		Value: Value{Kind: KindBool, U: 1},
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
	if (!t.cfg.Rebirth && !force) || t.publish == nil {
		return false
	}
	if !n.rebirthAt.IsZero() && now.Sub(n.rebirthAt) < time.Duration(t.cfg.RebirthIntervalSec)*time.Second {
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

// IsMetricTopic reports whether name is the translated topic of a metric.
func (t *Translator) IsMetricTopic(name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.byTopic[name] != nil
}

// InPrefix reports whether name is under the translated topic prefix.
func (t *Translator) InPrefix(name string) bool {
	return strings.HasPrefix(name, t.cfg.Prefix+"/")
}

// Command converts a write to a translated metric topic into an NCMD or
// DCMD message (topic and Sparkplug payload), typed by the metric's birth.
func (t *Translator) Command(name string, payload []byte) (cmdTopic string, cmd []byte, err error) {
	if !t.cfg.Commands {
		return "", nil, errors.New("writing to Sparkplug metrics is disabled (enable sparkplug commands)")
	}
	t.mu.RLock()
	m := t.byTopic[name]
	t.mu.RUnlock()
	if m == nil {
		return "", nil, fmt.Errorf("%w: %q (no birth certificate seen for it; the edge node must publish its births first)", ErrNotMetric, name)
	}
	group, edge := t.nodeOf(m)
	v, err := ValueFromJSON(m.datatype, payload)
	if err != nil {
		return "", nil, fmt.Errorf("%s (%s metric %q): %w", name, TypeName(m.datatype), m.name, err)
	}
	ms := uint64(t.now().UnixMilli())
	cmd = Encode(&Payload{Timestamp: ms, HasTimestamp: true, Metrics: []Metric{{
		Name: m.name, Timestamp: ms, HasTimestamp: true, Datatype: m.datatype, Value: v,
	}}})
	t.commands.Add(1)
	return CommandTopic(group, edge, m.device), cmd, nil
}

// nodeOf finds the node of a metric (from its translated topic).
func (t *Translator) nodeOf(m *metric) (group, edge string) {
	rest := strings.TrimPrefix(m.topic, t.cfg.Prefix+"/")
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

func metricInfos(defs map[string]*metric) []MetricInfo {
	out := make([]MetricInfo, 0, len(defs))
	for _, m := range defs {
		mi := MetricInfo{Name: m.name, Topic: m.topic, Type: TypeName(m.datatype), Unit: m.unit}
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
			RebirthRequests: n.rebirths, StaleDeaths: n.staleDeaths, Count: len(n.metrics), Devices: []DeviceInfo{}}
		if n.bdSeq >= 0 {
			b := n.bdSeq
			ni.BdSeq = &b
		}
		if metrics {
			ni.Metrics = metricInfos(n.metrics)
		}
		for _, d := range n.devices {
			di := DeviceInfo{ID: d.id, Online: d.online && n.online, BirthAt: tp(d.birthAt), DeathAt: tp(d.deathAt), Count: len(d.metrics)}
			if metrics {
				di.Metrics = metricInfos(d.metrics)
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
	st := Stats{Messages: t.messages.Load(), Metrics: t.metrics.Load(), Malformed: t.malformed.Load(), Dropped: t.dropped.Load(),
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
