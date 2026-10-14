package sparkplug

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tabreu8/mqtt-get/internal/topic"
	spb "github.com/tabreu8/mqtt-get/sparkplug"
)

// Definitions persistence: what births told about each edge node (metric
// names, aliases, types, units and the session's bdSeq) is saved, so that
// after a restart data messages that carry only aliases can be decoded
// right away, without waiting for a rebirth. Restored definitions are
// trusted until the first message that contradicts them.

const snapshotVersion = 1

type snapshot struct {
	Version int            `json:"version"`
	Prefix  string         `json:"prefix"`
	Nodes   []snapshotNode `json:"nodes"`
}

type snapshotNode struct {
	Group   string           `json:"group"`
	Edge    string           `json:"edge"`
	BdSeq   int64            `json:"bd_seq"`
	Metrics []snapshotMetric `json:"metrics"`
	Devices []string         `json:"devices,omitempty"`
}

type snapshotMetric struct {
	Name     string       `json:"name"`
	Device   string       `json:"device,omitempty"`
	Datatype spb.DataType `json:"type"`
	Kind     spb.Kind     `json:"kind"`
	Alias    *uint64      `json:"alias,omitempty"`
	Unit     string       `json:"unit,omitempty"`
}

// Dirty reports whether the definitions changed since the last Save (or
// Load), and clears the flag.
func (t *Translator) Dirty() bool { return t.dirty.Swap(false) }

// Save writes the definitions of every node whose birth was seen to path
// (atomically).
func (t *Translator) Save(path string) error {
	snap := snapshot{Version: snapshotVersion, Prefix: t.prefix, Nodes: []snapshotNode{}}
	for _, n := range t.allNodes() {
		n.mu.Lock()
		if n.born {
			sn := snapshotNode{Group: n.group, Edge: n.edge, BdSeq: n.bdSeq}
			add := func(m *metric) {
				sm := snapshotMetric{Name: m.name, Device: m.device, Datatype: m.datatype, Kind: m.kind, Unit: m.unit}
				if m.hasAlias {
					a := m.alias
					sm.Alias = &a
				}
				sn.Metrics = append(sn.Metrics, sm)
			}
			for _, m := range n.metrics {
				add(m)
			}
			for id, d := range n.devices {
				sn.Devices = append(sn.Devices, id)
				for _, m := range d.metrics {
					add(m)
				}
			}
			snap.Nodes = append(snap.Nodes, sn)
		}
		n.mu.Unlock()
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(&snap); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// Load restores the definitions saved at path, for the nodes not seen yet.
// A missing file is not an error. It returns the number of nodes restored;
// a file saved under another prefix is ignored.
func (t *Translator) Load(path string) (int, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var snap snapshot
	if err := json.NewDecoder(io.LimitReader(f, 1<<30)).Decode(&snap); err != nil {
		return 0, fmt.Errorf("%s: %w", path, err)
	}
	if snap.Version != snapshotVersion {
		return 0, fmt.Errorf("%s: unknown version %d", path, snap.Version)
	}
	if snap.Prefix != t.prefix {
		return 0, nil
	}
	restored := 0
	for _, sn := range snap.Nodes {
		if sn.Group == "" || sn.Edge == "" {
			continue
		}
		n := t.node(sn.Group, sn.Edge)
		if n == nil {
			break
		}
		n.mu.Lock()
		if n.born || !n.lastMsg.IsZero() {
			n.mu.Unlock()
			continue // already heard from: its own messages win
		}
		t.mu.Lock()
		for _, id := range sn.Devices {
			if id != "" {
				n.devices[id] = &device{id: id, metrics: map[string]*metric{}}
			}
		}
		for _, sm := range sn.Metrics {
			defs := n.metrics
			if sm.Device != "" {
				d := n.devices[sm.Device]
				if d == nil {
					d = &device{id: sm.Device, metrics: map[string]*metric{}}
					n.devices[sm.Device] = d
				}
				defs = d.metrics
			}
			if sm.Name == "" || len(defs) >= MaxMetricsPerNode {
				continue
			}
			m := &metric{name: sm.Name, device: sm.Device, datatype: sm.Datatype, kind: sm.Kind, unit: sm.Unit,
				topic: t.metricTopic(n.group, n.edge, sm.Device, sm.Name)}
			if topic.ValidateName(m.topic) != nil {
				continue
			}
			if sm.Alias != nil {
				m.alias, m.hasAlias = *sm.Alias, true
				n.aliases.set(m.alias, m)
			}
			defs[sm.Name] = m
			t.byTopic[m.topic] = m
		}
		t.mu.Unlock()
		n.born, n.restored, n.bdSeq = true, true, sn.BdSeq
		n.mu.Unlock()
		restored++
	}
	return restored, nil
}

// compatible reports whether a value received for m fits the definition
// restored from disk.
func compatible(m *metric, pm *spb.Metric) bool {
	if pm.Datatype != spb.TypeUnknown && pm.Datatype != m.datatype {
		return false
	}
	if pm.IsNull || pm.Value.Kind == m.kind {
		return true
	}
	switch m.kind {
	case spb.KindInt, spb.KindLong:
		return pm.Value.Kind == spb.KindInt || pm.Value.Kind == spb.KindLong
	case spb.KindFloat, spb.KindDouble:
		return pm.Value.Kind == spb.KindFloat || pm.Value.Kind == spb.KindDouble
	}
	return false
}

// checkRestored runs on the first messages from a node whose definitions
// came from disk (lock held). It returns false when they contradict the
// message: the definitions are dropped and the node must be born again.
func (t *Translator) checkRestored(n *node, devID string, p *spb.Payload) bool {
	defs := n.metrics
	if devID != "" {
		if d := n.devices[devID]; d != nil {
			defs = d.metrics
		}
	}
	for i := range p.Metrics {
		pm := &p.Metrics[i]
		var m *metric
		switch {
		case pm.Name != "":
			m = defs[pm.Name]
		case pm.HasAlias:
			if m = n.aliases.get(pm.Alias); m != nil && m.device != devID {
				m = nil
			}
		}
		if m != nil && !compatible(m, pm) {
			t.problem(n, t.clock(), "metric definitions restored from disk don't match %s (%s, %s): waiting for a rebirth",
				describe(m), pm.Datatype, pm.Value.Kind)
			for id, d := range n.devices {
				t.register(n, id, d.metrics, &spb.Payload{})
				delete(n.devices, id)
				n.lastDev = nil
			}
			t.register(n, "", n.metrics, &spb.Payload{})
			n.born, n.restored, n.bdSeq = false, false, -1
			return false
		}
	}
	return true
}
