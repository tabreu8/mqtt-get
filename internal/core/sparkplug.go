package core

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/sparkplug"
	"github.com/tabreu8/mqtt-get/internal/store"
	"github.com/tabreu8/mqtt-get/internal/topic"
)

// SetEnvSparkplug registers Sparkplug settings from SPARKPLUG_* variables.
// Settings saved through the API take precedence. Call it before Start.
func (s *Service) SetEnvSparkplug(c config.Sparkplug) { s.envSparkplug = &c }

// sparkplugConfig returns the effective settings and where they came from.
func (s *Service) sparkplugConfig() (config.Sparkplug, string) {
	if c, ok := s.state.Sparkplug(); ok {
		return c, "saved"
	}
	if s.envSparkplug != nil {
		return *s.envSparkplug, "env"
	}
	c := config.Sparkplug{}
	c.Normalize()
	return c, "default"
}

// applySparkplug (re)creates the translator. Translated values under a
// prefix that is no longer in use are forgotten.
func (s *Service) applySparkplug(c config.Sparkplug) {
	if old := s.spb.Load(); old != nil && (!c.Enabled || old.Config().Prefix != c.Prefix) {
		res, _ := s.store.Query(old.Config().Prefix+"/#", 0)
		for _, e := range res {
			s.store.Delete(e.Topic)
		}
	}
	s.applyHost(c)
	if !c.Enabled {
		s.spb.Store(nil)
		return
	}
	if old := s.spb.Load(); old != nil && old.SetConfig(c) {
		return // same prefix: keep the births, aliases and node states
	}
	sp := sparkplug.New(c, s.sparkplugPublish)
	s.loadSparkplug(sp)
	s.spb.Store(sp)
}

// applyHost makes mqtt-get a Sparkplug primary host (or stops being one)
// when the host id changes, reconnecting if needed: the STATE will is part
// of the connection.
func (s *Service) applyHost(c config.Sparkplug) {
	id := ""
	if c.Enabled {
		id = c.HostID
	}
	if id == s.hostID {
		return
	}
	s.hostID = id
	s.hostMode.Store(id != "")
	if id == "" {
		s.mqtt.SetPresence(nil)
	} else {
		s.mqtt.SetPresence(s.hostPresence(id))
	}
	if b := s.mqtt.Config(); b.IsConfigured() {
		if err := s.mqtt.Reconnect(); err != nil {
			s.log.Error("sparkplug: reconnecting for the primary host STATE failed", "err", err)
		}
	}
}

// hostPresence is the STATE of primary host id: for every connection a
// will {"online":false,"timestamp":T} and, once subscribed, a birth
// {"online":true,"timestamp":T} with the same T, both QoS 1 and retained.
func (s *Service) hostPresence(id string) *mqttc.Presence {
	stateTopic := sparkplug.StateTopic(id)
	return &mqttc.Presence{
		Build: func() (will, online, offline mqttc.Message) {
			ts := time.Now().UnixMilli()
			s.hostOnline.Store(false)
			s.hostTS.Store(ts)
			msg := func(on bool) mqttc.Message {
				return mqttc.Message{Topic: stateTopic, QoS: 1, Retain: true, Payload: sparkplug.StatePayload(on, ts)}
			}
			return msg(false), msg(true), msg(false)
		},
		Subscriptions: []config.Subscription{{Filter: sparkplug.Namespace + "/#", QoS: 1}, {Filter: stateTopic, QoS: 1}},
		Online: func() {
			s.hostOnline.Store(true)
			s.log.Info("sparkplug: primary host online", "host_id", id, "topic", stateTopic)
		},
	}
}

// hostState handles STATE messages on our own host id: another client (or
// a late will of an older session) saying we are offline is answered by
// publishing our online STATE again.
func (s *Service) hostState(sp *sparkplug.Translator, e *store.Entry) {
	id := sp.Config().HostID
	// Not gated on hostOnline: the offline message can arrive right after
	// our birth, before the broker's acknowledgement of it.
	if id == "" || e.Topic != sparkplug.StateTopic(id) || !s.hostMode.Load() {
		return
	}
	online, ts, ok := sparkplug.ParseState(e.Payload)
	if !ok || online || ts < s.hostTS.Load() {
		return // our birth, or the death of an older session
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.PublishTimeout+time.Second)
		defer cancel()
		if err := s.mqtt.AnnouncePresence(ctx); err != nil {
			s.log.Warn("sparkplug: republishing the primary host STATE failed", "err", err)
		}
	}()
}

// sparkplugPath is where the metric definitions are saved ("" = not
// persisted).
func (s *Service) sparkplugPath(c config.Sparkplug) string {
	if c.Persist == nil || !*c.Persist || s.cfg.DataDir == "" || s.state.Path() == "" {
		return ""
	}
	return filepath.Join(s.cfg.DataDir, "sparkplug.json")
}

// loadSparkplug restores the saved metric definitions into sp.
func (s *Service) loadSparkplug(sp *sparkplug.Translator) {
	p := s.sparkplugPath(sp.Config())
	if p == "" {
		return
	}
	n, err := sp.Load(p)
	switch {
	case err != nil:
		s.log.Error("sparkplug: could not restore the metric definitions", "file", p, "err", err)
	case n > 0:
		s.log.Info("sparkplug: metric definitions restored", "file", p, "nodes", n)
	}
	sp.Dirty() // what was just loaded needs no saving
}

// saveSparkplug saves the metric definitions if they changed.
func (s *Service) saveSparkplug() {
	sp := s.spb.Load()
	if sp == nil {
		return
	}
	p := s.sparkplugPath(sp.Config())
	if p == "" || !sp.Dirty() {
		return
	}
	if err := sp.Save(p); err != nil {
		s.log.Error("sparkplug: could not save the metric definitions", "file", p, "err", err)
	}
}

// sparkplugLoop translates the Sparkplug messages held for reordering once
// their timeout expires, and saves the metric definitions when they change
// (at most every 10 seconds, and on shutdown).
func (s *Service) sparkplugLoop() {
	defer s.bg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	save := time.NewTicker(10 * time.Second)
	defer save.Stop()
	for {
		select {
		case <-t.C:
			if sp := s.spb.Load(); sp != nil {
				for _, e := range sp.Flush() {
					s.ingest(e)
				}
			}
		case <-save.C:
			s.saveSparkplug()
		case <-s.stop:
			s.saveSparkplug()
			return
		}
	}
}

// sparkplugPublish sends a rebirth request without blocking ingest. A
// primary host publishes nothing before its STATE.
func (s *Service) sparkplugPublish(name string, payload []byte) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.PublishTimeout+time.Second)
		defer cancel()
		for s.hostMode.Load() && !s.hostOnline.Load() {
			select {
			case <-ctx.Done():
				s.log.Warn("sparkplug: rebirth request dropped: the primary host STATE is not published yet", "topic", name)
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
		// Sparkplug commands are QoS 0 and never retained.
		if _, err := s.mqtt.PublishMany(ctx, []mqttc.Message{{Topic: name, Payload: payload}}); err != nil {
			s.log.Warn("sparkplug: rebirth request failed", "topic", name, "err", err)
		} else {
			s.log.Info("sparkplug: rebirth requested", "topic", name)
		}
	}()
}

// sparkplugCommands converts publishes to translated metric topics into
// NCMD/DCMD messages. It returns msgs unchanged when none applies.
func (s *Service) sparkplugCommands(msgs []mqttc.Message) ([]mqttc.Message, error) {
	sp := s.spb.Load()
	if sp == nil {
		return msgs, nil
	}
	var out []mqttc.Message
	for i, m := range msgs {
		if !sp.InPrefix(m.Topic) {
			continue
		}
		if out == nil {
			out = append([]mqttc.Message(nil), msgs...)
		}
		if m.Retain {
			return nil, Invalidf("%s: Sparkplug commands are never retained (publish with retain=false)", m.Topic)
		}
		ct, cmd, err := sp.Command(m.Topic, m.Payload)
		if err != nil {
			if errors.Is(err, sparkplug.ErrNotMetric) {
				return nil, &Error{Kind: NotFound, Msg: err.Error()}
			}
			if errors.Is(err, sparkplug.ErrNotWritable) {
				return nil, &Error{Kind: Forbidden, Msg: err.Error()}
			}
			return nil, Invalidf("%s", err.Error())
		}
		out[i] = mqttc.Message{Topic: ct, Payload: cmd} // QoS 0, as the spec requires
	}
	if out == nil {
		return msgs, nil
	}
	return out, nil
}

// SparkplugView is the Sparkplug configuration and state.
type SparkplugView struct {
	Config config.Sparkplug `json:"config"`
	Source string           `json:"source"` // "saved", "env" or "default"
	Stats  *sparkplug.Stats `json:"stats,omitempty"`
	// Subscribed reports whether the broker subscriptions cover spBv1.0/#.
	Subscribed bool `json:"subscribed"`
	// HostOnline tells, for a primary host (host_id set), whether its
	// STATE birth is published on the current connection.
	HostOnline *bool  `json:"host_online,omitempty"`
	Hint       string `json:"hint,omitempty"`
}

// Sparkplug returns the Sparkplug settings and counters.
func (s *Service) Sparkplug() SparkplugView {
	c, src := s.sparkplugConfig()
	v := SparkplugView{Config: c, Source: src, Subscribed: s.subscribesSparkplug()}
	if sp := s.spb.Load(); sp != nil {
		st := sp.Stats()
		v.Stats = &st
		b := s.mqtt.Config()
		if c.HostID != "" {
			on := s.hostOnline.Load() && s.mqtt.Status().Connected
			v.HostOnline = &on
		}
		switch {
		case c.HostID != "" && (!b.UseCleanSession() || b.SessionExpirySec > 0):
			v.Hint = "a Sparkplug primary host must connect with a clean session: set clean_session to true and session_expiry_sec to 0 in the broker settings"
		case c.HostID != "" && b.Will != nil:
			v.Hint = "the broker will is not registered: as a primary host, mqtt-get's will is its STATE death"
		case !v.Subscribed:
			v.Hint = "the broker subscriptions don't include spBv1.0/#: add it to receive Sparkplug messages"
		case b.SharedGroup != "" && c.HostID == "":
			v.Hint = "with a shared subscription group, Sparkplug messages are spread over several connections and a node's messages can arrive out of order or at another subscriber: keep reorder_timeout_ms on, or subscribe to spBv1.0/# without a shared group"
		}
	}
	return v
}

func (s *Service) subscribesSparkplug() bool {
	if c, _ := s.sparkplugConfig(); c.Enabled && c.HostID != "" {
		return true // a primary host subscribes to spBv1.0/# itself
	}
	probe := sparkplug.Namespace + "/g/NDATA/e"
	for _, sub := range s.mqtt.Config().Subscriptions {
		if topic.Match(sub.Filter, probe) {
			return true
		}
	}
	return false
}

// SetSparkplug validates, applies and saves Sparkplug settings.
func (s *Service) SetSparkplug(c config.Sparkplug) (SparkplugView, error) {
	c.Normalize()
	if err := c.Validate(); err != nil {
		return SparkplugView{}, invalid(err)
	}
	s.spbMu.Lock()
	defer s.spbMu.Unlock()
	if err := s.state.SetSparkplug(c); err != nil {
		return SparkplugView{}, err
	}
	s.applySparkplug(c)
	return s.Sparkplug(), nil
}

// SparkplugConfig returns the effective settings, to build a partial update.
func (s *Service) SparkplugConfig() config.Sparkplug {
	c, _ := s.sparkplugConfig()
	return c
}

func (s *Service) translator() (*sparkplug.Translator, error) {
	sp := s.spb.Load()
	if sp == nil {
		return nil, Invalidf("Sparkplug B support is disabled (enable it with SPARKPLUG_ENABLED=true, PUT /api/v1/sparkplug or the MCP tool configure_sparkplug)")
	}
	return sp, nil
}

// SparkplugNodes lists the edge nodes seen (optionally of one group), with
// their devices; with metrics, every metric known from the births.
func (s *Service) SparkplugNodes(group string, metrics bool) ([]sparkplug.NodeInfo, error) {
	sp, err := s.translator()
	if err != nil {
		return nil, err
	}
	return sp.Nodes(group, metrics), nil
}

// SparkplugRebirth asks an edge node to publish its births again.
func (s *Service) SparkplugRebirth(group, edge string) error {
	sp, err := s.translator()
	if err != nil {
		return err
	}
	if !s.mqtt.Status().Connected {
		return &Error{Kind: Unavailable, Msg: mqttc.ErrNotConnected.Error()}
	}
	sent, err := sp.RequestRebirth(group, edge)
	switch {
	case errors.Is(err, sparkplug.ErrUnknownNode):
		return notFound("Sparkplug edge node %s/%s has not been seen", group, edge)
	case err != nil:
		return err
	case !sent:
		return &Error{Kind: Busy, Msg: "a rebirth was requested from this node less than rebirth_interval_sec ago"}
	}
	return nil
}
