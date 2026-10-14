package core

import (
	"context"
	"errors"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/mqttc"
	"github.com/tabreu8/mqtt-get/internal/sparkplug"
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
	if !c.Enabled {
		s.spb.Store(nil)
		return
	}
	if old := s.spb.Load(); old != nil && old.SetConfig(c) {
		return // same prefix: keep the births, aliases and node states
	}
	s.spb.Store(sparkplug.New(c, s.sparkplugPublish))
}

// sparkplugPublish sends a rebirth request without blocking ingest.
func (s *Service) sparkplugPublish(name string, payload []byte) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.PublishTimeout+time.Second)
		defer cancel()
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
	Subscribed bool   `json:"subscribed"`
	Hint       string `json:"hint,omitempty"`
}

// Sparkplug returns the Sparkplug settings and counters.
func (s *Service) Sparkplug() SparkplugView {
	c, src := s.sparkplugConfig()
	v := SparkplugView{Config: c, Source: src, Subscribed: s.subscribesSparkplug()}
	if sp := s.spb.Load(); sp != nil {
		st := sp.Stats()
		v.Stats = &st
		if !v.Subscribed {
			v.Hint = "the broker subscriptions don't include spBv1.0/#: add it to receive Sparkplug messages"
		}
	}
	return v
}

func (s *Service) subscribesSparkplug() bool {
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
