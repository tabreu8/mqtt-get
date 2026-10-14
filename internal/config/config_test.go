package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestBrokerFromEnv(t *testing.T) {
	env := map[string]string{
		"MQTT_URL":        "tcp://a:1883, ssl://b:8883",
		"MQTT_TOPICS":     "home/#:1,sensors/+/temp",
		"MQTT_USERNAME":   "u",
		"MQTT_WS_HEADERS": "Authorization: Bearer x",
	}
	Env = func(k string) string { return env[k] }
	defer func() { Env = os.Getenv }()
	b, ok, err := BrokerFromEnv()
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if len(b.URLs) != 2 || b.Subscriptions[0] != (Subscription{"home/#", 1}) || b.Subscriptions[1] != (Subscription{"sensors/+/temp", 0}) {
		t.Fatalf("%+v", b)
	}
	if b.WSHeaders["Authorization"] != "Bearer x" {
		t.Fatal("ws headers")
	}
	r := b.Redact()
	if r.WSHeaders["Authorization"] != Redacted || b.WSHeaders["Authorization"] != "Bearer x" {
		t.Fatal("redact must not modify original")
	}
	r.MergeSecrets(b)
	if r.WSHeaders["Authorization"] != "Bearer x" {
		t.Fatal("merge")
	}
}

func TestValidate(t *testing.T) {
	b := Broker{URLs: []string{"http://x"}}
	b.Normalize()
	if b.Validate() == nil {
		t.Fatal("http scheme must be rejected")
	}
	w := Webhook{URL: "https://x/y", Topics: []string{"a/#"}, Format: "raw", BatchSize: 5}
	w.Normalize()
	if w.Validate() == nil {
		t.Fatal("raw+batch must be rejected")
	}
}

func TestFiltersOverlap(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"factory/#", "energy/#", false},
		{"factory/#", "factory/line1/temp", true},
		{"a/+/c", "a/b/+", true},
		{"a/+/c", "a/b/d", false},
		{"a", "a/#", true},
		{"a", "a/b", false},
		{"a/b", "a/b/c", false},
		{"#", "anything/at/all", true},
		{"+", "a/b", false},
		{"+/+", "a/b", true},
		{"$SYS/#", "#", false},
		{"$SYS/#", "+/broker", false},
		{"$SYS/#", "$SYS/broker/+", true},
		{"home/+/temperature", "home/kitchen/humidity", false},
	}
	for _, c := range cases {
		if got := FiltersOverlap(c.a, c.b); got != c.want {
			t.Errorf("FiltersOverlap(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
		if got := FiltersOverlap(c.b, c.a); got != c.want {
			t.Errorf("FiltersOverlap(%q, %q) = %v, want %v (reversed)", c.b, c.a, got, c.want)
		}
	}
}

func TestSubscriptionsFor(t *testing.T) {
	subs := []Subscription{{"a/#", 0}, {"b/#", 1}, {"c/#", 0}, {"d/#", 2}, {"e/#", 0}}
	names := func(ss []Subscription) string {
		var out []string
		for _, s := range ss {
			out = append(out, s.Filter)
		}
		return strings.Join(out, ",")
	}
	b := Broker{Subscriptions: subs, Connections: 2, SubscriptionMode: SubscriptionSplit}
	if names(b.SubscriptionsFor(0)) != "a/#,c/#,e/#" || names(b.SubscriptionsFor(1)) != "b/#,d/#" {
		t.Fatalf("split: %v / %v", b.SubscriptionsFor(0), b.SubscriptionsFor(1))
	}
	if b.SubscriptionsFor(1)[1].QoS != 2 {
		t.Fatal("qos must be kept")
	}
	b = Broker{Subscriptions: subs[:1], Connections: 3, SharedGroup: "g"}
	for i := 0; i < 3; i++ {
		if names(b.SubscriptionsFor(i)) != "$share/g/a/#" {
			t.Fatalf("shared %d: %v", i, b.SubscriptionsFor(i))
		}
	}
	b = Broker{Subscriptions: subs, Connections: 3}
	if len(b.SubscriptionsFor(0)) != 5 || len(b.SubscriptionsFor(1)) != 0 {
		t.Fatal("auto: only the first connection subscribes")
	}
}

func TestSplitValidation(t *testing.T) {
	b := Broker{URLs: []string{"tcp://x:1883"}, Connections: 2, SubscriptionMode: SubscriptionSplit,
		Subscriptions: []Subscription{{"factory/#", 0}, {"factory/line1/+", 0}}}
	b.Normalize()
	if err := b.Validate(); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("overlap must be rejected: %v", err)
	}
	b.Subscriptions = []Subscription{{"factory/#", 0}, {"energy/#", 0}}
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	b.SharedGroup = "g"
	if err := b.Validate(); err == nil {
		t.Fatal("split + shared group must be rejected")
	}
	b.SharedGroup, b.SubscriptionMode = "", "bogus"
	if err := b.Validate(); err == nil {
		t.Fatal("unknown mode must be rejected")
	}
}

func TestHistoryFromEnv(t *testing.T) {
	env := map[string]string{}
	Env = func(k string) string { return env[k] }
	defer func() { Env = os.Getenv }()
	if _, ok, err := HistoryFromEnv(); ok || err != nil {
		t.Fatal("rule without HISTORY_TOPICS")
	}
	env["HISTORY_TOPICS"] = "home/+/temp, plant/#"
	env["HISTORY_MAX_AGE_SEC"] = "3600"
	env["HISTORY_ON_CHANGE"] = "true"
	r, ok, err := HistoryFromEnv()
	if !ok || err != nil || r.ID != "env" || !r.Env || len(r.Topics) != 2 || r.MaxPoints != DefaultHistoryPoints || r.MaxAgeSec != 3600 || !r.OnChange {
		t.Fatalf("%+v %v", r, err)
	}
	env["HISTORY_TOPICS"] = "bad/#/x"
	if _, _, err := HistoryFromEnv(); err == nil {
		t.Fatal("invalid filter accepted")
	}
}

func TestHistoryRuleValidate(t *testing.T) {
	for _, r := range []HistoryRule{
		{},
		{Topics: []string{"a"}, MaxPoints: MaxHistoryPoints + 1},
		{Topics: []string{"a"}, MaxAgeSec: -1},
	} {
		if r.Validate() == nil {
			t.Errorf("%+v accepted", r)
		}
	}
	r := HistoryRule{Topics: []string{"a/#"}}
	r.Normalize()
	if r.Validate() != nil || r.MaxPoints != DefaultHistoryPoints || !r.IsEnabled() {
		t.Fatalf("%+v", r)
	}
}

func TestSparkplugFromEnv(t *testing.T) {
	env := map[string]string{}
	Env = func(k string) string { return env[k] }
	defer func() { Env = os.Getenv }()
	if _, ok, _ := SparkplugFromEnv(); ok {
		t.Fatal("configured without SPARKPLUG_ENABLED")
	}
	env["SPARKPLUG_ENABLED"] = "true"
	env["SPARKPLUG_PREFIX"] = "/uns/sparkplug/"
	env["SPARKPLUG_COMMANDS"] = "true"
	s, ok, err := SparkplugFromEnv()
	if !ok || err != nil || !s.Enabled || s.Prefix != "uns/sparkplug" || !s.Commands || s.Rebirth || s.RebirthIntervalSec != 30 {
		t.Fatalf("%+v %v", s, err)
	}
	env["SPARKPLUG_COMMANDS_FILTER"] = " spb/a/#, ,spb/+/e/X "
	if s, _, err := SparkplugFromEnv(); err != nil || !reflect.DeepEqual(s.CommandsFilter, []string{"spb/a/#", "spb/+/e/X"}) {
		t.Fatalf("commands filter: %q %v", s.CommandsFilter, err)
	}
	env["SPARKPLUG_COMMANDS_FILTER"] = "spb/#/x"
	if _, _, err := SparkplugFromEnv(); err == nil {
		t.Fatal("invalid commands filter accepted")
	}
	delete(env, "SPARKPLUG_COMMANDS_FILTER")
	env["SPARKPLUG_PREFIX"] = "a/+/b"
	if _, _, err := SparkplugFromEnv(); err == nil {
		t.Fatal("wildcard prefix accepted")
	}
}
