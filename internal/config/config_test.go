package config

import (
	"os"
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
