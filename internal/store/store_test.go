package store

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func TestStore(t *testing.T) {
	s := New(3)
	s.Set(&Entry{Topic: "a/1", Payload: []byte("x"), Time: 1})
	s.Set(&Entry{Topic: "a/2", Payload: []byte("y"), Time: 1})
	s.Set(&Entry{Topic: "b/1", Payload: []byte("z"), Time: 1})
	if s.Set(&Entry{Topic: "c", Time: 1}) {
		t.Fatal("expected limit to reject new topic")
	}
	if !s.Set(&Entry{Topic: "a/1", Payload: []byte("x2"), Time: 2}) {
		t.Fatal("update of existing topic must succeed at limit")
	}
	s.Set(&Entry{Topic: "a/1", Payload: []byte("old"), Time: 1}) // out of order, ignored
	if e, _ := s.Get("a/1"); string(e.Payload) != "x2" {
		t.Fatalf("got %q", e.Payload)
	}
	res, total := s.Query("a/+", 1)
	if total != 2 || len(res) != 1 || res[0].Topic != "a/1" {
		t.Fatalf("query: %d %v", total, res)
	}
	if !s.Delete("a/1") || s.Len() != 2 {
		t.Fatal("delete")
	}
	s.Clear()
	if s.Len() != 0 {
		t.Fatal("clear")
	}
}

func BenchmarkSetParallel(b *testing.B) {
	s := New(0)
	topics := make([]string, 100000)
	for i := range topics {
		topics[i] = "devices/" + strconv.Itoa(i) + "/state"
	}
	payload := []byte(`{"temp":21.5}`)
	var mu sync.Mutex
	seed := 0
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		mu.Lock()
		i := seed * 7919
		seed++
		mu.Unlock()
		for pb.Next() {
			i++
			s.Set(&Entry{Topic: topics[i%len(topics)], Payload: payload, Time: int64(i)})
		}
	})
}

func BenchmarkGet(b *testing.B) {
	s := New(0)
	s.Set(&Entry{Topic: "devices/1/state", Payload: []byte("x")})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("devices/1/state")
		}
	})
}

func TestAppendJSON(t *testing.T) {
	cases := map[string]string{
		`{"a":1}`:  `"payload":{"a":1},"encoding":"json"`,
		"hi \"x\"": `"payload":"hi \"x\"","encoding":"utf8"`,
		"\xff\x00": `"payload":"/wA=","encoding":"base64"`,
		"":         `"payload":"","encoding":"utf8"`,
	}
	for in, want := range cases {
		got := string(AppendJSON(nil, &Entry{Topic: "t", Payload: []byte(in)}))
		if !strings.Contains(got, want) {
			t.Errorf("%q: got %s want substring %s", in, got, want)
		}
		var v map[string]any
		if err := json.Unmarshal([]byte(got), &v); err != nil {
			t.Errorf("%q: invalid json %s: %v", in, got, err)
		}
	}
}

func TestPayloadRoundTrip(t *testing.T) {
	s := New(0)
	for _, p := range [][]byte{nil, {}, []byte("x"), []byte(strings.Repeat("y", 70000))} {
		s.Set(&Entry{Topic: "t", Payload: p, Time: 1})
		e, _ := s.Get("t")
		if string(e.Payload) != string(p) || len(e.Payload) != len(p) {
			t.Fatalf("payload of len %d came back as len %d", len(p), len(e.Payload))
		}
	}
	if sz := unsafe.Sizeof(slot{}); sz > 32 {
		t.Fatalf("slot is %d bytes, want <= 32", sz)
	}
}

func TestPropsJSONAndExpiry(t *testing.T) {
	e := &Entry{Topic: "t", Payload: []byte("1"), Time: time.Now().Add(-2 * time.Second).UnixNano(), Props: &Props{
		ContentType: "application/json", ResponseTopic: "r/1", CorrelationData: []byte{0xff, 1},
		UserProperties: []UserProperty{{"site", "lisbon"}, {"site", "porto"}}, MessageExpiry: 1, PayloadUTF8: true,
	}}
	got := string(AppendJSON(nil, e))
	want := `"properties":{"content_type":"application/json","response_topic":"r/1","correlation_data_base64":"/wE=","user_properties":[{"key":"site","value":"lisbon"},{"key":"site","value":"porto"}],"message_expiry_sec":1,"payload_format":"utf8"}}`
	if !strings.HasSuffix(got, want) {
		t.Fatalf("got %s", got)
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(got), &v); err != nil {
		t.Fatal(err)
	}
	if !e.Expired(time.Now()) {
		t.Fatal("expired 1s-expiry message received 2s ago")
	}
	if strings.Contains(string(AppendJSON(nil, &Entry{Topic: "t", Props: &Props{}})), "properties") {
		t.Fatal("empty props must not be rendered")
	}
	s := New(0)
	s.Set(e)
	if g, _ := s.Get("t"); g.Props == nil || g.Props.ResponseTopic != "r/1" {
		t.Fatal("props not stored")
	}
}
