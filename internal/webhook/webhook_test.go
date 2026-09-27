package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

func wait(t *testing.T, f func() bool) {
	t.Helper()
	for i := 0; i < 250 && !f(); i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if !f() {
		t.Fatal("timeout")
	}
}

func TestRawFormatSignatureAndRetry(t *testing.T) {
	var calls atomic.Int32
	var gotBody, gotTopic, gotSig, gotTS atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503) // first attempt fails, must be retried
			return
		}
		b, _ := io.ReadAll(r.Body)
		gotBody.Store(string(b))
		gotTopic.Store(r.Header.Get("X-MQTT-Topic"))
		gotSig.Store(r.Header.Get("X-MQTT-Get-Signature"))
		gotTS.Store(r.Header.Get("X-MQTT-Get-Timestamp"))
	}))
	defer srv.Close()

	d := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer d.Close()
	d.Put(config.Webhook{ID: "h", URL: srv.URL, Topics: []string{"a/+"}, Format: "raw", Secret: "k"})
	d.Dispatch(&store.Entry{Topic: "b/x", Payload: []byte("no")})
	d.Dispatch(&store.Entry{Topic: "a/x", Payload: []byte("hello")})
	wait(t, func() bool { s, _ := d.Stats("h"); return s.Delivered == 1 })

	if gotBody.Load() != "hello" || gotTopic.Load() != "a/x" {
		t.Fatalf("body=%v topic=%v", gotBody.Load(), gotTopic.Load())
	}
	mac := hmac.New(sha256.New, []byte("k"))
	mac.Write([]byte(gotTS.Load().(string) + ".hello"))
	if want := "sha256=" + hex.EncodeToString(mac.Sum(nil)); gotSig.Load() != want {
		t.Fatalf("signature %v want %v", gotSig.Load(), want)
	}
	if s, _ := d.Stats("h"); s.Requests != 2 {
		t.Fatalf("requests = %d, want 2 (one retry)", s.Requests)
	}
}

func TestQueueFullDrops(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	defer srv.Close()
	defer close(block)
	d := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.Put(config.Webhook{ID: "h", URL: srv.URL, Topics: []string{"#"}, QueueSize: 5})
	for i := 0; i < 100; i++ {
		d.Dispatch(&store.Entry{Topic: "t", Payload: []byte("x")}) // never blocks
	}
	if s, _ := d.Stats("h"); s.Dropped < 90 {
		t.Fatalf("dropped = %d", s.Dropped)
	}
}
