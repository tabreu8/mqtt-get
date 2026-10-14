// Package tck runs the host application tests of the Eclipse Sparkplug TCK
// (Test Compatibility Kit) against mqtt-get as a primary host application.
//
// The TCK is a HiveMQ broker extension: tests are started by publishing to
// SPARKPLUG_TCK/TEST_CONTROL, it plays the edge nodes, asks the operator to
// act or to confirm what the host shows (SPARKPLUG_TCK/CONSOLE_PROMPT) and
// publishes the results (SPARKPLUG_TCK/RESULT). Here the operator is this
// test: it acts and checks through mqtt-get's REST API.
//
// It is skipped unless SPARKPLUG_TCK_BROKER is the URL of a broker running
// the TCK extension (which must listen on localhost:1883); run.sh downloads
// the TCK, starts one and runs the test:
//
//	test/tck/run.sh
package tck

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/httpapi"
	"github.com/tabreu8/mqtt-get/internal/state"
	spb "github.com/tabreu8/mqtt-get/sparkplug"
)

const (
	hostID         = "mqttget"
	group          = "tck"
	edge           = "tck_edge"
	device         = "tck_device"
	key            = "tck-key"
	reorderTimeout = 2000 // ms
)

type driver struct {
	t   *testing.T
	cli mqtt.Client

	mu       sync.Mutex
	results  chan string
	onPrompt func(prompt string) string // the reply ("" for none)
	ddeathTS int64                      // timestamp of the last DDEATH payload

	host    *host
	version uint
}

type host struct {
	svc  *core.Service
	http *httptest.Server
}

func TestHostApplication(t *testing.T) {
	broker := os.Getenv("SPARKPLUG_TCK_BROKER")
	if broker == "" {
		t.Skip("set SPARKPLUG_TCK_BROKER to a broker running the Sparkplug TCK extension (see run.sh)")
	}
	for _, c := range []struct {
		name    string
		version uint
		client  string
	}{{"MQTT 3.1.1", 4, ""}, {"MQTT 5", 5, ""}, {"paho MQTT 3.1.1", 4, "paho"}, {"paho MQTT 5", 5, "paho"}} {
		t.Run(c.name, func(t *testing.T) {
			d := &driver{t: t, results: make(chan string, 10), version: c.version}
			d.connect(broker)
			d.suite(broker, config.Broker{URLs: []string{broker}, ClientID: "mqtt-get-tck-" + strings.ReplaceAll(strings.ToLower(c.name), " ", "-"), ProtocolVersion: c.version, Client: c.client})
		})
	}
}

func (d *driver) suite(broker string, b config.Broker) {
	// The session establishment test watches the host connect, so it
	// starts before mqtt-get does.
	d.run("SessionEstablishmentTest", nil, func() { d.host = startHost(d.t, b) })
	defer func() {
		if d.host != nil {
			d.host.svc.Close()
		}
	}()

	d.run("SendCommandTest", d.sendCommands, nil, group, edge, device)
	d.run("EdgeSessionTerminationTest", d.checkTermination, nil, group, edge, device)
	d.run("MessageOrderingTest", nil, nil, group, edge, device, fmt.Sprint(reorderTimeout))

	// Last: a graceful shutdown. The test didn't see the host connect, so
	// it is given the host's client id.
	d.run("SessionTerminationTest", nil, func() {
		d.host.svc.Close()
		d.host = nil
	}, b.ClientID)
}

func (d *driver) connect(broker string) {
	o := mqtt.NewClientOptions().AddBroker(broker).SetClientID("mqtt-get-tck-driver").SetCleanSession(true)
	d.cli = mqtt.NewClient(o)
	if tok := d.cli.Connect(); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		d.t.Fatalf("connecting to the TCK broker: %v", tok.Error())
	}
	d.t.Cleanup(func() { d.cli.Disconnect(250) })
	d.subscribe("SPARKPLUG_TCK/RESULT", func(p []byte) { d.results <- string(p) })
	d.subscribe("SPARKPLUG_TCK/LOG", func(p []byte) { d.t.Logf("tck: %s", p) })
	d.subscribe("SPARKPLUG_TCK/CONSOLE_PROMPT", func(p []byte) {
		d.t.Logf("tck prompt: %s", p)
		d.mu.Lock()
		f := d.onPrompt
		d.mu.Unlock()
		if f == nil {
			return
		}
		go func() {
			if reply := f(string(p)); reply != "" {
				d.t.Logf("reply: %s", reply)
				d.cli.Publish("SPARKPLUG_TCK/CONSOLE_REPLY", 1, false, reply).Wait()
			}
		}()
	})
	d.subscribe("spBv1.0/+/DDEATH/+/+", func(p []byte) {
		if pl, err := spb.Decode(p); err == nil && pl.HasTimestamp {
			d.mu.Lock()
			d.ddeathTS = int64(pl.Timestamp)
			d.mu.Unlock()
		}
	})
}

func (d *driver) subscribe(filter string, f func([]byte)) {
	tok := d.cli.Subscribe(filter, 1, func(_ mqtt.Client, m mqtt.Message) { f(m.Payload()) })
	if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		d.t.Fatalf("subscribing to %s: %v", filter, tok.Error())
	}
}

// run runs one TCK host test: it starts it, calls then (if set), answers
// prompts with onPrompt, and checks every requirement of the result.
func (d *driver) run(test string, onPrompt func(string) string, then func(), params ...string) {
	d.t.Helper()
	ok := d.t.Run(test, func(t *testing.T) {
		d.mu.Lock()
		d.onPrompt = onPrompt
		d.mu.Unlock()
		// No test left running, no stale result.
		d.cli.Publish("SPARKPLUG_TCK/TEST_CONTROL", 1, false, "END_TEST").Wait()
		time.Sleep(300 * time.Millisecond)
		for len(d.results) > 0 {
			<-d.results
		}
		cmd := strings.Join(append([]string{"NEW_TEST", "host", test, hostID}, params...), " ")
		d.cli.Publish("SPARKPLUG_TCK/TEST_CONTROL", 1, false, cmd).Wait()
		if then != nil {
			time.Sleep(500 * time.Millisecond)
			then()
		}
		var res string
		select {
		case res = <-d.results:
		case <-time.After(60 * time.Second):
			d.cli.Publish("SPARKPLUG_TCK/TEST_CONTROL", 1, false, "END_TEST").Wait()
			select {
			case res = <-d.results:
			case <-time.After(10 * time.Second):
				t.Fatal("no result")
			}
		}
		t.Logf("result:\n%s", res)
		// Not applicable here: the clean session check of the other MQTT
		// version, and the will after a disconnect without DISCONNECT
		// (mqtt-get shuts down gracefully).
		skip := []string{"message-flow-phid-sparkplug-clean-session-50", "host-topic-phid-death-payload-timestamp-disconnect-with-no-disconnect-packet"}
		if d.version == 5 {
			skip[0] = "message-flow-phid-sparkplug-clean-session-311"
		}
		checkResult(t, res, skip...)
	})
	if !ok {
		d.t.FailNow() // later tests depend on the host state
	}
}

// checkResult fails on any requirement that didn't pass (except those in
// skip, which may be NOT EXECUTED). The result is "<id>: PASS;" lines and
// "OVERALL: PASS;".
func checkResult(t *testing.T, res string, skip ...string) {
	t.Helper()
	n := 0
	for _, line := range strings.Split(res, "\n") {
		id, verdict, found := strings.Cut(strings.TrimSuffix(strings.TrimSpace(line), ";"), ": ")
		if !found || strings.Contains(id, "Summary Test Results") || id == "OVERALL" || (slices.Contains(skip, id) && verdict == "NOT EXECUTED") {
			continue
		}
		n++
		if verdict != "PASS" {
			t.Errorf("%s: %s", id, verdict)
		}
	}
	if n < 2 {
		t.Errorf("no requirements in the result: %q", res)
	}
}

func startHost(t *testing.T, b config.Broker) *host {
	dir := t.TempDir()
	st, err := state.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetEnvKeys([]string{key}, nil, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("SPARKPLUG_TCK_VERBOSE") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	svc := core.New(config.Server{DataDir: dir, MaxTopics: 100000, MaxBodyBytes: 1 << 20, PublishTimeout: 5 * time.Second}, log, st)
	reorder := reorderTimeout
	sp := config.Sparkplug{Enabled: true, HostID: hostID, Rebirth: true, RebirthIntervalSec: 1, Commands: true, ReorderTimeoutMs: &reorder}
	sp.Normalize()
	svc.SetEnvSparkplug(sp)
	if err := svc.Start(b, true); err != nil {
		t.Fatal(err)
	}
	return &host{svc: svc, http: httptest.NewServer(httpapi.New(svc, nil).Handler())}
}

// api calls mqtt-get's REST API, retrying while the answer is 404 (the
// births may still be on their way).
func (d *driver) api(method, path, body string) (int, []byte) {
	for i := 0; ; i++ {
		req, _ := http.NewRequest(method, d.host.http.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			d.t.Error(err)
			return 0, nil
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 404 && i < 50 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if resp.StatusCode >= 300 {
			d.t.Errorf("%s %s: %d %s", method, path, resp.StatusCode, b)
		}
		return resp.StatusCode, b
	}
}

func metricPath(dev, metric string) string {
	p := "/api/v1/publish/spb/" + group + "/" + edge
	if dev != "" {
		p += "/" + dev
	}
	for _, level := range strings.Split(metric, "/") {
		p += "/" + url.PathEscape(level)
	}
	return p
}

// sendCommands does what SendCommandTest asks the operator for.
func (d *driver) sendCommands(prompt string) string {
	switch {
	case strings.HasPrefix(prompt, "Send an edge rebirth"):
		time.Sleep(1100 * time.Millisecond) // past rebirth_interval_sec
		d.api("POST", "/api/v1/sparkplug/nodes/"+group+"/"+edge+"/rebirth", "")
	case strings.HasPrefix(prompt, "Send an edge command"):
		d.api("POST", metricPath("", "TCK_metric/Int32"), "5")
	case strings.HasPrefix(prompt, "Send a device rebirth"):
		d.api("POST", metricPath(device, "EdgeNode Control/Rebirth"), "true")
	case strings.HasPrefix(prompt, "Send a device command"):
		d.api("POST", metricPath(device, "Outputs/1"), "7")
	default:
		d.t.Errorf("unexpected prompt %q", prompt)
	}
	return ""
}

type nodeInfo struct {
	Group   string    `json:"group"`
	Edge    string    `json:"edge_node"`
	Online  bool      `json:"online"`
	DeathAt time.Time `json:"death_at"`
	Devices []struct {
		ID      string    `json:"id"`
		Online  bool      `json:"online"`
		DeathAt time.Time `json:"death_at"`
	} `json:"devices"`
}

func (d *driver) node() nodeInfo {
	_, b := d.api("GET", "/api/v1/sparkplug/nodes?group="+group, "")
	var r struct{ Nodes []nodeInfo }
	_ = json.Unmarshal(b, &r)
	for _, n := range r.Nodes {
		if n.Edge == edge {
			return n
		}
	}
	d.t.Errorf("edge node %s/%s not listed: %s", group, edge, b)
	return nodeInfo{}
}

// stale reports whether every translated value under prefix is marked
// online:false (and there is at least one).
func (d *driver) stale(prefix string, exclude string) bool {
	_, b := d.api("GET", "/api/v1/values?filter="+url.QueryEscape(prefix+"/#"), "")
	var r struct {
		Values []struct {
			Topic   string
			Payload struct{ Online *bool }
		}
	}
	if json.Unmarshal(b, &r) != nil {
		return false
	}
	n := 0
	for _, v := range r.Values {
		if exclude != "" && strings.HasPrefix(v.Topic, exclude+"/") {
			continue
		}
		n++
		if v.Payload.Online == nil || *v.Payload.Online {
			d.t.Logf("%s is not marked offline", v.Topic)
			return false
		}
	}
	return n > 0
}

func verdict(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

// checkTermination answers EdgeSessionTerminationTest: it checks that
// mqtt-get shows the device, then the node, offline with stale values.
func (d *driver) checkTermination(prompt string) string {
	time.Sleep(200 * time.Millisecond)
	nodeTopic := "spb/" + group + "/" + edge
	devTopic := nodeTopic + "/" + device
	n := d.node()
	var dev struct {
		online  bool
		deathAt time.Time
		found   bool
	}
	for _, x := range n.Devices {
		if x.ID == device {
			dev.online, dev.deathAt, dev.found = x.Online, x.DeathAt, true
		}
	}
	switch {
	case strings.Contains(prompt, "DDEATH") && strings.Contains(prompt, "Sparkplug Device associated with the Edge Node as offline"):
		d.mu.Lock()
		ts := d.ddeathTS
		d.mu.Unlock()
		if dev.deathAt.UnixMilli() != ts {
			d.t.Logf("device death_at %v, DDEATH timestamp %d", dev.deathAt, ts)
		}
		return verdict(dev.found && !dev.online && dev.deathAt.UnixMilli() == ts)
	case strings.Contains(prompt, "DDEATH") && strings.Contains(prompt, "STALE"):
		return verdict(d.stale(devTopic, ""))
	case strings.Contains(prompt, "NDEATH") && strings.Contains(prompt, "mark the Edge Node as offline"):
		return verdict(!n.Online && time.Since(n.DeathAt) < time.Minute)
	case strings.Contains(prompt, "NDEATH") && strings.Contains(prompt, "NBIRTH as STALE"):
		return verdict(d.stale(nodeTopic, devTopic))
	case strings.Contains(prompt, "NDEATH") && strings.Contains(prompt, "Sparkplug Devices associated with the Edge Node as offline"):
		return verdict(dev.found && !dev.online && d.stale(devTopic, ""))
	}
	d.t.Errorf("unexpected prompt %q", prompt)
	return "FAIL"
}
