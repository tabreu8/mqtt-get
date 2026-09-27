// Command loadgen publishes MQTT messages as fast as possible (or at a fixed
// rate) to benchmark mqtt-get and brokers.
//
//	go run ./cmd/loadgen -url tcp://localhost:1883 -clients 4 -n 1000000 -topics 10000
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	url := flag.String("url", "tcp://localhost:1883", "broker URL")
	user := flag.String("user", "", "username")
	pass := flag.String("pass", "", "password")
	clients := flag.Int("clients", 4, "publishing connections")
	n := flag.Int("n", 1_000_000, "total messages")
	topics := flag.Int("topics", 10_000, "distinct topics")
	prefix := flag.String("prefix", "load", "topic prefix")
	qos := flag.Int("qos", 0, "QoS")
	size := flag.Int("size", 64, "payload size in bytes")
	rate := flag.Int("rate", 0, "target total messages/s (0 = unlimited)")
	flag.Parse()

	payload := make([]byte, *size)
	for i := range payload {
		payload[i] = 'a' + byte(i%26)
	}
	names := make([]string, *topics)
	for i := range names {
		names[i] = *prefix + "/" + strconv.Itoa(i%100) + "/" + strconv.Itoa(i)
	}

	var sent atomic.Int64
	var wg sync.WaitGroup
	per := *n / *clients
	start := time.Now()
	for c := 0; c < *clients; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			o := mqtt.NewClientOptions().AddBroker(*url).SetClientID(fmt.Sprintf("loadgen-%d-%d", os.Getpid(), c)).
				SetUsername(*user).SetPassword(*pass).SetWriteTimeout(30 * time.Second)
			cl := mqtt.NewClient(o)
			if t := cl.Connect(); t.Wait() && t.Error() != nil {
				fmt.Fprintln(os.Stderr, "connect:", t.Error())
				os.Exit(1)
			}
			var interval time.Duration
			if *rate > 0 {
				interval = time.Second * time.Duration(*clients) / time.Duration(*rate)
			}
			next := time.Now()
			var last mqtt.Token
			for i := 0; i < per; i++ {
				if interval > 0 {
					next = next.Add(interval)
					if d := time.Until(next); d > 0 {
						time.Sleep(d)
					}
				}
				last = cl.Publish(names[(i*(*clients)+c)%len(names)], byte(*qos), false, payload)
				sent.Add(1)
			}
			if last != nil {
				last.Wait()
			}
			cl.Disconnect(1000)
		}(c)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	prev := int64(0)
	for {
		select {
		case <-tick.C:
			s := sent.Load()
			fmt.Printf("sent %d (%d msg/s)\n", s, s-prev)
			prev = s
		case <-done:
			el := time.Since(start)
			fmt.Printf("done: %d messages in %v (%.0f msg/s)\n", sent.Load(), el.Round(time.Millisecond), float64(sent.Load())/el.Seconds())
			return
		}
	}
}
