package mqttc

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// leanlink is a compact MQTT 3.1 / 3.1.1 client built for ingest speed.
//
// The general-purpose client passes every message through several
// goroutines and channels and allocates 4-5 objects per message. Here the
// socket reader parses each PUBLISH straight out of a 64 KiB read buffer and
// hands it to the store on the same goroutine, with one allocation per
// message (topic and payload share one block; the topic is a zero-copy view
// into it). QoS 1/2 acknowledgements are batched and flushed once per socket
// read instead of once per message.
//
// Supported: QoS 0/1/2 in both directions (QoS 2 exactly-once via packet-id
// tracking), keepalive with a dead-connection watchdog, automatic reconnect
// with backoff and failover across URLs, resubscribe on reconnect, and every
// transport and auth method of the shared dialer (TLS, mTLS, websockets,
// password files). Not supported: persisting in-flight messages across
// restarts (like the default in-memory store of the paho client) and wills.
type leanlink struct {
	m        *Manager
	cfg      config.Broker
	tlsCfg   *tls.Config
	headers  http.Header
	clientID string
	subs     []config.Subscription

	ctr  counters
	up   atomic.Bool
	quit chan struct{}
	once sync.Once
	done chan struct{}

	lastRecv atomic.Int64 // unix nanos of the last packet received

	mu       sync.Mutex // guards everything below
	conn     net.Conn
	w        *bufio.Writer
	pending  map[uint16]*pending
	nextID   uint16
	inflight map[uint16]bool // inbound QoS 2 ids between PUBLISH and PUBREL
}

type pending struct {
	done  chan error
	codes chan []byte // SUBACK return codes
}

// Packet types.
const (
	pConnect    = 1
	pConnack    = 2
	pPublish    = 3
	pPuback     = 4
	pPubrec     = 5
	pPubrel     = 6
	pPubcomp    = 7
	pSubscribe  = 8
	pSuback     = 9
	pPingreq    = 12
	pPingresp   = 13
	pDisconnect = 14
)

var connackErrors = map[byte]string{
	1: "unacceptable protocol version", 2: "identifier rejected", 3: "server unavailable",
	4: "bad user name or password", 5: "not authorized",
}

func newLean(m *Manager, cfg config.Broker, tlsCfg *tls.Config, idx int) *leanlink {
	l := &leanlink{m: m, cfg: cfg, tlsCfg: tlsCfg, clientID: clientIDFor(cfg, idx), subs: cfg.SubscriptionsFor(idx),
		quit: make(chan struct{}), done: make(chan struct{})}
	if len(cfg.WSHeaders) > 0 {
		l.headers = http.Header{}
		for k, v := range cfg.WSHeaders {
			l.headers.Set(k, v)
		}
	}
	return l
}

func (l *leanlink) isUp() bool       { return l.up.Load() }
func (l *leanlink) stats() *counters { return &l.ctr }
func (l *leanlink) start()           { go l.run() }

func (l *leanlink) stopped() bool {
	select {
	case <-l.quit:
		return true
	default:
		return false
	}
}

func (l *leanlink) stop() {
	l.once.Do(func() { close(l.quit) })
	l.mu.Lock()
	if l.conn != nil {
		_ = l.conn.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
		_, _ = l.w.Write([]byte{pDisconnect << 4, 0})
		_ = l.w.Flush()
		_ = l.conn.Close()
	}
	l.mu.Unlock()
	select {
	case <-l.done:
	case <-time.After(2 * time.Second):
	}
	l.up.Store(false)
}

// run connects, serves the session and reconnects until stopped.
func (l *leanlink) run() {
	defer close(l.done)
	for attempt := 0; ; attempt++ {
		if d := backoff(attempt); d > 0 {
			select {
			case <-l.quit:
				return
			case <-time.After(d):
			}
		}
		for _, raw := range l.cfg.URLs {
			if l.stopped() {
				return
			}
			u, err := url.Parse(raw)
			if err != nil {
				l.m.setErr("invalid broker url: " + err.Error())
				continue
			}
			wasUp, err := l.session(u)
			if l.stopped() {
				return
			}
			if wasUp {
				l.m.linkDown(err.Error())
				attempt = -1 // reconnect quickly after an established session drops
				break
			}
			l.m.setErr("connect to " + u.Redacted() + " failed: " + err.Error())
		}
	}
}

// session runs one connection. wasUp reports whether it got past CONNACK.
func (l *leanlink) session(u *url.URL) (wasUp bool, err error) {
	timeout := time.Duration(l.cfg.ConnectTimeoutSec) * time.Second
	conn, err := openConn(u, dialOptions{TLSConfig: l.tlsCfg, ConnectTimeout: timeout, HTTPHeaders: l.headers, ForceTLS: l.cfg.TLS.Enabled})
	if err != nil {
		return false, err
	}
	var r *bufio.Reader
	if bc, ok := conn.(*bufConn); ok {
		r = bc.r // reuse the dialer's 64 KiB read buffer
	} else {
		r = bufio.NewReaderSize(conn, 64<<10)
	}
	w := bufio.NewWriterSize(conn, 32<<10)
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := w.Write(l.connectPacket()); err != nil {
		conn.Close()
		return false, err
	}
	if err := w.Flush(); err != nil {
		conn.Close()
		return false, err
	}
	h, body, err := readPacket(r, nil)
	if err != nil {
		conn.Close()
		return false, fmt.Errorf("waiting for CONNACK: %w", err)
	}
	if h>>4 != pConnack || len(body) < 2 {
		conn.Close()
		return false, fmt.Errorf("expected CONNACK, got packet type %d", h>>4)
	}
	if rc := body[1]; rc != 0 {
		conn.Close()
		if s, ok := connackErrors[rc]; ok {
			return false, errors.New(s)
		}
		return false, fmt.Errorf("connection refused (code %d)", rc)
	}
	_ = conn.SetDeadline(time.Time{})

	l.mu.Lock()
	l.conn, l.w = conn, w
	l.pending = map[uint16]*pending{}
	l.inflight = map[uint16]bool{}
	l.mu.Unlock()
	l.lastRecv.Store(time.Now().UnixNano())
	l.up.Store(true)
	l.m.linkUp(l.clientID)

	sessionDone := make(chan struct{})
	defer func() {
		close(sessionDone)
		l.up.Store(false)
		l.mu.Lock()
		conn.Close()
		l.conn, l.w = nil, nil
		for id, p := range l.pending {
			if p.done != nil {
				p.done <- ErrNotConnected
			}
			delete(l.pending, id)
		}
		l.mu.Unlock()
	}()
	if len(l.subs) > 0 {
		if err := l.subscribe(); err != nil {
			return true, err
		}
	}
	if ka := time.Duration(l.cfg.KeepAliveSec) * time.Second; ka > 0 {
		go l.keepalive(conn, ka, sessionDone)
	}
	return true, l.readLoop(r)
}

func (l *leanlink) connectPacket() []byte {
	name, level := "MQTT", byte(4)
	if l.cfg.ProtocolVersion == 3 {
		name, level = "MQIsdp", 3
	}
	var flags byte
	if l.cfg.UseCleanSession() {
		flags |= 0x02
	}
	user, pass := "", ""
	if hasCredentials(l.cfg) {
		user, pass = l.m.credentials(l.cfg) // re-read on every connect
	}
	if user != "" {
		flags |= 0x80
	}
	if pass != "" {
		flags |= 0x40
	}
	var b []byte
	b = appendStr(b, name)
	b = append(b, level, flags)
	b = binary.BigEndian.AppendUint16(b, uint16(l.cfg.KeepAliveSec))
	b = appendStr(b, l.clientID)
	if user != "" {
		b = appendStr(b, user)
	}
	if pass != "" {
		b = appendStr(b, pass)
	}
	return packet(pConnect<<4, b)
}

func (l *leanlink) subscribe() error {
	var b []byte
	l.mu.Lock()
	id := l.allocIDLocked()
	p := &pending{codes: make(chan []byte, 1)}
	l.pending[id] = p
	b = binary.BigEndian.AppendUint16(b, id)
	for _, s := range l.subs {
		b = appendStr(b, s.Filter)
		b = append(b, s.QoS)
	}
	_, err := l.w.Write(packet(pSubscribe<<4|0x02, b))
	if err == nil {
		err = l.w.Flush()
	}
	l.mu.Unlock()
	if err != nil {
		return err
	}
	go func() {
		select {
		case codes := <-p.codes:
			for i, c := range codes {
				if c >= 0x80 && i < len(l.subs) {
					l.m.subscriptionRejected(l.subs[i].Filter, "")
				}
			}
		case <-time.After(30 * time.Second):
			l.m.setErr("subscribe timed out")
		case <-l.quit:
		}
	}()
	return nil
}

// keepalive sends PINGREQ at 3/4 of the keepalive interval and closes the
// connection if nothing was received for 1.5 intervals.
func (l *leanlink) keepalive(conn net.Conn, ka time.Duration, done chan struct{}) {
	t := time.NewTicker(ka * 3 / 4)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			if time.Since(time.Unix(0, l.lastRecv.Load())) > ka*3/2 {
				l.m.setErr("keepalive timeout: no data from broker")
				conn.Close()
				return
			}
			l.writePacket([]byte{pPingreq << 4, 0}, true)
		}
	}
}

// readLoop parses packets until the connection fails.
func (l *leanlink) readLoop(r *bufio.Reader) error {
	var scratch [64]byte
	var now int64
	for {
		fresh := r.Buffered() == 0
		if fresh {
			l.flush() // send batched acks before blocking on the socket
		}
		h, err := r.ReadByte()
		if err != nil {
			return err
		}
		if fresh {
			// One clock read per socket read: everything in this buffer
			// arrived at the same time.
			now = time.Now().UnixNano()
			l.lastRecv.Store(now)
		}
		n, err := readVarint(r)
		if err != nil {
			return err
		}
		if h>>4 != pPublish {
			var body []byte
			if n <= len(scratch) {
				body = scratch[:n]
			} else {
				body = make([]byte, n)
			}
			if _, err := io.ReadFull(r, body); err != nil {
				return err
			}
			l.control(h, body)
			continue
		}
		// PUBLISH: one allocation holds topic and payload.
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}
		if n < 2 {
			return errors.New("malformed PUBLISH")
		}
		tl := int(binary.BigEndian.Uint16(buf))
		qos := (h >> 1) & 3
		p := 2 + tl
		if qos > 0 {
			p += 2
		}
		if tl == 0 || p > n {
			return errors.New("malformed PUBLISH")
		}
		topic := unsafe.String(&buf[2], tl) // buf is never modified again
		var id uint16
		if qos > 0 {
			id = binary.BigEndian.Uint16(buf[2+tl:])
		}
		if qos == 2 {
			l.mu.Lock()
			dup := l.inflight[id]
			l.inflight[id] = true
			l.mu.Unlock()
			if dup { // redelivery before PUBREL: acknowledge, don't deliver twice
				l.ack(pPubrec<<4, id)
				continue
			}
		}
		l.m.deliver(&l.ctr, store.Entry{Topic: topic, Payload: buf[p:n:n], QoS: qos, Retained: h&1 == 1, Time: now})
		switch qos {
		case 1:
			l.ack(pPuback<<4, id)
		case 2:
			l.ack(pPubrec<<4, id)
		}
	}
}

// control handles non-PUBLISH packets.
func (l *leanlink) control(h byte, body []byte) {
	if len(body) < 2 && h>>4 != pPingresp {
		return
	}
	switch h >> 4 {
	case pPingresp:
	case pPubrel: // inbound QoS 2 step 2
		id := binary.BigEndian.Uint16(body)
		l.mu.Lock()
		delete(l.inflight, id)
		l.mu.Unlock()
		l.ack(pPubcomp<<4, id)
	case pPuback, pPubcomp: // outbound QoS 1 done / QoS 2 done
		l.complete(binary.BigEndian.Uint16(body), nil)
	case pPubrec: // outbound QoS 2 step 1
		id := binary.BigEndian.Uint16(body)
		l.writePacket([]byte{pPubrel<<4 | 0x02, 2, byte(id >> 8), byte(id)}, true)
	case pSuback:
		id := binary.BigEndian.Uint16(body)
		l.mu.Lock()
		p := l.pending[id]
		delete(l.pending, id)
		l.mu.Unlock()
		if p != nil && p.codes != nil {
			p.codes <- append([]byte(nil), body[2:]...)
		}
	}
}

func (l *leanlink) complete(id uint16, err error) {
	l.mu.Lock()
	p := l.pending[id]
	delete(l.pending, id)
	l.mu.Unlock()
	if p != nil && p.done != nil {
		p.done <- err
	}
}

// ack queues a 2-byte-id acknowledgement; it is flushed with the next
// batch (see readLoop).
func (l *leanlink) ack(h byte, id uint16) {
	l.writePacket([]byte{h, 2, byte(id >> 8), byte(id)}, false)
}

func (l *leanlink) writePacket(b []byte, flush bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w == nil {
		return
	}
	_, _ = l.w.Write(b)
	if flush {
		l.flushLocked()
	}
}

func (l *leanlink) flush() {
	l.mu.Lock()
	l.flushLocked()
	l.mu.Unlock()
}

func (l *leanlink) flushLocked() {
	if l.w == nil || l.w.Buffered() == 0 {
		return
	}
	_ = l.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := l.w.Flush(); err != nil {
		l.conn.Close() // the read loop notices and reconnects
	}
}

func (l *leanlink) allocIDLocked() uint16 {
	for {
		l.nextID++
		if l.nextID == 0 {
			l.nextID = 1
		}
		if _, used := l.pending[l.nextID]; !used {
			return l.nextID
		}
	}
}

func (l *leanlink) send(ctx context.Context, msg Message) func() error {
	if !msg.Props.Empty() {
		return func() error { return ErrPropertiesNeedV5 }
	}
	var b []byte
	b = appendStr(b, msg.Topic)
	h := byte(pPublish<<4) | msg.QoS<<1
	if msg.Retain {
		h |= 1
	}
	l.mu.Lock()
	if l.w == nil {
		l.mu.Unlock()
		return func() error { return ErrNotConnected }
	}
	var p *pending
	var id uint16
	if msg.QoS > 0 {
		id = l.allocIDLocked()
		p = &pending{done: make(chan error, 1)}
		l.pending[id] = p
		b = binary.BigEndian.AppendUint16(b, id)
	}
	b = append(b, msg.Payload...)
	_, err := l.w.Write(packet(h, b))
	if err == nil {
		_ = l.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		err = l.w.Flush()
	}
	if err != nil && p != nil {
		delete(l.pending, id)
	}
	l.mu.Unlock()
	if err != nil {
		return func() error { return err }
	}
	if p == nil {
		return func() error { return nil }
	}
	return func() error {
		select {
		case err := <-p.done:
			return err
		case <-ctx.Done():
			l.mu.Lock()
			delete(l.pending, id)
			l.mu.Unlock()
			return fmt.Errorf("publish: %w", ctx.Err())
		}
	}
}

// --- encoding helpers ---

func appendStr(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func appendVarint(b []byte, n int) []byte {
	for {
		c := byte(n % 128)
		n /= 128
		if n > 0 {
			c |= 0x80
		}
		b = append(b, c)
		if n == 0 {
			return b
		}
	}
}

// packet frames a body with its fixed header.
func packet(h byte, body []byte) []byte {
	out := make([]byte, 0, len(body)+5)
	out = appendVarint(append(out, h), len(body))
	return append(out, body...)
}

func readVarint(r *bufio.Reader) (int, error) {
	n, mult := 0, 1
	for i := 0; i < 4; i++ {
		c, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		n += int(c&0x7F) * mult
		if c&0x80 == 0 {
			return n, nil
		}
		mult *= 128
	}
	return 0, errors.New("malformed remaining length")
}

// readPacket reads one whole packet (used during the handshake).
func readPacket(r *bufio.Reader, buf []byte) (byte, []byte, error) {
	h, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	n, err := readVarint(r)
	if err != nil {
		return 0, nil, err
	}
	if cap(buf) < n {
		buf = make([]byte, n)
	}
	buf = buf[:n]
	_, err = io.ReadFull(r, buf)
	return h, buf, err
}
