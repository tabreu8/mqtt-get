package mqttc

import (
	"bufio"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/net/proxy"
)

// bufConn adds a read buffer to a connection. paho reads each packet header
// byte-by-byte straight from the socket; buffering turns several syscalls
// per message into one per ~64 KiB, which roughly doubles ingest throughput.
type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func withDefaultPort(host, port string) string {
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	return net.JoinHostPort(host, port)
}

// dialOptions are the transport settings shared by the MQTT 3 and MQTT 5
// clients.
type dialOptions struct {
	TLSConfig      *tls.Config
	ConnectTimeout time.Duration
	HTTPHeaders    http.Header // websocket only
	ForceTLS       bool        // upgrade tcp:// and mqtt:// URLs (tls.enabled)
}

// dialer returns paho's CustomOpenConnectionFn (MQTT 3.1.1).
func dialer(forceTLS bool) mqtt.OpenConnectionFunc {
	return func(uri *url.URL, o mqtt.ClientOptions) (net.Conn, error) {
		return openConn(uri, dialOptions{TLSConfig: o.TLSConfig, ConnectTimeout: o.ConnectTimeout, HTTPHeaders: o.HTTPHeaders, ForceTLS: forceTLS})
	}
}

// openConn dials every supported scheme and returns a buffered connection.
func openConn(uri *url.URL, o dialOptions) (net.Conn, error) {
	d := &net.Dialer{Timeout: o.ConnectTimeout, KeepAlive: 30 * time.Second}
	var (
		conn net.Conn
		err  error
	)
	switch uri.Scheme {
	case "ws", "wss":
		u := *uri
		u.User = nil
		var tc *tls.Config
		if u.Scheme == "wss" {
			tc = o.TLSConfig
		}
		// The websocket implementation already buffers reads.
		ws, err := mqtt.NewWebsocket(u.String(), tc, o.ConnectTimeout, o.HTTPHeaders, nil)
		if err != nil {
			return nil, err
		}
		return &packetFramedConn{Conn: ws}, nil
	case "unix":
		addr := uri.Path
		if uri.Host != "" {
			addr = uri.Host
		}
		conn, err = d.Dial("unix", addr)
	case "mqtt", "tcp":
		if o.ForceTLS {
			o.ForceTLS = false
			return openConn(&url.URL{Scheme: "ssl", Host: uri.Host}, o)
		}
		conn, err = proxy.FromEnvironmentUsing(d).Dial("tcp", withDefaultPort(uri.Host, "1883"))
	case "ssl", "tls", "mqtts", "mqtt+ssl", "tcps":
		host := withDefaultPort(uri.Host, "8883")
		conn, err = proxy.FromEnvironmentUsing(d).Dial("tcp", host)
		if err != nil {
			return nil, err
		}
		tc := o.TLSConfig
		if tc == nil {
			tc = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		if tc.ServerName == "" && !tc.InsecureSkipVerify {
			tc = tc.Clone()
			tc.ServerName = uri.Hostname()
		}
		tlsConn := tls.Client(conn, tc)
		if o.ConnectTimeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(o.ConnectTimeout))
		}
		if err := tlsConn.Handshake(); err != nil {
			_ = conn.Close()
			return nil, err
		}
		_ = conn.SetDeadline(time.Time{})
		conn = tlsConn
	default:
		return nil, errors.New("unsupported scheme: " + uri.Scheme)
	}
	if err != nil {
		return nil, err
	}
	return &bufConn{Conn: conn, r: bufio.NewReaderSize(conn, 64<<10)}, nil
}

// packetFramedConn sends exactly one complete MQTT packet per websocket
// frame. The MQTT 5 client writes a packet's header and body separately,
// and each Write becomes its own frame. The MQTT spec requires brokers to
// accept packets split across frames, but some (Mosquitto 2.x over
// WebSocket with MQTT 5) reject them as malformed, so writes are
// reassembled into whole packets using the MQTT fixed header.
type packetFramedConn struct {
	net.Conn
	mu  sync.Mutex
	buf []byte
}

func (c *packetFramedConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf = append(c.buf, p...)
	for {
		n, ok := mqttPacketLen(c.buf)
		if !ok || len(c.buf) < n {
			return len(p), nil
		}
		if _, err := c.Conn.Write(c.buf[:n]); err != nil {
			return 0, err
		}
		c.buf = append(c.buf[:0], c.buf[n:]...)
	}
}

// mqttPacketLen returns the total length of the MQTT packet at the start of
// b (fixed header byte + variable-length remaining length + body), or
// ok=false if the length is not yet known.
func mqttPacketLen(b []byte) (int, bool) {
	remaining, mult := 0, 1
	for i := 1; i < len(b) && i <= 4; i++ {
		remaining += int(b[i]&0x7F) * mult
		if b[i]&0x80 == 0 {
			return 1 + i + remaining, true
		}
		mult *= 128
	}
	return 0, false
}
