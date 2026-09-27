package mqttc

import (
	"bufio"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
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

// dialer returns paho's CustomOpenConnectionFn: it dials every supported
// scheme and returns a buffered connection. With forceTLS, plain tcp:// and
// mqtt:// URLs are upgraded to TLS (tls.enabled / MQTT_TLS=true).
func dialer(forceTLS bool) mqtt.OpenConnectionFunc {
	return func(uri *url.URL, o mqtt.ClientOptions) (net.Conn, error) {
		return openConn(uri, o, forceTLS)
	}
}

func openConn(uri *url.URL, o mqtt.ClientOptions, forceTLS bool) (net.Conn, error) {
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
		return mqtt.NewWebsocket(u.String(), tc, o.ConnectTimeout, o.HTTPHeaders, o.WebsocketOptions)
	case "unix":
		addr := uri.Path
		if uri.Host != "" {
			addr = uri.Host
		}
		conn, err = d.Dial("unix", addr)
	case "mqtt", "tcp":
		if forceTLS {
			return openConn(&url.URL{Scheme: "ssl", Host: uri.Host}, o, false)
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
