package mqttc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// --- certificate helpers ---

type certPair struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM string
	keyPEM  string
}

func (c certPair) tls(t *testing.T) tls.Certificate {
	t.Helper()
	tc, err := tls.X509KeyPair([]byte(c.certPEM), []byte(c.keyPEM))
	if err != nil {
		t.Fatal(err)
	}
	return tc
}

var serial atomic.Int64

// newCert creates a certificate signed by parent (self-signed when nil).
func newCert(t *testing.T, cn string, parent *certPair, isCA bool, dns []string, ips []net.IP, usage x509.ExtKeyUsage) certPair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial.Add(1)),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		DNSNames:              dns,
		IPAddresses:           ips,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
	}
	if isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	} else {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{usage}
	}
	signer, signerKey := tmpl, key
	if parent != nil {
		signer, signerKey = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	kb, _ := x509.MarshalECPrivateKey(key)
	return certPair{
		cert:    cert,
		key:     key,
		certPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		keyPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})),
	}
}

type pki struct {
	ca, server, client, rogueClient certPair
	// serverNamed has only the DNS name "broker.internal" (no IP SAN).
	serverNamed certPair
}

func newPKI(t *testing.T) pki {
	ca := newCert(t, "test-ca", nil, true, nil, nil, 0)
	rogueCA := newCert(t, "rogue-ca", nil, true, nil, nil, 0)
	loopback := []net.IP{net.ParseIP("127.0.0.1")}
	return pki{
		ca:          ca,
		server:      newCert(t, "localhost", &ca, false, []string{"localhost"}, loopback, x509.ExtKeyUsageServerAuth),
		serverNamed: newCert(t, "broker.internal", &ca, false, []string{"broker.internal"}, nil, x509.ExtKeyUsageServerAuth),
		client:      newCert(t, "device-1", &ca, false, nil, nil, x509.ExtKeyUsageClientAuth),
		rogueClient: newCert(t, "intruder", &rogueCA, false, nil, nil, x509.ExtKeyUsageClientAuth),
	}
}

// --- broker helpers ---

type brokerOpts struct {
	serverCert certPair
	mtls       bool     // require client certificates signed by the CA
	websocket  bool     // WebSocket listener instead of TCP
	users      []string // "user:pass"; empty = anonymous allowed
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startBroker runs an embedded MQTT broker and returns its address.
func startBroker(t *testing.T, p pki, o brokerOpts) (*mochi.Server, string) {
	t.Helper()
	b := mochi.New(&mochi.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if len(o.users) == 0 {
		_ = b.AddHook(new(auth.AllowHook), nil)
	} else {
		var rules auth.AuthRules
		for _, u := range o.users {
			name, pass, _ := strings.Cut(u, ":")
			rules = append(rules, auth.AuthRule{Username: auth.RString(name), Password: auth.RString(pass), Allow: true})
		}
		_ = b.AddHook(new(auth.Hook), &auth.Options{Ledger: &auth.Ledger{Auth: rules,
			ACL: auth.ACLRules{{Filters: auth.Filters{"#": auth.ReadWrite}}}}})
	}
	lc := listeners.Config{ID: "l", Address: freeAddr(t)}
	if o.serverCert.certPEM != "" {
		tc := &tls.Config{Certificates: []tls.Certificate{o.serverCert.tls(t)}, MinVersion: tls.VersionTLS12}
		if o.mtls {
			pool := x509.NewCertPool()
			pool.AddCert(p.ca.cert)
			tc.ClientCAs = pool
			tc.ClientAuth = tls.RequireAndVerifyClientCert
		}
		lc.TLSConfig = tc
	}
	var err error
	if o.websocket {
		err = b.AddListener(listeners.NewWebsocket(lc))
	} else {
		err = b.AddListener(listeners.NewTCP(lc))
	}
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = b.Serve() }()
	t.Cleanup(func() {
		defer func() { _ = recover() }() // a test may already have closed it (restart tests)
		_ = b.Close()
	})
	time.Sleep(50 * time.Millisecond)
	return b, lc.Address
}

type recorder struct{ got chan *store.Entry }

func newManager(t *testing.T) (*Manager, *recorder) {
	r := &recorder{got: make(chan *store.Entry, 16)}
	m := New(slog.New(slog.NewTextHandler(io.Discard, nil)), func(e store.Entry) {
		select {
		case r.got <- &e:
		default:
		}
	})
	t.Cleanup(m.Close)
	return m, r
}

// expectConnected waits for a connection, then round-trips a message
// through the broker to prove the session actually works.
func expectConnected(t *testing.T, m *Manager, r *recorder) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !m.Status().Connected {
		if time.Now().After(deadline) {
			t.Fatalf("expected connection, status: %+v", m.Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // subscription
	if err := m.Publish(context.Background(), "tls/check", []byte("ping"), 1, false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	select {
	case e := <-r.got:
		if e.Topic != "tls/check" || string(e.Payload) != "ping" {
			t.Fatalf("unexpected message %s=%s", e.Topic, e.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message did not round-trip")
	}
}

// expectRejected checks that no connection is established and that the
// reported error mentions want.
func expectRejected(t *testing.T, m *Manager, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := m.Status()
		if st.Connected {
			t.Fatalf("connection should have been rejected")
		}
		if st.LastError != "" && strings.Contains(strings.ToLower(st.LastError), strings.ToLower(want)) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected error containing %q, got %q", want, m.Status().LastError)
}

func broker(url string, mut func(*config.Broker)) config.Broker {
	b := config.Broker{URLs: []string{url}, ClientID: "tls-test", ConnectTimeoutSec: 2,
		Subscriptions: []config.Subscription{{Filter: "tls/#", QoS: 1}}}
	if mut != nil {
		mut(&b)
	}
	return b
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// --- tests ---

func TestTLS(t *testing.T) {
	p := newPKI(t)
	_, addr := startBroker(t, p, brokerOpts{serverCert: p.server})

	for _, scheme := range []string{"ssl", "tls", "mqtts", "tcps", "mqtt+ssl"} {
		t.Run("ca_pem/"+scheme, func(t *testing.T) {
			m, r := newManager(t)
			if err := m.Apply(broker(scheme+"://"+addr, func(b *config.Broker) { b.TLS.CAPEM = p.ca.certPEM })); err != nil {
				t.Fatal(err)
			}
			expectConnected(t, m, r)
		})
	}
	t.Run("ca_file", func(t *testing.T) {
		m, r := newManager(t)
		ca := writeFile(t, "ca.pem", p.ca.certPEM)
		if err := m.Apply(broker("mqtts://"+addr, func(b *config.Broker) { b.TLS.CAFile = ca })); err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
	t.Run("tcp_url_with_tls_enabled", func(t *testing.T) {
		m, r := newManager(t)
		// A tcp:// URL with tls.enabled is upgraded to TLS.
		err := m.Apply(broker("tcp://"+addr, func(b *config.Broker) { b.TLS.Enabled = true; b.TLS.CAPEM = p.ca.certPEM }))
		if err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
	t.Run("untrusted_server_rejected", func(t *testing.T) {
		m, _ := newManager(t)
		if err := m.Apply(broker("mqtts://"+addr, nil)); err != nil { // system roots only
			t.Fatal(err)
		}
		expectRejected(t, m, "certificate")
	})
	t.Run("insecure_skip_verify", func(t *testing.T) {
		m, r := newManager(t)
		if err := m.Apply(broker("mqtts://"+addr, func(b *config.Broker) { b.TLS.InsecureSkipVerify = true })); err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
	t.Run("plaintext_to_tls_port_fails", func(t *testing.T) {
		m, _ := newManager(t)
		if err := m.Apply(broker("tcp://"+addr, nil)); err != nil {
			t.Fatal(err)
		}
		expectRejected(t, m, "")
	})
}

func TestTLSServerName(t *testing.T) {
	p := newPKI(t)
	// The certificate is only valid for "broker.internal", but we dial 127.0.0.1.
	_, addr := startBroker(t, p, brokerOpts{serverCert: p.serverNamed})
	t.Run("hostname_mismatch_rejected", func(t *testing.T) {
		m, _ := newManager(t)
		if err := m.Apply(broker("mqtts://"+addr, func(b *config.Broker) { b.TLS.CAPEM = p.ca.certPEM })); err != nil {
			t.Fatal(err)
		}
		expectRejected(t, m, "certificate")
	})
	t.Run("server_name_override", func(t *testing.T) {
		m, r := newManager(t)
		err := m.Apply(broker("mqtts://"+addr, func(b *config.Broker) {
			b.TLS.CAPEM = p.ca.certPEM
			b.TLS.ServerName = "broker.internal"
		}))
		if err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
}

func TestMutualTLS(t *testing.T) {
	p := newPKI(t)
	_, addr := startBroker(t, p, brokerOpts{serverCert: p.server, mtls: true})
	url := "mqtts://" + addr

	t.Run("client_cert_pem", func(t *testing.T) {
		m, r := newManager(t)
		err := m.Apply(broker(url, func(b *config.Broker) {
			b.TLS.CAPEM, b.TLS.CertPEM, b.TLS.KeyPEM = p.ca.certPEM, p.client.certPEM, p.client.keyPEM
		}))
		if err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
	t.Run("client_cert_files", func(t *testing.T) {
		m, r := newManager(t)
		ca, cert, key := writeFile(t, "ca.pem", p.ca.certPEM), writeFile(t, "c.pem", p.client.certPEM), writeFile(t, "k.pem", p.client.keyPEM)
		err := m.Apply(broker(url, func(b *config.Broker) { b.TLS.CAFile, b.TLS.CertFile, b.TLS.KeyFile = ca, cert, key }))
		if err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
	t.Run("no_client_cert_rejected", func(t *testing.T) {
		m, _ := newManager(t)
		if err := m.Apply(broker(url, func(b *config.Broker) { b.TLS.CAPEM = p.ca.certPEM })); err != nil {
			t.Fatal(err)
		}
		expectRejected(t, m, "")
	})
	t.Run("cert_from_untrusted_ca_rejected", func(t *testing.T) {
		m, _ := newManager(t)
		err := m.Apply(broker(url, func(b *config.Broker) {
			b.TLS.CAPEM, b.TLS.CertPEM, b.TLS.KeyPEM = p.ca.certPEM, p.rogueClient.certPEM, p.rogueClient.keyPEM
		}))
		if err != nil {
			t.Fatal(err)
		}
		expectRejected(t, m, "")
	})
	t.Run("mismatched_key_is_config_error", func(t *testing.T) {
		m, _ := newManager(t)
		err := m.Apply(broker(url, func(b *config.Broker) {
			b.TLS.CAPEM, b.TLS.CertPEM, b.TLS.KeyPEM = p.ca.certPEM, p.client.certPEM, p.rogueClient.keyPEM
		}))
		if err == nil || !strings.Contains(err.Error(), "client certificate") {
			t.Fatalf("want client certificate error, got %v", err)
		}
	})
	t.Run("missing_file_is_config_error", func(t *testing.T) {
		m, _ := newManager(t)
		err := m.Apply(broker(url, func(b *config.Broker) { b.TLS.CAFile = "/nonexistent/ca.pem" }))
		if err == nil || !strings.Contains(err.Error(), "CA file") {
			t.Fatalf("want CA file error, got %v", err)
		}
	})
}

func TestMutualTLSWithPassword(t *testing.T) {
	p := newPKI(t)
	_, addr := startBroker(t, p, brokerOpts{serverCert: p.server, mtls: true, users: []string{"dev:pw"}})
	m, r := newManager(t)
	err := m.Apply(broker("mqtts://"+addr, func(b *config.Broker) {
		b.TLS.CAPEM, b.TLS.CertPEM, b.TLS.KeyPEM = p.ca.certPEM, p.client.certPEM, p.client.keyPEM
		b.Username, b.Password = "dev", "pw"
	}))
	if err != nil {
		t.Fatal(err)
	}
	expectConnected(t, m, r)
}

func TestSecureWebsocket(t *testing.T) {
	p := newPKI(t)
	t.Run("wss", func(t *testing.T) {
		_, addr := startBroker(t, p, brokerOpts{serverCert: p.server, websocket: true})
		m, r := newManager(t)
		if err := m.Apply(broker("wss://"+addr+"/mqtt", func(b *config.Broker) { b.TLS.CAPEM = p.ca.certPEM })); err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
	t.Run("wss_mtls", func(t *testing.T) {
		_, addr := startBroker(t, p, brokerOpts{serverCert: p.server, websocket: true, mtls: true})
		m, r := newManager(t)
		err := m.Apply(broker("wss://"+addr+"/mqtt", func(b *config.Broker) {
			b.TLS.CAPEM, b.TLS.CertPEM, b.TLS.KeyPEM = p.ca.certPEM, p.client.certPEM, p.client.keyPEM
		}))
		if err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
	t.Run("ws_plain", func(t *testing.T) {
		_, addr := startBroker(t, p, brokerOpts{websocket: true})
		m, r := newManager(t)
		if err := m.Apply(broker("ws://"+addr+"/mqtt", nil)); err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
}

func TestUsernamePassword(t *testing.T) {
	p := newPKI(t)
	_, addr := startBroker(t, p, brokerOpts{users: []string{"alice:s3cret"}})
	url := "tcp://" + addr
	t.Run("ok", func(t *testing.T) {
		m, r := newManager(t)
		if err := m.Apply(broker(url, func(b *config.Broker) { b.Username, b.Password = "alice", "s3cret" })); err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
	t.Run("wrong_password", func(t *testing.T) {
		m, _ := newManager(t)
		if err := m.Apply(broker(url, func(b *config.Broker) { b.Username, b.Password = "alice", "nope" })); err != nil {
			t.Fatal(err)
		}
		expectRejected(t, m, "")
	})
	t.Run("password_file_rotation", func(t *testing.T) {
		// Starts with a stale token; after the file is updated the next
		// automatic retry must pick it up without any API call.
		pf := writeFile(t, "token", "stale\n")
		m, r := newManager(t)
		if err := m.Apply(broker(url, func(b *config.Broker) { b.Username, b.PasswordFile = "alice", pf })); err != nil {
			t.Fatal(err)
		}
		expectRejected(t, m, "")
		if err := os.WriteFile(pf, []byte("s3cret\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		expectConnected(t, m, r)
	})
}

func writeFileAt(path, content string) error { return os.WriteFile(path, []byte(content), 0o600) }

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// restartBroker starts a new plain broker on a previously used address.
func restartBroker(t *testing.T, addr string) *mochi.Server {
	t.Helper()
	b := mochi.New(&mochi.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	_ = b.AddHook(new(auth.AllowHook), nil)
	if err := b.AddListener(listeners.NewTCP(listeners.Config{ID: "l2", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go func() { _ = b.Serve() }()
	t.Cleanup(func() { _ = b.Close() })
	return b
}
