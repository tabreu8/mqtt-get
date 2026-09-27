package mqttc

import (
	"bufio"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/store"
)

// scramBroker is an MQTT 5 broker that authenticates with SCRAM-SHA-256
// (server side written from RFC 5802, independently of the client).
type scramBroker struct {
	password      string
	badSignature  bool        // answer with a wrong server signature
	sawPassword   atomic.Bool // a CONNECT carried the password flag
	authenticated atomic.Int32
}

func (b *scramBroker) start(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go b.serve(c)
		}
	}()
	return "tcp://" + l.Addr().String()
}

func mac(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

func (b *scramBroker) serve(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	_, body, err := readPacket(r, nil)
	if err != nil || len(body) < 11 {
		return
	}
	if body[7]&0x40 != 0 {
		b.sawPassword.Store(true)
	}
	props, _, err := splitProps(body[10:])
	if err != nil {
		return
	}
	var method string
	var first []byte
	_ = forEachProp(props, func(id byte, v []byte) {
		switch id {
		case propAuthMethod:
			method = string(v)
		case propAuthData:
			first = append([]byte(nil), v...)
		}
	})
	refuse := func() { _, _ = c.Write([]byte{pConnack << 4, 3, 0, 0x86, 0}) }
	if method != "SCRAM-SHA-256" || !strings.HasPrefix(string(first), "n,,") {
		refuse()
		return
	}
	firstBare := string(first[3:])
	cnonce := scramAttrs(firstBare)["r"]
	salt := []byte("0123456789abcdef")
	serverFirst := "r=" + cnonce + "srv-nonce,s=" + base64.StdEncoding.EncodeToString(salt) + ",i=4096"
	_, _ = c.Write(authPacket(method, []byte(serverFirst)))

	h, body, err := readPacket(r, nil)
	if err != nil || h>>4 != pAuth || len(body) < 1 || body[0] != 0x18 {
		return
	}
	aprops, _, _ := splitProps(body[1:])
	clientFinal := string(authData(aprops))
	i := strings.LastIndex(clientFinal, ",p=")
	if i < 0 {
		refuse()
		return
	}
	proof, _ := base64.StdEncoding.DecodeString(clientFinal[i+3:])
	salted, _ := pbkdf2.Key(sha256.New, b.password, salt, 4096, 32)
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	authMsg := firstBare + "," + serverFirst + "," + clientFinal[:i]
	sig := mac(stored[:], authMsg)
	if len(proof) != len(sig) {
		refuse()
		return
	}
	for k := range proof {
		proof[k] ^= sig[k]
	}
	if !hmac.Equal(proof, clientKey) {
		refuse() // wrong password
		return
	}
	serverSig := mac(mac(salted, "Server Key"), authMsg)
	if b.badSignature {
		serverSig[0] ^= 0xFF
	}
	final := "v=" + base64.StdEncoding.EncodeToString(serverSig)
	cp := appendStr([]byte{propAuthMethod}, method)
	cp = binary.BigEndian.AppendUint16(append(cp, propAuthData), uint16(len(final)))
	cp = append(cp, final...)
	_, _ = c.Write(packet(pConnack<<4, append(appendVarint([]byte{0, 0}, len(cp)), cp...)))
	b.authenticated.Add(1)
	// SUBSCRIBE → SUBACK, then just keep the connection.
	if _, body, err := readPacket(r, nil); err == nil && len(body) >= 2 {
		_, _ = c.Write([]byte{pSuback << 4, 4, body[0], body[1], 0, 1})
	}
	_, _ = io.Copy(io.Discard, r)
}

func scramCfg(url, password, client string) config.Broker {
	b := leanCfg5(url)
	b.Client = client
	b.Username, b.Password, b.AuthMethod = "device-7", password, "SCRAM-SHA-256"
	return b
}

func TestSCRAMEnhancedAuth(t *testing.T) {
	for _, client := range []string{config.ClientLean, config.ClientPaho} {
		t.Run(client, func(t *testing.T) {
			t.Run("success", func(t *testing.T) {
				b := &scramBroker{password: "s3cret"}
				m := New(quietLogger(), func(store.Entry) {})
				defer m.Close()
				if err := m.Apply(scramCfg(b.start(t), "s3cret", client)); err != nil {
					t.Fatal(err)
				}
				waitFor(t, "connect with SCRAM", func() bool { return m.Status().Connected })
				if b.sawPassword.Load() {
					t.Fatal("the password was sent in CONNECT")
				}
			})
			t.Run("wrong_password", func(t *testing.T) {
				b := &scramBroker{password: "s3cret"}
				m := New(quietLogger(), func(store.Entry) {})
				defer m.Close()
				if err := m.Apply(scramCfg(b.start(t), "nope", client)); err != nil {
					t.Fatal(err)
				}
				waitFor(t, "refusal", func() bool { return strings.Contains(m.Status().LastError, "bad user name or password") })
				if m.Status().Connected {
					t.Fatal("connected with a wrong password")
				}
			})
			t.Run("impostor_broker", func(t *testing.T) {
				b := &scramBroker{password: "s3cret", badSignature: true}
				m := New(quietLogger(), func(store.Entry) {})
				defer m.Close()
				if err := m.Apply(scramCfg(b.start(t), "s3cret", client)); err != nil {
					t.Fatal(err)
				}
				waitFor(t, "signature rejected", func() bool { return strings.Contains(m.Status().LastError, "signature is wrong") })
				if m.Status().Connected {
					t.Fatal("trusted a broker that does not know the password")
				}
			})
		})
	}
}

func TestSCRAMValidation(t *testing.T) {
	for _, b := range []config.Broker{
		{URLs: []string{"tcp://x"}, ProtocolVersion: 4, Username: "u", Password: "p", AuthMethod: "SCRAM-SHA-256"},
		{URLs: []string{"tcp://x"}, ProtocolVersion: 5, Username: "u", AuthMethod: "SCRAM-SHA-256"},
		{URLs: []string{"tcp://x"}, ProtocolVersion: 5, Username: "u", Password: "p", AuthMethod: "PLAIN"},
	} {
		b.Normalize()
		if b.Validate() == nil {
			t.Errorf("%+v should be rejected", b)
		}
	}
}
