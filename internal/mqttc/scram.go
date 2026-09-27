package mqttc

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
)

// scram is the client side of SCRAM (RFC 5802, RFC 7677), used as the
// MQTT 5 enhanced authentication method: the password never crosses the
// wire, and the broker proves it knows the password too. One value per
// authentication exchange.
type scram struct {
	method    string
	hash      func() hash.Hash
	user      string
	pass      string
	nonce     string
	firstBare string
	serverSig []byte
}

// scramHashes lists the supported methods (the MQTT 5 Authentication
// Method names brokers use).
var scramHashes = map[string]func() hash.Hash{
	"SCRAM-SHA-1":   sha1.New,
	"SCRAM-SHA-256": sha256.New,
	"SCRAM-SHA-512": sha512.New,
}

// maxSCRAMIterations bounds the work a broker can make us do.
const maxSCRAMIterations = 10_000_000

func newSCRAM(method, user, pass string) (*scram, error) {
	h, ok := scramHashes[method]
	if !ok {
		return nil, fmt.Errorf("unsupported authentication method %q", method)
	}
	b := make([]byte, 18)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return &scram{method: method, hash: h, user: user, pass: pass, nonce: base64.RawStdEncoding.EncodeToString(b)}, nil
}

// clientFirst returns the client-first-message (no channel binding).
func (s *scram) clientFirst() []byte {
	name := strings.NewReplacer("=", "=3D", ",", "=2C").Replace(s.user)
	s.firstBare = "n=" + name + ",r=" + s.nonce
	return []byte("n,," + s.firstBare)
}

// clientFinal answers the server-first-message with the proof.
func (s *scram) clientFinal(serverFirst []byte) ([]byte, error) {
	attrs := scramAttrs(string(serverFirst))
	if e, ok := attrs["e"]; ok {
		return nil, errors.New("SCRAM: broker error: " + e)
	}
	nonce, salt64, iter := attrs["r"], attrs["s"], attrs["i"]
	if !strings.HasPrefix(nonce, s.nonce) || len(nonce) == len(s.nonce) {
		return nil, errors.New("SCRAM: the broker's nonce does not extend ours")
	}
	salt, err := base64.StdEncoding.DecodeString(salt64)
	if err != nil || len(salt) == 0 {
		return nil, errors.New("SCRAM: invalid salt")
	}
	n, err := strconv.Atoi(iter)
	if err != nil || n < 1 || n > maxSCRAMIterations {
		return nil, fmt.Errorf("SCRAM: invalid iteration count %q", iter)
	}
	salted, err := pbkdf2.Key(s.hash, s.pass, salt, n, s.hash().Size())
	if err != nil {
		return nil, err
	}
	clientKey := s.hmac(salted, "Client Key")
	h := s.hash()
	h.Write(clientKey)
	storedKey := h.Sum(nil)
	withoutProof := "c=biws,r=" + nonce // biws = base64("n,,")
	authMsg := s.firstBare + "," + string(serverFirst) + "," + withoutProof
	proof := s.hmac(storedKey, authMsg)
	for i := range proof {
		proof[i] ^= clientKey[i]
	}
	s.serverSig = s.hmac(s.hmac(salted, "Server Key"), authMsg)
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)), nil
}

// verifyServer checks the server-final-message: the broker proves it knows
// the password.
func (s *scram) verifyServer(serverFinal []byte) error {
	if s.serverSig == nil {
		return errors.New("SCRAM: the broker finished authentication early")
	}
	attrs := scramAttrs(string(serverFinal))
	if e, ok := attrs["e"]; ok {
		return errors.New("SCRAM: broker error: " + e)
	}
	v, err := base64.StdEncoding.DecodeString(attrs["v"])
	if err != nil || subtle.ConstantTimeCompare(v, s.serverSig) != 1 {
		return errors.New("SCRAM: the broker's signature is wrong (it does not know the password)")
	}
	return nil
}

func (s *scram) hmac(key []byte, msg string) []byte {
	m := hmac.New(s.hash, key)
	m.Write([]byte(msg))
	return m.Sum(nil)
}

func scramAttrs(msg string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.Split(msg, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok && len(k) == 1 {
			if _, dup := out[k]; !dup {
				out[k] = v
			}
		}
	}
	return out
}
