package mqttc

import (
	"strings"
	"testing"
)

// Test vectors from RFC 5802 (SHA-1) and RFC 7677 (SHA-256).
func TestSCRAMVectors(t *testing.T) {
	for _, v := range []struct {
		method, nonce, serverFirst, clientFinal, serverFinal string
	}{
		{"SCRAM-SHA-1", "fyko+d2lbbFgONRv9qkxdawL",
			"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
			"c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=",
			"v=rmF9pqV8S7suAoZWja4dJRkFsKQ="},
		{"SCRAM-SHA-256", "rOprNGfwEbeRWgbNEkqO",
			"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
			"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
			"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="},
	} {
		t.Run(v.method, func(t *testing.T) {
			s, err := newSCRAM(v.method, "user", "pencil")
			if err != nil {
				t.Fatal(err)
			}
			s.nonce = v.nonce
			if got := string(s.clientFirst()); got != "n,,n=user,r="+v.nonce {
				t.Fatalf("client-first %q", got)
			}
			final, err := s.clientFinal([]byte(v.serverFirst))
			if err != nil || string(final) != v.clientFinal {
				t.Fatalf("client-final %q (%v), want %q", final, err, v.clientFinal)
			}
			if err := s.verifyServer([]byte(v.serverFinal)); err != nil {
				t.Fatal(err)
			}
			if err := s.verifyServer([]byte("v=AAAA")); err == nil {
				t.Fatal("a wrong server signature must be rejected")
			}
		})
	}
}

func TestSCRAMRejectsBadServerFirst(t *testing.T) {
	for _, sf := range []string{
		"r=othernonce,s=QSXCR+Q6sek8bf92,i=4096",   // does not extend our nonce
		"r=abc,s=QSXCR+Q6sek8bf92,i=4096",          // equals our nonce (no server part)
		"r=abcXYZ,s=!!,i=4096",                     // bad salt
		"r=abcXYZ,s=QSXCR+Q6sek8bf92,i=0",          // bad iterations
		"r=abcXYZ,s=QSXCR+Q6sek8bf92,i=2000000000", // too many iterations
		"e=unknown-user",
	} {
		s, _ := newSCRAM("SCRAM-SHA-256", "u", "p")
		s.nonce = "abc"
		s.clientFirst()
		if _, err := s.clientFinal([]byte(sf)); err == nil {
			t.Errorf("server-first %q should be rejected", sf)
		}
	}
	s, _ := newSCRAM("SCRAM-SHA-256", "a=b,c", "p")
	if got := string(s.clientFirst()); !strings.HasPrefix(got, "n,,n=a=3Db=2Cc,r=") {
		t.Errorf("username not escaped: %q", got)
	}
	if _, err := newSCRAM("SCRAM-MD5", "u", "p"); err == nil {
		t.Error("unknown method accepted")
	}
}

// FuzzSCRAM feeds arbitrary broker messages to the client: it must return
// an error or a proof, never panic.
func FuzzSCRAM(f *testing.F) {
	f.Add("r=abcXYZ,s=QSXCR+Q6sek8bf92,i=16", "v=rmF9pqV8S7suAoZWja4dJRkFsKQ=")
	f.Add("e=invalid-proof", "e=other")
	f.Add("r=abc,,s=,i=-1,=", "v=")
	f.Fuzz(func(t *testing.T, serverFirst, serverFinal string) {
		s, _ := newSCRAM("SCRAM-SHA-256", "u", "p")
		s.nonce = "abc"
		s.clientFirst()
		if strings.Contains(serverFirst, "i=") {
			// Keep iteration counts small so the fuzzer stays fast.
			if n := scramAttrs(serverFirst)["i"]; len(n) > 3 {
				return
			}
		}
		_, _ = s.clientFinal([]byte(serverFirst))
		_ = s.verifyServer([]byte(serverFinal))
	})
}
