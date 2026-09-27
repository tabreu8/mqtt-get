// Package state persists runtime configuration (broker settings, webhooks
// and API keys) to a JSON file so changes made through the REST API, MCP or
// web UI survive restarts.
package state

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
)

// ErrNotFound is returned for unknown ids.
var ErrNotFound = errors.New("not found")

type fileData struct {
	Broker   *config.Broker   `json:"broker,omitempty"`
	Webhooks []config.Webhook `json:"webhooks"`
	APIKeys  []config.APIKey  `json:"api_keys"`
}

// State is the persisted configuration. All methods are safe for concurrent use.
type State struct {
	mu   sync.Mutex
	path string
	data fileData

	envKeys []config.APIKey
	// keyIndex maps SHA-256(key) -> key for lock-free lookups on every request.
	keyIndex atomic.Pointer[map[string]*config.APIKey]
}

// Open loads (or creates) the state file in dir. An empty dir disables
// persistence.
func Open(dir string) (*State, error) {
	s := &State{}
	if dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create data dir: %w", err)
		}
		s.path = filepath.Join(dir, "state.json")
		b, err := os.ReadFile(s.path)
		switch {
		case err == nil:
			if err := json.Unmarshal(b, &s.data); err != nil {
				return nil, fmt.Errorf("parse %s: %w", s.path, err)
			}
		case !errors.Is(err, os.ErrNotExist):
			return nil, err
		}
	}
	s.rebuildIndex()
	return s, nil
}

// Path returns the state file path ("" if not persisted).
func (s *State) Path() string { return s.path }

func (s *State) saveLocked() error {
	if s.path == "" {
		return nil
	}
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Broker returns the persisted broker config, if any.
func (s *State) Broker() (config.Broker, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Broker == nil {
		return config.Broker{}, false
	}
	return cloneBroker(*s.data.Broker), true
}

// SetBroker persists the broker config.
func (s *State) SetBroker(b config.Broker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := cloneBroker(b)
	s.data.Broker = &c
	return s.saveLocked()
}

func cloneBroker(b config.Broker) config.Broker {
	b.URLs = slices.Clone(b.URLs)
	b.Subscriptions = slices.Clone(b.Subscriptions)
	b.TLS.ALPN = slices.Clone(b.TLS.ALPN)
	if b.WSHeaders != nil {
		h := make(map[string]string, len(b.WSHeaders))
		for k, v := range b.WSHeaders {
			h[k] = v
		}
		b.WSHeaders = h
	}
	return b
}

// --- webhooks ---

// Webhooks returns all webhooks.
func (s *State) Webhooks() []config.Webhook {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.data.Webhooks)
}

// Webhook returns one webhook.
func (s *State) Webhook(id string) (config.Webhook, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.data.Webhooks {
		if w.ID == id {
			return w, nil
		}
	}
	return config.Webhook{}, ErrNotFound
}

// PutWebhook creates or replaces a webhook (by ID).
func (s *State) PutWebhook(w config.Webhook) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.data.Webhooks {
		if s.data.Webhooks[i].ID == w.ID {
			s.data.Webhooks[i] = w
			return s.saveLocked()
		}
	}
	s.data.Webhooks = append(s.data.Webhooks, w)
	return s.saveLocked()
}

// DeleteWebhook removes a webhook.
func (s *State) DeleteWebhook(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.data.Webhooks {
		if s.data.Webhooks[i].ID == id {
			s.data.Webhooks = slices.Delete(s.data.Webhooks, i, i+1)
			return s.saveLocked()
		}
	}
	return ErrNotFound
}

// --- API keys ---

// HashKey returns the hex SHA-256 of an API key.
func HashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// NewID returns a random identifier.
func NewID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// NewSecret returns a new random API key.
func NewSecret() string {
	var b [24]byte
	_, _ = rand.Read(b[:])
	return "mg_" + hex.EncodeToString(b[:])
}

func prefixOf(key string) string {
	if len(key) > 10 {
		return key[:10] + "…"
	}
	return "…"
}

// SetEnvKeys registers keys coming from the environment (never persisted).
func (s *State) SetEnvKeys(admin, read, publish []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.envKeys = nil
	add := func(keys []string, scopes []string, name string) {
		for i, k := range keys {
			s.envKeys = append(s.envKeys, config.APIKey{
				ID: fmt.Sprintf("env-%s-%d", name, i+1), Name: "env " + name,
				Hash: HashKey(k), Prefix: prefixOf(k), Scopes: scopes, Env: true,
			})
		}
	}
	add(admin, []string{config.ScopeAdmin}, "admin")
	add(read, []string{config.ScopeRead}, "read")
	add(publish, []string{config.ScopeRead, config.ScopePublish}, "publish")
	s.rebuildIndexLocked()
}

// HasKeys reports whether any API key exists.
func (s *State) HasKeys() bool {
	m := s.keyIndex.Load()
	return m != nil && len(*m) > 0
}

// CreateKey creates a new API key and returns it with its plaintext secret.
func (s *State) CreateKey(name string, scopes []string) (config.APIKey, string, error) {
	if len(scopes) == 0 {
		scopes = []string{config.ScopeRead}
	}
	for _, sc := range scopes {
		if !config.ValidScope(sc) {
			return config.APIKey{}, "", fmt.Errorf("invalid scope %q (use read, publish, admin)", sc)
		}
	}
	secret := NewSecret()
	k := config.APIKey{
		ID: NewID(), Name: name, Hash: HashKey(secret), Prefix: prefixOf(secret),
		Scopes: scopes, CreatedAt: time.Now().UTC(),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.APIKeys = append(s.data.APIKeys, k)
	s.rebuildIndexLocked()
	if err := s.saveLocked(); err != nil {
		return config.APIKey{}, "", err
	}
	k.Hash = ""
	return k, secret, nil
}

// Keys lists API keys (without hashes).
func (s *State) Keys() []config.APIKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]config.APIKey, 0, len(s.envKeys)+len(s.data.APIKeys))
	for _, k := range append(slices.Clone(s.envKeys), s.data.APIKeys...) {
		k.Hash = ""
		out = append(out, k)
	}
	return out
}

// DeleteKey removes a persisted API key.
func (s *State) DeleteKey(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.data.APIKeys {
		if s.data.APIKeys[i].ID == id {
			s.data.APIKeys = slices.Delete(s.data.APIKeys, i, i+1)
			s.rebuildIndexLocked()
			return s.saveLocked()
		}
	}
	return ErrNotFound
}

// Lookup finds the API key matching the plaintext secret.
func (s *State) Lookup(secret string) (*config.APIKey, bool) {
	m := s.keyIndex.Load()
	if m == nil || secret == "" {
		return nil, false
	}
	k, ok := (*m)[HashKey(secret)]
	return k, ok
}

func (s *State) rebuildIndex() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rebuildIndexLocked()
}

func (s *State) rebuildIndexLocked() {
	m := make(map[string]*config.APIKey, len(s.envKeys)+len(s.data.APIKeys))
	for _, list := range [][]config.APIKey{s.envKeys, s.data.APIKeys} {
		for i := range list {
			k := list[i]
			m[k.Hash] = &k
		}
	}
	s.keyIndex.Store(&m)
}

// HasScope reports whether the key grants scope.
func HasScope(k *config.APIKey, scope string) bool {
	for _, s := range k.Scopes {
		if s == scope || s == config.ScopeAdmin {
			return true
		}
	}
	return false
}
