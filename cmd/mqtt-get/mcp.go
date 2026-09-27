package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/mcp"
	"github.com/tabreu8/mqtt-get/internal/state"
)

const mcpUsage = `usage:
  mqtt-get mcp                       run the whole service in-process and speak MCP on stdio
                                     (configure with the usual MQTT_* variables; no HTTP server)
  mqtt-get mcp --url URL [--key KEY] proxy stdio to a running mqtt-get server's /mcp endpoint

environment:
  MCP_SCOPES         scopes granted to the stdio agent in standalone mode (default admin),
                     e.g. "read" or "read,publish"
  MQTT_GET_URL       default for --url
  MQTT_GET_API_KEY   default for --key
`

// mcpCommand runs MCP over stdio for local agents (Claude Desktop, IDEs,
// CLIs): standalone, or as a proxy to a remote mqtt-get.
func mcpCommand() error {
	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, mcpUsage) }
	url := fs.String("url", os.Getenv("MQTT_GET_URL"), "mqtt-get base URL (proxy mode)")
	key := fs.String("key", os.Getenv("MQTT_GET_API_KEY"), "API key (proxy mode)")
	_ = fs.Parse(os.Args[1:])
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *url != "" {
		return mcpProxy(ctx, *url, *key)
	}
	return mcpStandalone(ctx)
}

func mcpStandalone(ctx context.Context) error {
	cfg, err := config.LoadServer()
	if err != nil {
		return err
	}
	if os.Getenv("DATA_DIR") == "" {
		// Desktop apps often start servers with an unwritable working dir.
		if dir, err := os.UserConfigDir(); err == nil {
			cfg.DataDir = filepath.Join(dir, "mqtt-get")
		}
	}
	// stdout carries the protocol: logs go to stderr only.
	log := newLogger(cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(log)
	tuneRuntime(log)
	st, err := state.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	scopes := splitList(envOr("MCP_SCOPES", config.ScopeAdmin))
	for _, sc := range scopes {
		if !config.ValidScope(sc) {
			return fmt.Errorf("MCP_SCOPES: invalid scope %q", sc)
		}
	}
	envBroker, envOK, err := config.BrokerFromEnv()
	if err != nil {
		return fmt.Errorf("MQTT configuration: %w", err)
	}
	svc := core.New(cfg, log, st)
	if err := svc.Start(envBroker, envOK); err != nil {
		log.Error("broker configuration error", "err", err)
	}
	defer svc.Close()
	ms := mcp.New(svc, log, mcp.Options{})
	defer ms.Close()
	log.Info("mcp: serving on stdio", "scopes", scopes, "data_dir", cfg.DataDir)
	return ms.ServeStdio(ctx, os.Stdin, os.Stdout, &core.Principal{ID: "stdio", Name: "stdio agent", Scopes: scopes})
}

// mcpProxy forwards stdio MCP to a remote server's Streamable HTTP endpoint,
// keeping its session and relaying its notification stream.
func mcpProxy(ctx context.Context, base, key string) error {
	p := &proxy{
		endpoint: strings.TrimRight(base, "/") + "/mcp",
		key:      key,
		client:   &http.Client{Timeout: 10 * time.Minute},
		out:      bufio.NewWriter(os.Stdout),
		ctx:      ctx,
	}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 64<<10), 16<<20)
	var wg sync.WaitGroup
	for in.Scan() {
		line := append([]byte(nil), bytes.TrimSpace(in.Bytes())...)
		if len(line) == 0 {
			continue
		}
		wg.Add(1)
		go func() { defer wg.Done(); p.forward(line) }()
	}
	wg.Wait()
	p.endSession()
	return in.Err()
}

type proxy struct {
	endpoint, key string
	client        *http.Client
	ctx           context.Context

	mu        sync.Mutex
	sessionID string
	streaming bool

	wmu sync.Mutex
	out *bufio.Writer
}

func (p *proxy) write(b []byte) {
	p.wmu.Lock()
	defer p.wmu.Unlock()
	p.out.Write(bytes.TrimSpace(b))
	p.out.WriteByte('\n')
	p.out.Flush()
}

func (p *proxy) newRequest(method string, body io.Reader) *http.Request {
	req, _ := http.NewRequestWithContext(p.ctx, method, p.endpoint, body)
	if p.key != "" {
		req.Header.Set("Authorization", "Bearer "+p.key)
	}
	p.mu.Lock()
	if p.sessionID != "" {
		req.Header.Set("Mcp-Session-Id", p.sessionID)
	}
	p.mu.Unlock()
	return req
}

func (p *proxy) forward(line []byte) {
	req := p.newRequest(http.MethodPost, bytes.NewReader(line))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := p.client.Do(req)
	if err != nil {
		p.fail(line, "mqtt-get unreachable: "+err.Error())
		return
	}
	defer resp.Body.Close()
	if id := resp.Header.Get("Mcp-Session-Id"); id != "" {
		p.mu.Lock()
		p.sessionID = id
		start := !p.streaming
		p.streaming = true
		p.mu.Unlock()
		if start {
			go p.listen()
		}
	}
	body, _ := io.ReadAll(resp.Body)
	switch {
	case resp.StatusCode == http.StatusAccepted:
	case strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream"):
		for _, l := range strings.Split(string(body), "\n") {
			if d, ok := strings.CutPrefix(l, "data: "); ok {
				p.write([]byte(d))
			}
		}
	case resp.StatusCode == http.StatusOK:
		p.write(body)
	default:
		p.fail(line, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(body)))
	}
}

// fail answers a request with a JSON-RPC error so the client never hangs.
func (p *proxy) fail(line []byte, msg string) {
	fmt.Fprintln(os.Stderr, "mqtt-get mcp:", msg)
	var m struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(line, &m) != nil || len(m.ID) == 0 {
		return
	}
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": m.ID, "error": map[string]any{"code": -32603, "message": msg}})
	p.write(b)
}

// listen relays the server's SSE notification stream, reconnecting until
// the session ends.
func (p *proxy) listen() {
	backoff := time.Second
	for p.ctx.Err() == nil {
		req := p.newRequest(http.MethodGet, nil)
		req.Header.Set("Accept", "text/event-stream")
		resp, err := (&http.Client{}).Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			backoff = time.Second
			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(make([]byte, 64<<10), 16<<20)
			for sc.Scan() {
				if d, ok := strings.CutPrefix(sc.Text(), "data: "); ok {
					p.write([]byte(d))
				}
			}
			resp.Body.Close()
		} else if resp != nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusNotFound {
				return // session gone
			}
		}
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (p *proxy) endSession() {
	p.mu.Lock()
	id := p.sessionID
	p.mu.Unlock()
	if id == "" {
		return
	}
	req := p.newRequest(http.MethodDelete, nil)
	req = req.WithContext(context.Background())
	if resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req); err == nil {
		resp.Body.Close()
	}
}
