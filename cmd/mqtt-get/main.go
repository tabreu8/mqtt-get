// Command mqtt-get bridges an MQTT broker to HTTP: REST access to the latest
// value of every topic, publishing over HTTP, webhooks, an MCP endpoint and a
// small web UI.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tabreu8/mqtt-get/internal/api"
	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/state"
)

func main() {
	cmd := "serve"
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		cmd = os.Args[1]
		os.Args = append(os.Args[:1], os.Args[2:]...)
	}
	var err error
	switch cmd {
	case "serve":
		err = serve()
	case "mcp":
		err = mcpStdio()
	case "healthcheck":
		err = healthcheck()
	case "version":
		fmt.Println(api.Version)
	default:
		fmt.Fprintf(os.Stderr, "usage: mqtt-get [serve|mcp|healthcheck|version]\n")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newLogger(level, format string) *slog.Logger {
	var lv slog.Level
	_ = lv.UnmarshalText([]byte(level))
	opts := &slog.HandlerOptions{Level: lv}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func serve() error {
	cfg, err := config.LoadServer()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(log)

	st, err := state.Open(cfg.DataDir)
	if err != nil {
		return err
	}
	st.SetEnvKeys(cfg.AdminKeys, cfg.ReadKeys, cfg.PublishKeys)
	if !cfg.AuthDisabled && !st.HasKeys() {
		_, secret, err := st.CreateKey("bootstrap admin", []string{config.ScopeAdmin})
		if err != nil {
			return err
		}
		log.Warn("no API keys configured: generated an admin key (shown only once; store it now)", "api_key", secret)
	}
	if cfg.AuthDisabled {
		log.Warn("AUTH_DISABLED=true: the API is open to anyone who can reach it")
	}

	envBroker, envOK, err := config.BrokerFromEnv()
	if err != nil {
		return fmt.Errorf("MQTT configuration: %w", err)
	}

	srv := api.New(cfg, log, st)
	if err := srv.Start(envBroker, envOK); err != nil {
		log.Error("broker configuration error", "err", err)
	}
	defer srv.Close()

	if addr := os.Getenv("PPROF_ADDR"); addr != "" {
		pm := http.NewServeMux()
		pm.HandleFunc("/debug/pprof/", pprof.Index)
		pm.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pm.HandleFunc("/debug/pprof/trace", pprof.Trace)
		go func() { log.Info("pprof: listening", "addr", addr, "err", http.ListenAndServe(addr, pm)) }()
	}

	hs := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("http: listening", "addr", cfg.HTTPAddr, "ui", cfg.UIEnabled, "mcp", cfg.MCPEnabled, "tls", cfg.TLSCertFile != "")
		if cfg.TLSCertFile != "" {
			errc <- hs.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			errc <- hs.ListenAndServe()
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return hs.Shutdown(sctx)
}

// mcpStdio bridges MCP over stdio (for clients such as Claude Desktop) to a
// running mqtt-get server's /mcp endpoint.
func mcpStdio() error {
	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	url := fs.String("url", envOr("MQTT_GET_URL", "http://localhost:8080"), "mqtt-get base URL (env MQTT_GET_URL)")
	key := fs.String("key", os.Getenv("MQTT_GET_API_KEY"), "API key (env MQTT_GET_API_KEY)")
	_ = fs.Parse(os.Args[1:])
	endpoint := strings.TrimRight(*url, "/") + "/mcp"
	client := &http.Client{Timeout: 60 * time.Second}

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 64<<10), 16<<20)
	out := bufio.NewWriter(os.Stdout)
	for in.Scan() {
		line := bytes.TrimSpace(in.Bytes())
		if len(line) == 0 {
			continue
		}
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(line))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		if *key != "" {
			req.Header.Set("Authorization", "Bearer "+*key)
		}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, "mqtt-get mcp:", err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusAccepted {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			fmt.Fprintf(os.Stderr, "mqtt-get mcp: HTTP %d: %s\n", resp.StatusCode, bytes.TrimSpace(body))
			continue
		}
		out.Write(bytes.TrimSpace(body))
		out.WriteByte('\n')
		out.Flush()
	}
	return in.Err()
}

// healthcheck exits non-zero if the local server is unhealthy (for Docker
// HEALTHCHECK in images without curl).
func healthcheck() error {
	addr := envOr("HTTP_ADDR", ":8080")
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	scheme := "http"
	if os.Getenv("HTTP_TLS_CERT_FILE") != "" {
		scheme = "https"
	}
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(scheme + "://" + addr + "/healthz")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: HTTP %d", resp.StatusCode)
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
