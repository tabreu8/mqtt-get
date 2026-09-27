// Command mqtt-get bridges an MQTT broker to systems and agents: a REST API
// (latest value of every topic, publishing, webhooks) and an MCP server, both
// over the same core, plus a small web UI.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/tabreu8/mqtt-get/internal/config"
	"github.com/tabreu8/mqtt-get/internal/core"
	"github.com/tabreu8/mqtt-get/internal/httpapi"
	"github.com/tabreu8/mqtt-get/internal/mcp"
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
		err = mcpCommand()
	case "healthcheck":
		err = healthcheck()
	case "version":
		fmt.Println(core.Version)
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

	svc := core.New(cfg, log, st)
	if err := svc.Start(envBroker, envOK); err != nil {
		log.Error("broker configuration error", "err", err)
	}
	defer svc.Close()

	var mcpHandler http.Handler
	if cfg.MCPEnabled {
		ms := mcp.New(svc, log, mcp.Options{AllowedOrigins: splitList(cfg.CORSOrigins)})
		defer ms.Close()
		mcpHandler = ms.HTTPHandler(func(r *http.Request) (*core.Principal, error) {
			return svc.Authenticate(httpapi.APIKey(r, cfg.AllowQueryAPIKey))
		}, cfg.MaxBodyBytes)
	}
	srv := httpapi.New(svc, mcpHandler)

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

func splitList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
