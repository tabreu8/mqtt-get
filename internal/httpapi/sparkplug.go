package httpapi

import (
	"net/http"
	"strconv"

	"github.com/tabreu8/mqtt-get/internal/config"
)

func (s *Server) sparkplugRoutes(mux *http.ServeMux) {
	read, pub, admin := config.ScopeRead, config.ScopePublish, config.ScopeAdmin
	mux.HandleFunc("GET /api/v1/sparkplug", s.auth(read, s.handleSparkplugGet))
	mux.HandleFunc("PUT /api/v1/sparkplug", s.auth(admin, s.handleSparkplugPut))
	mux.HandleFunc("GET /api/v1/sparkplug/nodes", s.auth(read, s.handleSparkplugNodes))
	mux.HandleFunc("POST /api/v1/sparkplug/nodes/{group}/{edge}/rebirth", s.auth(pub, s.handleSparkplugRebirth))
}

func (s *Server) handleSparkplugGet(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, s.svc.Sparkplug())
}

func (s *Server) handleSparkplugPut(w http.ResponseWriter, r *http.Request) {
	var c config.Sparkplug
	if err := s.decode(w, r, &c); err != nil {
		writeError(w, err)
		return
	}
	v, err := s.svc.SetSparkplug(c)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, v)
}

// handleSparkplugNodes lists edge nodes and devices (?group=, ?metrics=true
// to include every metric with its translated topic and type).
func (s *Server) handleSparkplugNodes(w http.ResponseWriter, r *http.Request) {
	metrics, _ := strconv.ParseBool(r.URL.Query().Get("metrics"))
	nodes, err := s.svc.SparkplugNodes(r.URL.Query().Get("group"), metrics)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"count": len(nodes), "nodes": nodes})
}

func (s *Server) handleSparkplugRebirth(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.SparkplugRebirth(r.PathValue("group"), r.PathValue("edge")); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "requested": true})
}
