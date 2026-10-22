package server

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer manages HTTP observability endpoints for the Alert Dispatcher Daemon.
type HTTPServer struct {
	mux *http.ServeMux
}

// NewHTTPServer builds the HTTP router with /healthz and /metrics on designated port :8083.
func NewHTTPServer() *HTTPServer {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "UP",
			"system": "pulse-alert-dispatcher",
		})
	})

	mux.Handle("GET /metrics", promhttp.Handler())

	return &HTTPServer{
		mux: mux,
	}
}

// Handler returns the underlying http.Handler.
func (s *HTTPServer) Handler() http.Handler {
	return s.mux
}
