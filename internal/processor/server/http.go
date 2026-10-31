package server

import (
	"encoding/json"
	"net/http"

	"github.com/losercreates/pulse/internal/processor/cep"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer manages HTTP observability endpoints for the Stream Processing Engine.
type HTTPServer struct {
	mux      *http.ServeMux
	reloader *cep.RuleReloader
}

// HTTPOption allows functional configuration of HTTPServer.
type HTTPOption func(*HTTPServer)

// WithRuleReloader configures the live CEP rule reloader endpoint.
func WithRuleReloader(r *cep.RuleReloader) HTTPOption {
	return func(s *HTTPServer) {
		s.reloader = r
	}
}

// NewHTTPServer builds the HTTP router with /healthz, /metrics, and rule administration endpoints.
func NewHTTPServer(opts ...HTTPOption) *HTTPServer {
	mux := http.NewServeMux()

	s := &HTTPServer{
		mux: mux,
	}

	for _, opt := range opts {
		opt(s)
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "UP",
			"system": "pulse-stream-processor",
		})
	})

	mux.Handle("GET /metrics", promhttp.Handler())

	if s.reloader != nil {
		mux.HandleFunc("POST /api/v1/rules/reload", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			rules, err := s.reloader.Reload()
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "ERROR",
					"error":  err.Error(),
				})
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status":       "SUCCESS",
				"rules_loaded": len(rules),
			})
		})
	}

	return s
}

// Handler returns the underlying http.Handler.
func (s *HTTPServer) Handler() http.Handler {
	return s.mux
}
