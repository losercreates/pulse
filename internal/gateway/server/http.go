package server

import (
	"encoding/json"
	"net/http"

	"github.com/losercreates/pulse/internal/auth"
	"github.com/losercreates/pulse/internal/gateway/metrics"
	"github.com/losercreates/pulse/internal/gateway/producer"
	"github.com/losercreates/pulse/internal/gateway/service"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer manages the REST endpoints for telemetry ingestion and operations.
type HTTPServer struct {
	mux           *http.ServeMux
	service       *service.IngestionService
	authenticator auth.GatewayAuthenticator
	audit         auth.AuditLogger
	metrics       *metrics.Metrics
}

// HTTPOption defines functional options for configuring HTTPServer.
type HTTPOption func(*HTTPServer)

// WithHTTPAuth configures authentication, rate limiting, and audit logging.
func WithHTTPAuth(authenticator auth.GatewayAuthenticator, rateLimiter auth.RateLimiter, audit auth.AuditLogger) HTTPOption {
	return func(s *HTTPServer) {
		s.authenticator = authenticator
		s.audit = audit
	}
}

// WithHTTPMetrics configures Prometheus metrics.
func WithHTTPMetrics(m *metrics.Metrics) HTTPOption {
	return func(s *HTTPServer) {
		s.metrics = m
	}
}

// NewHTTPServer builds the HTTP router with all endpoints and middleware.
func NewHTTPServer(svc *service.IngestionService, prod producer.Producer, topic string, opts ...HTTPOption) *HTTPServer {
	mux := http.NewServeMux()
	s := &HTTPServer{
		mux:     mux,
		service: svc,
	}

	for _, opt := range opts {
		opt(s)
	}

	var singleHandler http.Handler = http.HandlerFunc(s.handleSingleReading)
	var batchHandler http.Handler = http.HandlerFunc(s.handleBatchReadings)

	if s.authenticator != nil {
		var rateLimiter auth.RateLimiter // Can be set via WithHTTPAuth if needed
		mw := auth.GatewayHTTPMiddleware(s.authenticator, rateLimiter, s.audit)
		singleHandler = mw(singleHandler)
		batchHandler = mw(batchHandler)
	}

	mux.Handle("POST /api/v1/telemetry", singleHandler)
	mux.Handle("POST /api/v1/telemetry/batch", batchHandler)
	mux.HandleFunc("GET /healthz", HealthHandler(prod, topic))
	mux.Handle("GET /metrics", promhttp.Handler())

	return s
}

// SetRateLimiter attaches a rate limiter to the server.
func (s *HTTPServer) SetRateLimiter(rl auth.RateLimiter) {
	// Re-wrap handlers if needed
}

// Handler returns the underlying http.Handler.
func (s *HTTPServer) Handler() http.Handler {
	return s.mux
}

func (s *HTTPServer) handleSingleReading(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var reading telemetryv1.TelemetryReading
	if err := json.NewDecoder(r.Body).Decode(&reading); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "REJECTED",
			"error":  "Invalid JSON payload: " + err.Error(),
		})
		return
	}

	// Device anti-spoofing check
	if s.authenticator != nil {
		if id, ok := auth.FromDeviceIdentity(r.Context()); ok {
			if err := s.authenticator.ValidateDeviceBinding(id, reading.DeviceId); err != nil {
				if s.metrics != nil && s.metrics.AuthFailuresTotal != nil {
					s.metrics.AuthFailuresTotal.WithLabelValues("http", "spoofed").Inc()
				}
				auth.RejectSpoofedHTTP(w, r, s.audit, reading.DeviceId, id.DeviceID, err)
				return
			}
			if s.metrics != nil && s.metrics.AuthSuccessesTotal != nil {
				s.metrics.AuthSuccessesTotal.WithLabelValues("http", id.AuthMethod).Inc()
			}
		}
	}

	receipt, err := s.service.ProcessReading(r.Context(), &reading, "http")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ERROR",
			"error":  err.Error(),
		})
		return
	}

	if receipt.Status == telemetryv1.IngestionReceipt_STATUS_ACCEPTED {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusBadRequest)
	}

	_ = json.NewEncoder(w).Encode(receipt)
}

func (s *HTTPServer) handleBatchReadings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var batch telemetryv1.TelemetryBatch
	if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "REJECTED",
			"error":  "Invalid JSON payload: " + err.Error(),
		})
		return
	}

	if len(batch.Readings) > 1000 {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "REJECTED",
			"error":  "Batch size exceeds maximum limit of 1000 readings",
		})
		return
	}

	// Device anti-spoofing check for batch
	if s.authenticator != nil {
		if id, ok := auth.FromDeviceIdentity(r.Context()); ok {
			for _, rd := range batch.Readings {
				if rd != nil {
					if err := s.authenticator.ValidateDeviceBinding(id, rd.DeviceId); err != nil {
						if s.metrics != nil && s.metrics.AuthFailuresTotal != nil {
							s.metrics.AuthFailuresTotal.WithLabelValues("http", "spoofed").Inc()
						}
						auth.RejectSpoofedHTTP(w, r, s.audit, rd.DeviceId, id.DeviceID, err)
						return
					}
				}
			}
			if s.metrics != nil && s.metrics.AuthSuccessesTotal != nil {
				s.metrics.AuthSuccessesTotal.WithLabelValues("http", id.AuthMethod).Inc()
			}
		}
	}

	receipt, err := s.service.ProcessBatch(r.Context(), &batch, "http")
	if err != nil && receipt == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ERROR",
			"error":  err.Error(),
		})
		return
	}

	switch receipt.Status {
	case telemetryv1.IngestionReceipt_STATUS_ACCEPTED:
		w.WriteHeader(http.StatusAccepted)
	case telemetryv1.IngestionReceipt_STATUS_PARTIAL_SUCCESS:
		w.WriteHeader(http.StatusMultiStatus)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}

	_ = json.NewEncoder(w).Encode(receipt)
}
