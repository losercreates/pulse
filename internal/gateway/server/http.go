package server

import (
	"encoding/json"
	"net/http"

	"github.com/losercreates/pulse/internal/gateway/producer"
	"github.com/losercreates/pulse/internal/gateway/service"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer manages the REST endpoints for telemetry ingestion and operations.
type HTTPServer struct {
	mux     *http.ServeMux
	service *service.IngestionService
}

// NewHTTPServer builds the HTTP router with all endpoints and middleware.
func NewHTTPServer(svc *service.IngestionService, prod producer.Producer, topic string) *HTTPServer {
	mux := http.NewServeMux()
	s := &HTTPServer{
		mux:     mux,
		service: svc,
	}

	mux.HandleFunc("POST /api/v1/telemetry", s.handleSingleReading)
	mux.HandleFunc("POST /api/v1/telemetry/batch", s.handleBatchReadings)
	mux.HandleFunc("GET /healthz", HealthHandler(prod, topic))
	mux.Handle("GET /metrics", promhttp.Handler())

	return s
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
