package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/losercreates/pulse/internal/gateway/producer"
)

// HealthResponse represents the health probe payload.
type HealthResponse struct {
	Status          string `json:"status"`
	BrokerConnected bool   `json:"broker_connected"`
	Topic           string `json:"topic"`
	Error           string `json:"error,omitempty"`
}

// HealthHandler returns an HTTP handler for /healthz.
func HealthHandler(prod producer.Producer, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		err := prod.Ping(ctx)
		w.Header().Set("Content-Type", "application/json")

		resp := HealthResponse{
			Topic: topic,
		}

		if err != nil {
			resp.Status = "DOWN"
			resp.BrokerConnected = false
			resp.Error = err.Error()
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			resp.Status = "UP"
			resp.BrokerConnected = true
			w.WriteHeader(http.StatusOK)
		}

		_ = json.NewEncoder(w).Encode(resp)
	}
}
