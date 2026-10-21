package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/gateway/metrics"
	"github.com/losercreates/pulse/internal/gateway/server"
	"github.com/losercreates/pulse/internal/gateway/service"
	"github.com/losercreates/pulse/internal/gateway/validator"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockIntegrationProducer struct {
	published []*telemetryv1.TelemetryReading
}

func (m *mockIntegrationProducer) Publish(ctx context.Context, reading *telemetryv1.TelemetryReading) error {
	m.published = append(m.published, reading)
	return nil
}

func (m *mockIntegrationProducer) PublishBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	m.published = append(m.published, readings...)
	return len(readings), nil
}

func (m *mockIntegrationProducer) Ping(ctx context.Context) error {
	return nil
}

func (m *mockIntegrationProducer) Close() {}

func setupTestServer(t *testing.T) (*httptest.Server, *mockIntegrationProducer) {
	t.Helper()

	m := metrics.New(prometheus.NewRegistry())
	val := validator.New(5*time.Minute, 24*time.Hour)
	prod := &mockIntegrationProducer{}
	svc := service.NewIngestionService(val, prod, m)
	httpWrapper := server.NewHTTPServer(svc, prod, "telemetry.raw")

	ts := httptest.NewServer(httpWrapper.Handler())
	t.Cleanup(ts.Close)

	return ts, prod
}

func TestHTTPServer_SingleReading(t *testing.T) {
	ts, prod := setupTestServer(t)
	now := time.Now()

	t.Run("Valid Reading returns 202 Accepted", func(t *testing.T) {
		payload := map[string]any{
			"device_id":    "turbine-01",
			"timestamp_ns": now.UnixNano(),
			"metric_name":  "bearing_temp",
			"value":        72.5,
			"unit":         "celsius",
		}
		body, _ := json.Marshal(payload)

		resp, err := http.Post(ts.URL+"/api/v1/telemetry", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusAccepted, resp.StatusCode)
		assert.Len(t, prod.published, 1)
		assert.Equal(t, "turbine-01", prod.published[0].DeviceId)
	})

	t.Run("Missing Device ID returns 400 Bad Request", func(t *testing.T) {
		prod.published = nil
		payload := map[string]any{
			"device_id":    "",
			"timestamp_ns": now.UnixNano(),
			"metric_name":  "bearing_temp",
			"value":        72.5,
		}
		body, _ := json.Marshal(payload)

		resp, err := http.Post(ts.URL+"/api/v1/telemetry", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Empty(t, prod.published)
	})

	t.Run("Future Timestamp (>5m) returns 400 Bad Request", func(t *testing.T) {
		payload := map[string]any{
			"device_id":    "turbine-01",
			"timestamp_ns": now.Add(10 * time.Minute).UnixNano(),
			"metric_name":  "bearing_temp",
			"value":        72.5,
		}
		body, _ := json.Marshal(payload)

		resp, err := http.Post(ts.URL+"/api/v1/telemetry", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

func TestHTTPServer_BatchReadings(t *testing.T) {
	ts, prod := setupTestServer(t)
	now := time.Now()

	t.Run("Batch of 5 readings returns 202 Accepted", func(t *testing.T) {
		prod.published = nil
		var readings []map[string]any
		for i := 0; i < 5; i++ {
			readings = append(readings, map[string]any{
				"device_id":    "dev-01",
				"timestamp_ns": now.UnixNano(),
				"metric_name":  "rpm",
				"value":        1500.0,
			})
		}
		body, _ := json.Marshal(map[string]any{"readings": readings})

		resp, err := http.Post(ts.URL+"/api/v1/telemetry/batch", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusAccepted, resp.StatusCode)
		assert.Len(t, prod.published, 5)
	})
}

func TestHTTPServer_HealthAndMetrics(t *testing.T) {
	ts, _ := setupTestServer(t)

	t.Run("Healthz returns 200 UP", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/healthz")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var health server.HealthResponse
		err = json.NewDecoder(resp.Body).Decode(&health)
		require.NoError(t, err)
		assert.Equal(t, "UP", health.Status)
		assert.True(t, health.BrokerConnected)
		assert.Equal(t, "telemetry.raw", health.Topic)
	})

	t.Run("Metrics endpoint returns Prometheus text format", func(t *testing.T) {
		resp, err := http.Get(ts.URL + "/metrics")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}
