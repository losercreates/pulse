package canary_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/canary"
	"github.com/losercreates/pulse/internal/simulator/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSender struct {
	sent []*telemetryv1.TelemetryReading
}

func (m *mockSender) SendBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	m.sent = append(m.sent, readings...)
	return len(readings), nil
}

func (m *mockSender) Close() error {
	return nil
}

func TestCanaryProbe_SendAndResolve(t *testing.T) {
	// Mock ChronoDB query server
	queried := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/query", r.URL.Path)
		queried = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_id":     "canary-probe-01",
			"metric_name":   "canary_latency",
			"total_records": 1,
			"records": []map[string]any{
				{
					"window_start_ns": time.Now().Add(-5 * time.Second).UnixNano(),
					"window_end_ns":   time.Now().UnixNano(),
					"count":           1,
					"mean":            1.0,
				},
			},
		})
	}))
	defer server.Close()

	sender := &mockSender{}
	tracker := metrics.NewTracker()

	probe := canary.NewProbe(sender, server.URL, tracker, 100*time.Millisecond)
	err := probe.SendProbe(context.Background())
	require.NoError(t, err)

	assert.True(t, queried)
	assert.Len(t, sender.sent, 1)
	assert.Equal(t, "canary-probe-01", sender.sent[0].DeviceId)

	snap := tracker.Snapshot()
	assert.Equal(t, 1, snap.CanaryCount)
	assert.Greater(t, snap.CanaryP50Ms, 0.0)
}
