package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPSender_SendBatch(t *testing.T) {
	var receivedBatch telemetryv1.TelemetryBatch
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/telemetry/batch", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		err := json.NewDecoder(r.Body).Decode(&receivedBatch)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	sender := NewHTTPSender(server.URL)
	defer sender.Close()

	readings := []*telemetryv1.TelemetryReading{
		{
			DeviceId:    "turbine-0001",
			TimestampNs: time.Now().UnixNano(),
			MetricName:  "vibration",
			Value:       2.5,
			Unit:        "mm/s",
		},
		{
			DeviceId:    "turbine-0002",
			TimestampNs: time.Now().UnixNano(),
			MetricName:  "bearing_temp",
			Value:       65.4,
			Unit:        "celsius",
		},
	}

	sent, err := sender.SendBatch(context.Background(), readings)
	require.NoError(t, err)
	assert.Equal(t, 2, sent)
	assert.Len(t, receivedBatch.Readings, 2)
	assert.Equal(t, "turbine-0001", receivedBatch.Readings[0].DeviceId)
}
