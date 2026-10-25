package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/config"
	"github.com/losercreates/pulse/internal/simulator/fleet"
	"github.com/losercreates/pulse/internal/simulator/metrics"
	"github.com/losercreates/pulse/internal/simulator/pacing"
	"github.com/losercreates/pulse/internal/simulator/report"
	"github.com/losercreates/pulse/internal/simulator/transport"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimulator_IntegrationHTTPRun(t *testing.T) {
	var receivedCount atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/telemetry/batch" {
			var batch telemetryv1.TelemetryBatch
			if err := json.NewDecoder(r.Body).Decode(&batch); err == nil {
				receivedCount.Add(int64(len(batch.Readings)))
			}
			w.WriteHeader(http.StatusAccepted)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	cfg := &config.Config{
		TargetRate:      2000,
		NumDevices:      50,
		Duration:        500 * time.Millisecond,
		Transport:       "http",
		GatewayHTTPAddr: server.URL,
		Workers:         4,
		Profile:         "steady",
	}

	tracker := metrics.NewTracker()
	f := fleet.NewFleet(cfg.NumDevices)
	sender := transport.NewHTTPSender(cfg.GatewayHTTPAddr)
	defer sender.Close()

	limiter := pacing.NewLimiter(cfg.TargetRate)
	partitions := f.WorkerPartitions(cfg.Workers)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	for workerID, partition := range partitions {
		go func(wID int, devs []*fleet.Device) {
			batchSize := 25
			idx := 0
			devCount := len(devs)
			batch := make([]*telemetryv1.TelemetryReading, 0, batchSize)

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if !limiter.Wait(ctx, batchSize) {
					return
				}

				nowNs := time.Now().UnixNano()
				batch = batch[:0]
				for i := 0; i < batchSize; i++ {
					dev := devs[idx%devCount]
					idx++
					r := dev.NextReading(nowNs, 100*time.Millisecond, 0)
					if r != nil {
						batch = append(batch, r)
					}
				}

				if len(batch) > 0 {
					sent, err := sender.SendBatch(ctx, batch)
					if err == nil {
						tracker.RecordSent(sent)
					} else {
						tracker.RecordFailed(len(batch) - sent)
					}
				}
			}
		}(workerID, partition)
	}

	<-ctx.Done()

	snap := tracker.Snapshot()
	assert.Greater(t, snap.TotalSent, int64(100), "Should have transmitted telemetry readings")
	assert.Equal(t, int64(0), snap.TotalFailed, "Should have zero errors")
	assert.Greater(t, receivedCount.Load(), int64(100), "Gateway should have received batches")

	rep := report.BuildReport(cfg, snap, 0, 0)
	require.NotNil(t, rep)
	assert.True(t, rep.SLAVerdict.ErrorSLAMet)
}
