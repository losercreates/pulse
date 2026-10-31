package bridge_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge"
	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/losercreates/pulse/internal/bridge/spooler"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockForwarder struct {
	mu         sync.Mutex
	shouldFail bool
	forwarded  []*telemetryv1.TelemetryReading
}

func (m *mockForwarder) ForwardBatch(ctx context.Context, batch []*telemetryv1.TelemetryReading) (*telemetryv1.IngestionReceipt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.shouldFail {
		return nil, errors.New("network partition disconnected")
	}
	m.forwarded = append(m.forwarded, batch...)
	return &telemetryv1.IngestionReceipt{Status: telemetryv1.IngestionReceipt_STATUS_ACCEPTED, ProcessedCount: int32(len(batch))}, nil
}

func (m *mockForwarder) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.shouldFail
}

func (m *mockForwarder) Close() error {
	return nil
}

func TestCoordinator_DirectForwardAndSpoolBackfill(t *testing.T) {
	tmpDir := t.TempDir()
	sp, err := spooler.NewDiskSpooler(tmpDir, 10*1024*1024)
	require.NoError(t, err)

	fwd := &mockForwarder{shouldFail: false}

	cfg := &config.EdgeBridgeConfig{
		BridgeID:        "test-bridge",
		BatchLinger:     10 * time.Millisecond,
		BatchMaxRecords: 5,
	}

	coord := bridge.NewCoordinator(cfg, nil, sp, fwd, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, coord.Start(ctx))

	// Ingest 3 readings while connected
	coord.IngestChannel() <- &telemetryv1.TelemetryReading{DeviceId: "d1", MetricName: "m1", Value: 10}
	coord.IngestChannel() <- &telemetryv1.TelemetryReading{DeviceId: "d1", MetricName: "m2", Value: 20}
	coord.IngestChannel() <- &telemetryv1.TelemetryReading{DeviceId: "d1", MetricName: "m3", Value: 30}

	// Wait for linger flush
	time.Sleep(50 * time.Millisecond)

	fwd.mu.Lock()
	assert.Len(t, fwd.forwarded, 3)
	fwd.mu.Unlock()
	assert.Equal(t, int64(0), sp.BufferedCount())

	// Simulate Network Partition: fail forwarder
	fwd.mu.Lock()
	fwd.shouldFail = true
	fwd.mu.Unlock()

	coord.IngestChannel() <- &telemetryv1.TelemetryReading{DeviceId: "d1", MetricName: "m4", Value: 40}
	coord.IngestChannel() <- &telemetryv1.TelemetryReading{DeviceId: "d1", MetricName: "m5", Value: 50}

	// Wait for flush to attempt forward, fail, and spill to disk spool
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(2), sp.BufferedCount())

	// Reconnect gateway
	fwd.mu.Lock()
	fwd.shouldFail = false
	fwd.mu.Unlock()

	// Next ticker tick should drain spool in FIFO order
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int64(0), sp.BufferedCount())

	fwd.mu.Lock()
	assert.Len(t, fwd.forwarded, 5)
	assert.Equal(t, float64(40), fwd.forwarded[3].Value)
	assert.Equal(t, float64(50), fwd.forwarded[4].Value)
	fwd.mu.Unlock()

	require.NoError(t, coord.Stop())
}
