package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/storage/config"
	"github.com/losercreates/pulse/internal/storage/engine"
	"github.com/losercreates/pulse/internal/storage/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestStorageConsumerSink_BatchingAndFlushing(t *testing.T) {
	cfg := &config.Config{
		BatchSize:     5,
		FlushInterval: 50 * time.Millisecond,
	}

	eng := engine.NewShardedMemoryEngine(4)
	defer eng.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	sink := NewStorageConsumerSink(cfg, nil, eng, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = sink.Start(ctx)
	}()

	// Ingest 5 records (triggers instant batch flush)
	for i := 0; i < 5; i++ {
		sink.Inbound() <- &telemetryv1.StoredAggregate{
			DeviceId:      "machine-01",
			MetricName:    "vibration",
			WindowStartNs: int64(i * 1000),
			WindowEndNs:   int64((i + 1) * 1000),
			Count:         10,
			Sum:           50.0,
			Min:           4.0,
			Max:           6.0,
			Mean:          5.0,
		}
	}

	assert.Eventually(t, func() bool {
		return eng.TotalRecords() == 5
	}, 1*time.Second, 20*time.Millisecond)

	// Ingest 2 records (less than batch size, flushes on ticker)
	for i := 5; i < 7; i++ {
		sink.Inbound() <- &telemetryv1.StoredAggregate{
			DeviceId:      "machine-01",
			MetricName:    "vibration",
			WindowStartNs: int64(i * 1000),
			WindowEndNs:   int64((i + 1) * 1000),
			Count:         10,
			Sum:           50.0,
			Min:           4.0,
			Max:           6.0,
			Mean:          5.0,
		}
	}

	assert.Eventually(t, func() bool {
		return eng.TotalRecords() == 7
	}, 1*time.Second, 20*time.Millisecond)

	cancel()
}
