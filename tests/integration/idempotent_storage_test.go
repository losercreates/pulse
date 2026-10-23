package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/storage/config"
	"github.com/losercreates/pulse/internal/storage/consumer"
	"github.com/losercreates/pulse/internal/storage/engine"
	"github.com/losercreates/pulse/internal/storage/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIdempotentStorage_CrashAndReplayInvariance validates Constitution Principle III:
// Writes to storage MUST be idempotent upserts keyed by (device_id, metric, window_start, window_end).
// Reprocessing after a consumer crash, rebalance, or historical offset rewind MUST recompute
// identical window results with ZERO double-counting or data duplication.
func TestIdempotentStorage_CrashAndReplayInvariance(t *testing.T) {
	cfg := &config.Config{
		BatchSize:     50,
		FlushInterval: 20 * time.Millisecond,
		NumShards:     8,
	}

	eng := engine.NewShardedMemoryEngine(cfg.NumShards)
	defer eng.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	sink := consumer.NewStorageConsumerSink(cfg, nil, eng, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = sink.Start(ctx)
	}()

	// 1. Initial Ingestion: Generate 500 distinct window aggregates across 5 devices
	numRecords := 500
	for i := 0; i < numRecords; i++ {
		devID := fmt.Sprintf("turbine-%02d", i%5)
		start := int64(i * 10_000_000_000)
		end := start + 10_000_000_000

		sink.Inbound() <- &telemetryv1.StoredAggregate{
			DeviceId:      devID,
			MetricName:    "vibration",
			WindowStartNs: start,
			WindowEndNs:   end,
			Count:         100,
			Sum:           500.0,
			Min:           4.5,
			Max:           5.5,
			Mean:          5.0,
		}
	}

	// Wait for initial batch writes to flush
	assert.Eventually(t, func() bool {
		return eng.TotalRecords() == numRecords
	}, 2*time.Second, 50*time.Millisecond)

	// Sample baseline query for turbine-00
	baselineQuery, err := eng.QueryTimeSeries(ctx, "turbine-00", "vibration", 0, int64(numRecords*10_000_000_000), 1000)
	require.NoError(t, err)
	expectedDeviceRecords := numRecords / 5
	require.Len(t, baselineQuery, expectedDeviceRecords)

	initialFirstCount := baselineQuery[0].Count
	initialFirstSum := baselineQuery[0].Sum

	// 2. Simulate Consumer Crash & Historical Replay
	// Re-ingest the exact same 500 records into the sink (simulating rewind of consumer offsets)
	for i := 0; i < numRecords; i++ {
		devID := fmt.Sprintf("turbine-%02d", i%5)
		start := int64(i * 10_000_000_000)
		end := start + 10_000_000_000

		sink.Inbound() <- &telemetryv1.StoredAggregate{
			DeviceId:      devID,
			MetricName:    "vibration",
			WindowStartNs: start,
			WindowEndNs:   end,
			Count:         100,
			Sum:           500.0,
			Min:           4.5,
			Max:           5.5,
			Mean:          5.0,
		}
	}

	// Allow flusher loop to process duplicate batch
	time.Sleep(100 * time.Millisecond)

	// 3. Invariant Verifications:
	// Total records stored MUST remain exactly 500 (zero duplicated rows)
	assert.Equal(t, numRecords, eng.TotalRecords(), "Total unique records must not increase on duplicate replay")

	// Query turbine-00 again and assert statistics are NOT doubled
	replayQuery, err := eng.QueryTimeSeries(ctx, "turbine-00", "vibration", 0, int64(numRecords*10_000_000_000), 1000)
	require.NoError(t, err)
	assert.Len(t, replayQuery, expectedDeviceRecords)

	assert.Equal(t, initialFirstCount, replayQuery[0].Count, "Record count must remain 100, not doubled to 200")
	assert.Equal(t, initialFirstSum, replayQuery[0].Sum, "Record sum must remain 500.0, not doubled to 1000.0")

	// 4. Verify Downsampled Rollup over the replayed records is mathematically identical
	stepNs := int64(60_000_000_000) // 1 minute
	rollups, err := eng.QueryRollup(ctx, "turbine-00", "vibration", 0, int64(numRecords*10_000_000_000), stepNs)
	require.NoError(t, err)
	assert.NotEmpty(t, rollups)

	for _, bucket := range rollups {
		assert.Equal(t, 5.0, bucket.Mean)
		assert.Equal(t, 4.5, bucket.Min)
		assert.Equal(t, 5.5, bucket.Max)
	}

	cancel()
}
