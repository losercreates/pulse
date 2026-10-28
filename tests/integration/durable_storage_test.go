package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// TestDurableStorage_CrashRecoveryAndDeduplication validates User Story 2 (SC-001, SC-002, SC-003):
// - Zero data loss across process termination.
// - Restart recovers state from disk WAL and SSTables with torn-tail tolerance.
// - Idempotent replay of committed records produces zero duplicate entries.
func TestDurableStorage_CrashRecoveryAndDeduplication(t *testing.T) {
	dataDir := t.TempDir()

	cfg := &config.Config{
		DataDir:            dataDir,
		NumShards:          8,
		BatchSize:          50,
		FlushInterval:      20 * time.Millisecond,
		RetentionDays:      14,
		CompactionInterval: 1 * time.Minute,
	}

	durableCfg := engine.DurableConfig{
		DataDir:            dataDir,
		NumShards:          cfg.NumShards,
		MemtableLimit:      100, // force SSTable flushes
		RetentionTTL:       14 * 24 * time.Hour,
		CompactionInterval: 1 * time.Minute,
	}

	// 1. First run: Start durable engine and sink
	eng, err := engine.NewDurableEngine(durableCfg)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	sink := consumer.NewStorageConsumerSink(cfg, nil, eng, m)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = sink.Start(ctx)
	}()

	numRecords := 500
	for i := 0; i < numRecords; i++ {
		devID := fmt.Sprintf("generator-%02d", i%5)
		start := int64(i * 10_000_000_000)
		end := start + 10_000_000_000

		sink.Inbound() <- &telemetryv1.StoredAggregate{
			DeviceId:      devID,
			MetricName:    "voltage",
			WindowStartNs: start,
			WindowEndNs:   end,
			Count:         50,
			Sum:           11500.0,
			Min:           228.0,
			Max:           232.0,
			Mean:          230.0,
		}
	}

	// Await ingestion completion
	assert.Eventually(t, func() bool {
		return eng.TotalRecords() >= numRecords
	}, 3*time.Second, 50*time.Millisecond)

	// Sample baseline query
	baselineRecords, err := eng.QueryTimeSeries(ctx, "generator-00", "voltage", 0, int64(numRecords*10_000_000_000), 1000)
	require.NoError(t, err)
	require.Equal(t, numRecords/5, len(baselineRecords))

	// Stop sink & engine to simulate abrupt process termination
	cancel()
	sink.Stop()
	require.NoError(t, eng.Close())

	// 2. Inject torn trailing write into a shard's active WAL (crash during write)
	shardDir := engine.ShardDir(dataDir, 0)
	entries, err := os.ReadDir(shardDir)
	require.NoError(t, err)
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".log" {
			f, err := os.OpenFile(filepath.Join(shardDir, entry.Name()), os.O_WRONLY|os.O_APPEND, 0644)
			require.NoError(t, err)
			// Append torn partial header (7 bytes)
			_, _ = f.Write([]byte{0x50, 0x55, 0x4C, 0x53, 0x00, 0x00, 0x10})
			_ = f.Close()
			break
		}
	}

	// 3. Restart daemon from persistent directory (Crash Recovery)
	startTime := time.Now()
	recoveredEng, err := engine.NewDurableEngine(durableCfg)
	require.NoError(t, err)
	defer recoveredEng.Close()

	recoveryDuration := time.Since(startTime)
	t.Logf("Crash recovery completed in %v", recoveryDuration)
	assert.Less(t, recoveryDuration, 5*time.Second, "Startup crash recovery must complete under 5s (SC-002)")

	// Invariant 1: 100% of acknowledged records recovered despite crash & torn tail
	recoveredRecords, err := recoveredEng.QueryTimeSeries(context.Background(), "generator-00", "voltage", 0, int64(numRecords*10_000_000_000), 1000)
	require.NoError(t, err)
	assert.Equal(t, len(baselineRecords), len(recoveredRecords), "Recovered record count must match pre-crash records (SC-001)")

	// Invariant 2: Catalog persistence recovered
	catalog, err := recoveredEng.GetDeviceCatalog(context.Background(), 10)
	require.NoError(t, err)
	assert.Equal(t, 5, len(catalog), "All 5 devices must be recovered in catalog")

	// 4. Invariant 3: Idempotent replay of previously committed records produces 0 duplicates (SC-003)
	duplicateBatch := make([]*telemetryv1.StoredAggregate, 0, len(baselineRecords))
	for _, rec := range baselineRecords {
		duplicateBatch = append(duplicateBatch, &telemetryv1.StoredAggregate{
			DeviceId:      rec.DeviceId,
			MetricName:    rec.MetricName,
			WindowStartNs: rec.WindowStartNs,
			WindowEndNs:   rec.WindowEndNs,
			Count:         rec.Count,
			Sum:           rec.Sum,
			Min:           rec.Min,
			Max:           rec.Max,
			Mean:          rec.Mean,
		})
	}

	inserted, updated, err := recoveredEng.PutAggregates(context.Background(), duplicateBatch)
	require.NoError(t, err)
	assert.Equal(t, 0, inserted, "Replayed duplicate records must result in 0 new insertions (SC-003)")
	assert.Equal(t, len(duplicateBatch), updated, "Replayed duplicate records must be detected as updates")

	// Verify query results are not doubled
	afterReplayRecords, err := recoveredEng.QueryTimeSeries(context.Background(), "generator-00", "voltage", 0, int64(numRecords*10_000_000_000), 1000)
	require.NoError(t, err)
	assert.Equal(t, len(baselineRecords), len(afterReplayRecords))
	assert.Equal(t, baselineRecords[0].Count, afterReplayRecords[0].Count)
	assert.Equal(t, baselineRecords[0].Sum, afterReplayRecords[0].Sum)
}
