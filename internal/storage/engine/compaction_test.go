package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/storage/engine"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompaction_MultiWayMergeAndDeduplication(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := engine.DurableConfig{
		DataDir:            tmpDir,
		NumShards:          1,
		MemtableLimit:      5,
		RetentionTTL:       0, // disable TTL pruning for pure deduplication test
		CompactionInterval: 1 * time.Minute,
	}

	eng, err := engine.NewDurableEngine(cfg)
	require.NoError(t, err)
	defer eng.Close()

	ctx := context.Background()

	// Ingest batch 1: records 0..9 (forces 2 flushes at limit=5)
	var batch1 []*telemetryv1.StoredAggregate
	for i := 0; i < 10; i++ {
		batch1 = append(batch1, &telemetryv1.StoredAggregate{
			DeviceId:      "dev-01",
			MetricName:    "rpm",
			WindowStartNs: int64(i * 1000),
			WindowEndNs:   int64((i + 1) * 1000),
			Count:         10,
			Mean:          100.0,
		})
	}
	_, _, err = eng.PutAggregates(ctx, batch1)
	require.NoError(t, err)

	// Flush to ensure all records are in SSTables
	require.NoError(t, eng.FlushAll())

	// Ingest batch 2: update records 0..4 with new Mean=200.0 (forces more flushes)
	var batch2 []*telemetryv1.StoredAggregate
	for i := 0; i < 5; i++ {
		batch2 = append(batch2, &telemetryv1.StoredAggregate{
			DeviceId:      "dev-01",
			MetricName:    "rpm",
			WindowStartNs: int64(i * 1000),
			WindowEndNs:   int64((i + 1) * 1000),
			Count:         20,
			Mean:          200.0,
		})
	}
	_, _, err = eng.PutAggregates(ctx, batch2)
	require.NoError(t, err)
	require.NoError(t, eng.FlushAll())

	// Verify shard has multiple SSTables before compaction
	shardDir := filepath.Join(tmpDir, "shard-00")
	entriesBefore, err := os.ReadDir(shardDir)
	require.NoError(t, err)
	sstCountBefore := 0
	for _, e := range entriesBefore {
		if filepath.Ext(e.Name()) == ".sst" {
			sstCountBefore++
		}
	}
	assert.Greater(t, sstCountBefore, 1, "Should have multiple SSTables before compaction")

	// Run Compaction
	reclaimed, err := eng.CompactAll()
	require.NoError(t, err)
	t.Logf("Compaction reclaimed %d bytes", reclaimed)

	// Verify shard has only 1 compacted SSTable now
	entriesAfter, err := os.ReadDir(shardDir)
	require.NoError(t, err)
	sstCountAfter := 0
	for _, e := range entriesAfter {
		if filepath.Ext(e.Name()) == ".sst" {
			sstCountAfter++
		}
	}
	assert.Equal(t, 1, sstCountAfter, "Compacted shard must consolidate into 1 SSTable")

	// Verify updated data: records 0..4 should have Mean=200.0, records 5..9 have Mean=100.0
	res, err := eng.QueryTimeSeries(ctx, "dev-01", "rpm", 0, 100000, 100)
	require.NoError(t, err)
	require.Len(t, res, 10)

	for i := 0; i < 5; i++ {
		assert.Equal(t, 200.0, res[i].Mean)
		assert.Equal(t, uint64(20), res[i].Count)
	}
	for i := 5; i < 10; i++ {
		assert.Equal(t, 100.0, res[i].Mean)
		assert.Equal(t, uint64(10), res[i].Count)
	}

	// Verify disk usage calculation
	usage, err := eng.TotalDiskUsage()
	require.NoError(t, err)
	assert.Greater(t, usage, int64(0))
}

func TestCompaction_TTLPruning(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := engine.DurableConfig{
		DataDir:            tmpDir,
		NumShards:          1,
		MemtableLimit:      5,
		RetentionTTL:       1 * time.Hour, // 1 hour TTL
		CompactionInterval: 1 * time.Minute,
	}

	eng, err := engine.NewDurableEngine(cfg)
	require.NoError(t, err)
	defer eng.Close()

	ctx := context.Background()

	now := time.Now()
	expiredStart := now.Add(-3 * time.Hour).UnixNano()
	expiredEnd := expiredStart + 1000

	activeStart := now.Add(-10 * time.Minute).UnixNano()
	activeEnd := activeStart + 1000

	records := []*telemetryv1.StoredAggregate{
		{
			DeviceId:      "dev-ttl",
			MetricName:    "temp",
			WindowStartNs: expiredStart,
			WindowEndNs:   expiredEnd,
			Count:         5,
			Mean:          10.0,
		},
		{
			DeviceId:      "dev-ttl",
			MetricName:    "temp",
			WindowStartNs: activeStart,
			WindowEndNs:   activeEnd,
			Count:         15,
			Mean:          20.0,
		},
	}

	_, _, err = eng.PutAggregates(ctx, records)
	require.NoError(t, err)
	require.NoError(t, eng.FlushAll())

	// Run compaction with 1 hour TTL
	_, err = eng.CompactAll()
	require.NoError(t, err)

	// Query across range spanning both records
	res, err := eng.QueryTimeSeries(ctx, "dev-ttl", "temp", expiredStart-1000, activeEnd+1000, 100)
	require.NoError(t, err)

	// The expired record must be evicted, only the active record remains!
	require.Len(t, res, 1, "Expired record must be pruned by TTL during compaction")
	assert.Equal(t, activeStart, res[0].WindowStartNs)
	assert.Equal(t, 20.0, res[0].Mean)
}
