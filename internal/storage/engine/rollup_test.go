package engine

import (
	"context"
	"testing"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedMemoryEngine_RollupDownsampling(t *testing.T) {
	eng := NewShardedMemoryEngine(4)
	defer eng.Close()

	ctx := context.Background()

	// Insert 12 sequential 10-second window aggregates (total 120s = 2 minutes)
	// First 6 windows: in [0, 60s) bucket
	// Next 6 windows: in [60s, 120s) bucket
	var batch []*telemetryv1.StoredAggregate
	for i := 0; i < 12; i++ {
		start := int64(i * 10_000_000_000)
		end := start + 10_000_000_000
		val := 50.0 + float64(i)
		batch = append(batch, &telemetryv1.StoredAggregate{
			DeviceId:      "generator-01",
			MetricName:    "voltage",
			WindowStartNs: start,
			WindowEndNs:   end,
			Count:         10,
			Sum:           val * 10,
			Min:           val - 1.0,
			Max:           val + 1.0,
			Mean:          val,
		})
	}

	_, _, err := eng.PutAggregates(ctx, batch)
	require.NoError(t, err)

	// Query with 1-minute rollup step (60,000,000,000 ns)
	stepNs := int64(60_000_000_000)
	buckets, err := eng.QueryRollup(ctx, "generator-01", "voltage", 0, 120_000_000_000, stepNs)
	require.NoError(t, err)
	require.Len(t, buckets, 2)

	// Bucket 0: [0, 60s) -> 6 intervals (i=0..5)
	// Counts: 6 * 10 = 60
	// Global Min: min(50-1) = 49.0
	// Global Max: max(55+1) = 56.0
	b0 := buckets[0]
	assert.Equal(t, int64(0), b0.BucketStartNs)
	assert.Equal(t, stepNs, b0.BucketEndNs)
	assert.Equal(t, uint64(60), b0.Count)
	assert.Equal(t, 49.0, b0.Min)
	assert.Equal(t, 56.0, b0.Max)
	// Sum: 500 + 510 + 520 + 530 + 540 + 550 = 3150
	assert.Equal(t, 3150.0, b0.Sum)
	// Weighted Mean: 3150 / 60 = 52.5
	assert.Equal(t, 52.5, b0.Mean)

	// Bucket 1: [60s, 120s) -> 6 intervals (i=6..11)
	b1 := buckets[1]
	assert.Equal(t, stepNs, b1.BucketStartNs)
	assert.Equal(t, int64(120_000_000_000), b1.BucketEndNs)
	assert.Equal(t, uint64(60), b1.Count)
}
