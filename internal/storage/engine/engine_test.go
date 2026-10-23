package engine

import (
	"context"
	"fmt"
	"testing"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompositeKeyFormat(t *testing.T) {
	key := FormatCompositeKey("turbine-01", "bearing_temp", 1000000000, 1010000000)
	assert.Equal(t, "turbine-01#bearing_temp#1000000000#1010000000", key)
}

func TestShardedMemoryEngine_IdempotentUpsert(t *testing.T) {
	eng := NewShardedMemoryEngine(4)
	defer eng.Close()

	ctx := context.Background()

	record1 := &telemetryv1.StoredAggregate{
		DeviceId:      "pump-01",
		MetricName:    "vibration",
		WindowStartNs: 1000,
		WindowEndNs:   2000,
		Count:         10,
		Sum:           50.0,
		Min:           4.5,
		Max:           5.5,
		Mean:          5.0,
	}

	// 1. Initial write
	inserted, updated, err := eng.PutAggregates(ctx, []*telemetryv1.StoredAggregate{record1})
	require.NoError(t, err)
	assert.Equal(t, 1, inserted)
	assert.Equal(t, 0, updated)
	assert.Equal(t, 1, eng.TotalRecords())

	// 2. Replay of identical record (simulating broker offset replay after crash)
	// Constitution Principle III: MUST be strictly idempotent, zero duplicate rows, zero doubled counts
	inserted, updated, err = eng.PutAggregates(ctx, []*telemetryv1.StoredAggregate{record1})
	require.NoError(t, err)
	assert.Equal(t, 0, inserted, "Replayed aggregate must not increment inserted count")
	assert.Equal(t, 1, updated, "Replayed aggregate must be recognized as existing upsert")
	assert.Equal(t, 1, eng.TotalRecords(), "Total unique records must remain exactly 1")

	// 3. Query back the record
	res, err := eng.QueryTimeSeries(ctx, "pump-01", "vibration", 0, 3000, 10)
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.Equal(t, uint64(10), res[0].Count, "Count must remain 10, not doubled to 20")
	assert.Equal(t, 50.0, res[0].Sum, "Sum must remain 50.0, not doubled to 100.0")
	assert.Equal(t, 5.0, res[0].Mean)
}

func TestShardedMemoryEngine_TimeRangeQuery(t *testing.T) {
	eng := NewShardedMemoryEngine(4)
	defer eng.Close()

	ctx := context.Background()

	// Insert 10 sequential 10-second window aggregates
	var batch []*telemetryv1.StoredAggregate
	for i := 0; i < 10; i++ {
		start := int64(i * 10_000_000_000)
		end := start + 10_000_000_000
		batch = append(batch, &telemetryv1.StoredAggregate{
			DeviceId:      "turbine-02",
			MetricName:    "bearing_temp",
			WindowStartNs: start,
			WindowEndNs:   end,
			Count:         100,
			Sum:           7200.0,
			Min:           70.0,
			Max:           74.0,
			Mean:          72.0,
		})
	}

	inserted, _, err := eng.PutAggregates(ctx, batch)
	require.NoError(t, err)
	assert.Equal(t, 10, inserted)

	// Query partial range: windows from 20s to 70s (indices 2, 3, 4, 5, 6)
	res, err := eng.QueryTimeSeries(ctx, "turbine-02", "bearing_temp", 20_000_000_000, 70_000_000_000, 100)
	require.NoError(t, err)
	require.Len(t, res, 5)
	assert.Equal(t, int64(20_000_000_000), res[0].WindowStartNs)
	assert.Equal(t, int64(60_000_000_000), res[4].WindowStartNs)

	// Query with limit 2
	limited, err := eng.QueryTimeSeries(ctx, "turbine-02", "bearing_temp", 0, 100_000_000_000, 2)
	require.NoError(t, err)
	assert.Len(t, limited, 2)

	// Query non-existent metric returns empty slice with no error
	empty, err := eng.QueryTimeSeries(ctx, "turbine-02", "non_existent", 0, 100_000_000_000, 10)
	require.NoError(t, err)
	assert.Empty(t, empty)
}

func TestShardedMemoryEngine_MultiDeviceSharding(t *testing.T) {
	eng := NewShardedMemoryEngine(8)
	defer eng.Close()

	ctx := context.Background()

	// Insert aggregates for 100 distinct devices
	var batch []*telemetryv1.StoredAggregate
	for i := 0; i < 100; i++ {
		devID := fmt.Sprintf("sensor-%03d", i)
		batch = append(batch, &telemetryv1.StoredAggregate{
			DeviceId:      devID,
			MetricName:    "vibration",
			WindowStartNs: 1000,
			WindowEndNs:   2000,
			Count:         50,
			Sum:           250.0,
			Min:           4.0,
			Max:           6.0,
			Mean:          5.0,
		})
	}

	inserted, _, err := eng.PutAggregates(ctx, batch)
	require.NoError(t, err)
	assert.Equal(t, 100, inserted)
	assert.Equal(t, 100, eng.TotalRecords())

	// Verify each device's query works independently
	for i := 0; i < 100; i++ {
		devID := fmt.Sprintf("sensor-%03d", i)
		res, err := eng.QueryTimeSeries(ctx, devID, "vibration", 0, 5000, 10)
		require.NoError(t, err)
		require.Len(t, res, 1)
		assert.Equal(t, devID, res[0].DeviceId)
	}
}
