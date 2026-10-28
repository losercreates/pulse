package engine_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/storage/engine"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDurableEngine_LifecycleAndRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := engine.DurableConfig{
		DataDir:            tmpDir,
		NumShards:          4,
		MemtableLimit:      10, // small limit to force SSTable flush
		CompactionInterval: 1 * time.Minute,
	}

	// 1. Start engine and write records
	dEngine, err := engine.NewDurableEngine(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	var records []*telemetryv1.StoredAggregate
	for i := 0; i < 25; i++ {
		records = append(records, &telemetryv1.StoredAggregate{
			DeviceId:      fmt.Sprintf("dev-%02d", i%5),
			MetricName:    "vibration",
			WindowStartNs: int64(i * 1000),
			WindowEndNs:   int64((i + 1) * 1000),
			Count:         uint64(i + 1),
			Sum:           float64(10 * (i + 1)),
			Min:           5.0,
			Max:           15.0,
			Mean:          10.0,
		})
	}

	inserted, updated, err := dEngine.PutAggregates(ctx, records)
	require.NoError(t, err)
	assert.Equal(t, 25, inserted)
	assert.Equal(t, 0, updated)

	// Idempotent duplicate write
	inserted, updated, err = dEngine.PutAggregates(ctx, records)
	require.NoError(t, err)
	assert.Equal(t, 0, inserted)
	assert.Equal(t, 25, updated)

	// Query TimeSeries
	res, err := dEngine.QueryTimeSeries(ctx, "dev-00", "vibration", 0, 100000, 100)
	require.NoError(t, err)
	assert.NotEmpty(t, res)

	// Catalog check
	catalog, err := dEngine.GetDeviceCatalog(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, catalog, 5)

	// Close engine
	err = dEngine.Close()
	require.NoError(t, err)

	// 2. Restart engine from same directory (Recovery Protocol)
	recoveredEngine, err := engine.NewDurableEngine(cfg)
	require.NoError(t, err)
	defer recoveredEngine.Close()

	// Verify records are intact after recovery
	resRecovered, err := recoveredEngine.QueryTimeSeries(ctx, "dev-00", "vibration", 0, 100000, 100)
	require.NoError(t, err)
	assert.Equal(t, len(res), len(resRecovered))

	recoveredCatalog, err := recoveredEngine.GetDeviceCatalog(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, recoveredCatalog, 5)
}
