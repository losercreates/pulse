package engine

import (
	"context"
	"testing"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardedMemoryEngine_DeviceCatalog(t *testing.T) {
	eng := NewShardedMemoryEngine(4)
	defer eng.Close()

	ctx := context.Background()

	// Ingest aggregates for 3 devices with multiple metrics
	batch := []*telemetryv1.StoredAggregate{
		{
			DeviceId:      "alpha-01",
			MetricName:    "temp",
			WindowStartNs: 1000,
			WindowEndNs:   2000,
			Count:         10,
			Sum:           500.0,
			Min:           45.0,
			Max:           55.0,
			Mean:          50.0,
		},
		{
			DeviceId:      "alpha-01",
			MetricName:    "vibration",
			WindowStartNs: 1500,
			WindowEndNs:   2500,
			Count:         10,
			Sum:           20.0,
			Min:           1.5,
			Max:           2.5,
			Mean:          2.0,
		},
		{
			DeviceId:      "beta-02",
			MetricName:    "pressure",
			WindowStartNs: 3000,
			WindowEndNs:   4000,
			Count:         5,
			Sum:           100.0,
			Min:           19.0,
			Max:           21.0,
			Mean:          20.0,
		},
	}

	_, _, err := eng.PutAggregates(ctx, batch)
	require.NoError(t, err)

	catalog, err := eng.GetDeviceCatalog(ctx, 10)
	require.NoError(t, err)
	require.Len(t, catalog, 2)

	// Results sorted by DeviceId
	assert.Equal(t, "alpha-01", catalog[0].DeviceId)
	assert.Equal(t, []string{"temp", "vibration"}, catalog[0].Metrics)
	assert.Equal(t, int64(1000), catalog[0].FirstSeenNs)
	assert.Equal(t, int64(2500), catalog[0].LastSeenNs)

	assert.Equal(t, "beta-02", catalog[1].DeviceId)
	assert.Equal(t, []string{"pressure"}, catalog[1].Metrics)
}
