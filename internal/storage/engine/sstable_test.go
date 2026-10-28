package engine_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/losercreates/pulse/internal/storage/engine"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSTable_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "segment-001.sst")

	var records []*telemetryv1.StoredAggregate
	for i := 0; i < 200; i++ {
		records = append(records, &telemetryv1.StoredAggregate{
			DeviceId:      fmt.Sprintf("dev-%03d", i%10),
			MetricName:    "temperature",
			WindowStartNs: int64(i * 1000),
			WindowEndNs:   int64((i + 1) * 1000),
			Count:         uint64(i + 1),
			Mean:          float64(20 + i),
		})
	}

	err := engine.WriteSSTable(sstPath, records)
	require.NoError(t, err)

	reader, err := engine.OpenSSTableReader(sstPath)
	require.NoError(t, err)
	defer reader.Close()

	assert.Equal(t, uint32(200), reader.RecordCount())

	// 1. Point lookup for an existing key
	targetKey := engine.FormatCompositeKey("dev-000", "temperature", 0, 1000)
	found, exists, err := reader.SearchPoint(targetKey)
	require.NoError(t, err)
	assert.True(t, exists)
	require.NotNil(t, found)
	assert.Equal(t, "dev-000", found.DeviceId)
	assert.Equal(t, int64(0), found.WindowStartNs)

	// 2. Point lookup for non-existent key
	nonExistentKey := engine.FormatCompositeKey("dev-999", "temperature", 99999999, 999999999)
	found, exists, err = reader.SearchPoint(nonExistentKey)
	require.NoError(t, err)
	assert.False(t, exists)
	assert.Nil(t, found)

	// 3. ScanRange for dev-001
	rangeResults, err := reader.ScanRange("dev-001", "temperature", 0, 50000)
	require.NoError(t, err)
	assert.NotEmpty(t, rangeResults)
	for _, rec := range rangeResults {
		assert.Equal(t, "dev-001", rec.DeviceId)
		assert.Equal(t, "temperature", rec.MetricName)
		assert.True(t, rec.WindowStartNs >= 0 && rec.WindowStartNs < 50000)
	}

	// 4. ReadAllRecords
	all, err := reader.ReadAllRecords()
	require.NoError(t, err)
	assert.Len(t, all, 200)
}

func TestBloomFilter_Membership(t *testing.T) {
	bf := engine.NewBloomFilter(100)
	bf.Add("turbine-01#temp#100#200")
	bf.Add("turbine-02#pressure#300#400")

	assert.True(t, bf.MayContain("turbine-01#temp#100#200"))
	assert.True(t, bf.MayContain("turbine-02#pressure#300#400"))
	assert.False(t, bf.MayContain("turbine-99#nonexistent#0#0"))
}
