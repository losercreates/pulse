package fleet_test

import (
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/fleet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFleet_CreationAndArchetypes(t *testing.T) {
	numDevices := 100
	f := fleet.NewFleet(numDevices)
	assert.Equal(t, numDevices, f.TotalDevices())

	devices := f.Devices()
	require.Len(t, devices, numDevices)

	// Check diversity of archetypes
	archCounts := make(map[fleet.Archetype]int)
	for _, d := range devices {
		archCounts[d.Archetype]++
		assert.NotEmpty(t, d.ID)
		assert.NotEmpty(t, d.Metrics)
	}

	assert.Equal(t, 25, archCounts[fleet.ArchetypeTurbine])
	assert.Equal(t, 25, archCounts[fleet.ArchetypeMotor])
	assert.Equal(t, 25, archCounts[fleet.ArchetypePump])
	assert.Equal(t, 25, archCounts[fleet.ArchetypeGenerator])
}

func TestFleet_WorkerPartitioning(t *testing.T) {
	numDevices := 1000
	numWorkers := 16
	f := fleet.NewFleet(numDevices)

	partitions := f.WorkerPartitions(numWorkers)
	require.Len(t, partitions, numWorkers)

	totalAssigned := 0
	for _, part := range partitions {
		totalAssigned += len(part)
		assert.InDelta(t, numDevices/numWorkers, len(part), 2)
	}
	assert.Equal(t, numDevices, totalAssigned)
}

func TestDevice_NextReadingAndOfflineBuffering(t *testing.T) {
	f := fleet.NewFleet(1)
	dev := f.Devices()[0]

	ts := time.Now().UnixNano()
	reading := dev.NextReading(ts, 1*time.Second, 0)
	require.NotNil(t, reading)
	assert.Equal(t, dev.ID, reading.DeviceId)
	assert.Equal(t, ts, reading.TimestampNs)
	assert.NotEmpty(t, reading.MetricName)
	assert.NotEmpty(t, reading.Unit)

	// Test offline mode
	dev.SetOffline(true)
	assert.True(t, dev.IsOffline())

	offlineReading := dev.NextReading(ts+1000, 2*time.Second, 0)
	assert.Nil(t, offlineReading, "Offline device should buffer and return nil")

	buffered := dev.DrainOfflineBuffer()
	require.Len(t, buffered, 1)
	assert.Equal(t, ts+1000, buffered[0].TimestampNs)

	// Buffer should be cleared after drain
	assert.Empty(t, dev.DrainOfflineBuffer())
}
