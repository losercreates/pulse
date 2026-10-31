package spooler_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge/spooler"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskSpooler_WriteReadAck(t *testing.T) {
	tmpDir := t.TempDir()
	sp, err := spooler.NewDiskSpooler(tmpDir, 100*1024*1024)
	require.NoError(t, err)
	defer sp.Close()

	readings := []*telemetryv1.TelemetryReading{
		{
			DeviceId:    "dev-001",
			MetricName:  "vibration",
			Value:       3.14,
			TimestampNs: time.Now().UnixNano(),
			Unit:        "mm/s",
		},
		{
			DeviceId:    "dev-001",
			MetricName:  "temperature",
			Value:       68.5,
			TimestampNs: time.Now().UnixNano(),
			Unit:        "celsius",
		},
	}

	// Write batch
	err = sp.Write(readings)
	require.NoError(t, err)
	assert.Equal(t, int64(2), sp.BufferedCount())

	// Read batch
	read, err := sp.ReadNext(10)
	require.NoError(t, err)
	require.Len(t, read, 2)
	assert.Equal(t, "dev-001", read[0].DeviceId)
	assert.Equal(t, "vibration", read[0].MetricName)
	assert.Equal(t, 3.14, read[0].Value)
	assert.Equal(t, "temperature", read[1].MetricName)

	// Second read before ack should return empty (unacked read pending)
	read2, err := sp.ReadNext(10)
	require.NoError(t, err)
	assert.Empty(t, read2)

	// Ack both records
	err = sp.Ack(2)
	require.NoError(t, err)
	assert.Equal(t, int64(0), sp.BufferedCount())

	// Next read should be empty
	read3, err := sp.ReadNext(10)
	require.NoError(t, err)
	assert.Empty(t, read3)
}

func TestDiskSpooler_CrashRecovery(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "crash_test")
	sp1, err := spooler.NewDiskSpooler(tmpDir, 100*1024*1024)
	require.NoError(t, err)

	records := []*telemetryv1.TelemetryReading{
		{DeviceId: "turbine-01", MetricName: "rpm", Value: 3000, TimestampNs: 1000},
		{DeviceId: "turbine-01", MetricName: "rpm", Value: 3050, TimestampNs: 2000},
		{DeviceId: "turbine-01", MetricName: "rpm", Value: 3100, TimestampNs: 3000},
	}

	require.NoError(t, sp1.Write(records))
	assert.Equal(t, int64(3), sp1.BufferedCount())
	require.NoError(t, sp1.Close())

	// Reopen spooler as if process crashed and restarted
	sp2, err := spooler.NewDiskSpooler(tmpDir, 100*1024*1024)
	require.NoError(t, err)
	defer sp2.Close()

	assert.Equal(t, int64(3), sp2.BufferedCount())
	read, err := sp2.ReadNext(2)
	require.NoError(t, err)
	require.Len(t, read, 2)
	assert.Equal(t, float64(3000), read[0].Value)
	assert.Equal(t, float64(3050), read[1].Value)

	require.NoError(t, sp2.Ack(2))
	assert.Equal(t, int64(1), sp2.BufferedCount())

	readRem, err := sp2.ReadNext(10)
	require.NoError(t, err)
	require.Len(t, readRem, 1)
	assert.Equal(t, float64(3100), readRem[0].Value)
	require.NoError(t, sp2.Ack(1))
	assert.Equal(t, int64(0), sp2.BufferedCount())
}

func TestDiskSpooler_CapacityLimit(t *testing.T) {
	tmpDir := t.TempDir()
	// Allocate small capacity limit (e.g. 500 bytes)
	sp, err := spooler.NewDiskSpooler(tmpDir, 500)
	require.NoError(t, err)
	defer sp.Close()

	batch := make([]*telemetryv1.TelemetryReading, 20)
	for i := range batch {
		batch[i] = &telemetryv1.TelemetryReading{
			DeviceId:    "large-device-name-string-for-buffer-overflow-test",
			MetricName:  "pressure_psi_sensor_channel_alpha",
			Value:       123456.789,
			TimestampNs: time.Now().UnixNano(),
		}
	}

	// Should fail with ErrBufferFull when capacity exceeded
	err = sp.Write(batch)
	assert.ErrorIs(t, err, spooler.ErrBufferFull)
}

