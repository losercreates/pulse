package engine_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/losercreates/pulse/internal/storage/engine"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALFrame_EncodeDecode(t *testing.T) {
	record := &telemetryv1.StoredAggregate{
		DeviceId:      "dev-001",
		MetricName:    "cpu_usage",
		WindowStartNs: 1000,
		WindowEndNs:   2000,
		Count:         10,
		Sum:           500.0,
		Min:           40.0,
		Max:           60.0,
		Mean:          50.0,
	}

	payload, err := engine.MarshalAggregate(record)
	require.NoError(t, err)

	frame := engine.EncodeWALFrame(engine.OpPut, payload)
	require.NotEmpty(t, frame)

	// Decode from bytes buffer
	buf := bytes.NewReader(frame)
	op, decodedPayload, err := engine.DecodeWALFrame(buf)
	require.NoError(t, err)
	assert.Equal(t, engine.OpPut, op)

	decodedRecord, err := engine.UnmarshalAggregate(decodedPayload)
	require.NoError(t, err)
	assert.Equal(t, record.DeviceId, decodedRecord.DeviceId)
	assert.Equal(t, record.MetricName, decodedRecord.MetricName)
	assert.Equal(t, record.WindowStartNs, decodedRecord.WindowStartNs)
	assert.Equal(t, record.WindowEndNs, decodedRecord.WindowEndNs)
	assert.Equal(t, record.Count, decodedRecord.Count)
	assert.Equal(t, record.Mean, decodedRecord.Mean)

	// Next read should return EOF
	_, _, err = engine.DecodeWALFrame(buf)
	assert.Equal(t, io.EOF, err)
}

func TestWALFrame_CorruptedCRC(t *testing.T) {
	record := &telemetryv1.StoredAggregate{
		DeviceId:      "dev-001",
		MetricName:    "temp",
		WindowStartNs: 10,
		WindowEndNs:   20,
	}

	payload, err := engine.MarshalAggregate(record)
	require.NoError(t, err)

	frame := engine.EncodeWALFrame(engine.OpPut, payload)

	// Corrupt the payload (flip a byte at end of frame)
	frame[len(frame)-1] ^= 0xFF

	buf := bytes.NewReader(frame)
	_, _, err = engine.DecodeWALFrame(buf)
	require.Error(t, err)
	assert.ErrorIs(t, err, engine.ErrCorruptWAL)
}

func TestWALFrame_InvalidMagic(t *testing.T) {
	frame := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x01, 0xFF}
	buf := bytes.NewReader(frame)
	_, _, err := engine.DecodeWALFrame(buf)
	require.Error(t, err)
	assert.ErrorIs(t, err, engine.ErrInvalidMagic)
}

func TestWALFrame_TruncatedHeader(t *testing.T) {
	// Only 5 bytes of 13-byte header
	frame := []byte{0x50, 0x55, 0x4C, 0x53, 0x00}
	buf := bytes.NewReader(frame)
	_, _, err := engine.DecodeWALFrame(buf)
	assert.Equal(t, io.EOF, err)
}

func TestWALManager_AppendReplay(t *testing.T) {
	tmpDir := t.TempDir()

	mgr, err := engine.OpenWALManager(tmpDir)
	require.NoError(t, err)
	defer mgr.Close()

	var records []*telemetryv1.StoredAggregate
	for i := 0; i < 5; i++ {
		records = append(records, &telemetryv1.StoredAggregate{
			DeviceId:      "dev-001",
			MetricName:    "cpu_usage",
			WindowStartNs: int64(i * 1000),
			WindowEndNs:   int64((i + 1) * 1000),
			Count:         uint64(i + 1),
			Mean:          42.0,
		})
	}

	err = mgr.AppendBatch(records)
	require.NoError(t, err)

	// Replay records
	var replayed []*telemetryv1.StoredAggregate
	count, err := mgr.Replay(func(record *telemetryv1.StoredAggregate) error {
		replayed = append(replayed, record)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 5, count)
	assert.Len(t, replayed, 5)
	assert.Equal(t, records[0].WindowStartNs, replayed[0].WindowStartNs)
	assert.Equal(t, records[4].WindowEndNs, replayed[4].WindowEndNs)
}

func TestWALManager_RotateAndPurge(t *testing.T) {
	tmpDir := t.TempDir()

	mgr, err := engine.OpenWALManager(tmpDir)
	require.NoError(t, err)
	defer mgr.Close()

	rec1 := &telemetryv1.StoredAggregate{
		DeviceId:      "dev-001",
		MetricName:    "mem_usage",
		WindowStartNs: 1000,
		WindowEndNs:   2000,
	}
	require.NoError(t, mgr.AppendBatch([]*telemetryv1.StoredAggregate{rec1}))

	// Rotate
	require.NoError(t, mgr.Rotate())

	rec2 := &telemetryv1.StoredAggregate{
		DeviceId:      "dev-001",
		MetricName:    "mem_usage",
		WindowStartNs: 2000,
		WindowEndNs:   3000,
	}
	require.NoError(t, mgr.AppendBatch([]*telemetryv1.StoredAggregate{rec2}))

	// Both should be replayed
	var replayed []*telemetryv1.StoredAggregate
	count, err := mgr.Replay(func(record *telemetryv1.StoredAggregate) error {
		replayed = append(replayed, record)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Purge checkpointed
	require.NoError(t, mgr.PurgeCheckpointed())

	// Only rec2 remains after checkpointed is purged
	replayed = nil
	count, err = mgr.Replay(func(record *telemetryv1.StoredAggregate) error {
		replayed = append(replayed, record)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, int64(2000), replayed[0].WindowStartNs)
}

