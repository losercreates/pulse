package validator_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/gateway/validator"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidator_ValidateReading(t *testing.T) {
	v := validator.New(5*time.Minute, 24*time.Hour)
	now := time.Now()

	t.Run("Valid Reading", func(t *testing.T) {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "dev-001",
			TimestampNs: now.UnixNano(),
			MetricName:  "temperature_celsius",
			Value:       45.2,
			Unit:        "celsius",
		}
		err := v.ValidateReading(reading, now)
		assert.Nil(t, err)
	})

	t.Run("Missing Device ID", func(t *testing.T) {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "",
			TimestampNs: now.UnixNano(),
			MetricName:  "temperature_celsius",
			Value:       45.2,
		}
		err := v.ValidateReading(reading, now)
		require.NotNil(t, err)
		assert.Equal(t, validator.ErrCodeMissingDeviceID, err.ErrorCode)
		assert.Equal(t, "device_id", err.Field)
	})

	t.Run("Device ID Exceeds 128 Chars", func(t *testing.T) {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    strings.Repeat("a", 129),
			TimestampNs: now.UnixNano(),
			MetricName:  "temperature_celsius",
			Value:       45.2,
		}
		err := v.ValidateReading(reading, now)
		require.NotNil(t, err)
		assert.Equal(t, validator.ErrCodeInvalidDeviceID, err.ErrorCode)
	})

	t.Run("Missing Metric Name", func(t *testing.T) {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "dev-001",
			TimestampNs: now.UnixNano(),
			MetricName:  "",
			Value:       45.2,
		}
		err := v.ValidateReading(reading, now)
		require.NotNil(t, err)
		assert.Equal(t, validator.ErrCodeMissingMetricName, err.ErrorCode)
	})

	t.Run("Metric Name Exceeds 128 Chars", func(t *testing.T) {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "dev-001",
			TimestampNs: now.UnixNano(),
			MetricName:  strings.Repeat("b", 129),
			Value:       45.2,
		}
		err := v.ValidateReading(reading, now)
		require.NotNil(t, err)
		assert.Equal(t, validator.ErrCodeInvalidMetricName, err.ErrorCode)
	})

	t.Run("NaN Value Rejected", func(t *testing.T) {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "dev-001",
			TimestampNs: now.UnixNano(),
			MetricName:  "temp",
			Value:       math.NaN(),
		}
		err := v.ValidateReading(reading, now)
		require.NotNil(t, err)
		assert.Equal(t, validator.ErrCodeInvalidNumericValue, err.ErrorCode)
	})

	t.Run("Infinity Value Rejected", func(t *testing.T) {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "dev-001",
			TimestampNs: now.UnixNano(),
			MetricName:  "temp",
			Value:       math.Inf(1),
		}
		err := v.ValidateReading(reading, now)
		require.NotNil(t, err)
		assert.Equal(t, validator.ErrCodeInvalidNumericValue, err.ErrorCode)
	})

	t.Run("Future Timestamp Exceeds 5 Minutes", func(t *testing.T) {
		futureTime := now.Add(6 * time.Minute)
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "dev-001",
			TimestampNs: futureTime.UnixNano(),
			MetricName:  "temp",
			Value:       25.0,
		}
		err := v.ValidateReading(reading, now)
		require.NotNil(t, err)
		assert.Equal(t, validator.ErrCodeClockSkewOutOfBounds, err.ErrorCode)
	})

	t.Run("Past Timestamp Exceeds 24 Hours", func(t *testing.T) {
		pastTime := now.Add(-25 * time.Hour)
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "dev-001",
			TimestampNs: pastTime.UnixNano(),
			MetricName:  "temp",
			Value:       25.0,
		}
		err := v.ValidateReading(reading, now)
		require.NotNil(t, err)
		assert.Equal(t, validator.ErrCodeClockSkewOutOfBounds, err.ErrorCode)
	})
}

func TestValidator_ValidateBatch(t *testing.T) {
	v := validator.New(5*time.Minute, 24*time.Hour)
	now := time.Now()

	t.Run("Empty Batch Rejected", func(t *testing.T) {
		batch := &telemetryv1.TelemetryBatch{Readings: nil}
		valid, errs := v.ValidateBatch(batch, now)
		assert.Nil(t, valid)
		assert.NotEmpty(t, errs)
	})

	t.Run("Batch Over 1000 Rejected", func(t *testing.T) {
		readings := make([]*telemetryv1.TelemetryReading, 1001)
		for i := range readings {
			readings[i] = &telemetryv1.TelemetryReading{
				DeviceId:    "dev-01",
				TimestampNs: now.UnixNano(),
				MetricName:  "m",
				Value:       10,
			}
		}
		batch := &telemetryv1.TelemetryBatch{Readings: readings}
		valid, errs := v.ValidateBatch(batch, now)
		assert.Nil(t, valid)
		require.Len(t, errs, 1)
		assert.Equal(t, validator.ErrCodeBatchTooLarge, errs[0].ErrorCode)
	})

	t.Run("Mixed Valid and Invalid Items", func(t *testing.T) {
		batch := &telemetryv1.TelemetryBatch{
			Readings: []*telemetryv1.TelemetryReading{
				{
					DeviceId:    "dev-01",
					TimestampNs: now.UnixNano(),
					MetricName:  "temp",
					Value:       22.5,
				},
				{
					DeviceId:    "", // invalid
					TimestampNs: now.UnixNano(),
					MetricName:  "temp",
					Value:       22.5,
				},
			},
		}
		valid, errs := v.ValidateBatch(batch, now)
		assert.Len(t, valid, 1)
		assert.Len(t, errs, 1)
		assert.Equal(t, "dev-01", valid[0].DeviceId)
		assert.Equal(t, validator.ErrCodeMissingDeviceID, errs[0].ErrorCode)
	})
}
