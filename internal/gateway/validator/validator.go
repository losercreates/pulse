package validator

import (
	"fmt"
	"math"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// Standard error codes for telemetry validation.
const (
	ErrCodeMissingDeviceID      = "ERR_MISSING_DEVICE_ID"
	ErrCodeInvalidDeviceID      = "ERR_INVALID_DEVICE_ID"
	ErrCodeMissingMetricName    = "ERR_MISSING_METRIC_NAME"
	ErrCodeInvalidMetricName    = "ERR_INVALID_METRIC_NAME"
	ErrCodeClockSkewOutOfBounds = "ERR_CLOCK_SKEW_OUT_OF_BOUNDS"
	ErrCodeInvalidNumericValue  = "ERR_INVALID_NUMERIC_VALUE"
	ErrCodeBatchTooLarge        = "ERR_BATCH_TOO_LARGE"
)

// Validator validates incoming telemetry payloads against schema and temporal bounds.
type Validator struct {
	maxFutureSkew time.Duration
	maxPastSkew   time.Duration
}

// New creates a new Validator with configurable clock skew boundaries.
func New(maxFutureSkew, maxPastSkew time.Duration) *Validator {
	if maxFutureSkew <= 0 {
		maxFutureSkew = 5 * time.Minute
	}
	if maxPastSkew <= 0 {
		maxPastSkew = 24 * time.Hour
	}
	return &Validator{
		maxFutureSkew: maxFutureSkew,
		maxPastSkew:   maxPastSkew,
	}
}

// ValidateReading validates an individual TelemetryReading.
func (v *Validator) ValidateReading(reading *telemetryv1.TelemetryReading, now time.Time) *telemetryv1.ValidationError {
	if reading == nil {
		return &telemetryv1.ValidationError{
			Field:     "reading",
			ErrorCode: ErrCodeInvalidDeviceID,
			Message:   "Telemetry reading cannot be nil",
		}
	}

	// 1. Device ID validation: 1 to 128 chars
	devIDLen := len(reading.DeviceId)
	if devIDLen == 0 {
		return &telemetryv1.ValidationError{
			Field:     "device_id",
			ErrorCode: ErrCodeMissingDeviceID,
			Message:   "Device ID must not be empty",
		}
	}
	if devIDLen > 128 {
		return &telemetryv1.ValidationError{
			Field:     "device_id",
			ErrorCode: ErrCodeInvalidDeviceID,
			Message:   fmt.Sprintf("Device ID length %d exceeds 128 characters", devIDLen),
		}
	}

	// 2. Metric Name validation: 1 to 128 chars
	metricLen := len(reading.MetricName)
	if metricLen == 0 {
		return &telemetryv1.ValidationError{
			Field:     "metric_name",
			ErrorCode: ErrCodeMissingMetricName,
			Message:   "Metric name must not be empty",
		}
	}
	if metricLen > 128 {
		return &telemetryv1.ValidationError{
			Field:     "metric_name",
			ErrorCode: ErrCodeInvalidMetricName,
			Message:   fmt.Sprintf("Metric name length %d exceeds 128 characters", metricLen),
		}
	}

	// 3. Numeric value validation: must be finite IEEE 754 float
	if math.IsNaN(reading.Value) || math.IsInf(reading.Value, 0) {
		return &telemetryv1.ValidationError{
			Field:     "value",
			ErrorCode: ErrCodeInvalidNumericValue,
			Message:   "Metric value must be a finite numeric double (NaN and Inf are prohibited)",
		}
	}

	// 4. Timestamp clock skew validation
	readingTime := time.Unix(0, reading.TimestampNs)
	futureBound := now.Add(v.maxFutureSkew)
	pastBound := now.Add(-v.maxPastSkew)

	if readingTime.After(futureBound) {
		return &telemetryv1.ValidationError{
			Field:     "timestamp_ns",
			ErrorCode: ErrCodeClockSkewOutOfBounds,
			Message:   fmt.Sprintf("Timestamp is more than %s in the future", v.maxFutureSkew),
		}
	}

	if readingTime.Before(pastBound) {
		return &telemetryv1.ValidationError{
			Field:     "timestamp_ns",
			ErrorCode: ErrCodeClockSkewOutOfBounds,
			Message:   fmt.Sprintf("Timestamp is older than %s in the past", v.maxPastSkew),
		}
	}

	return nil
}

// ValidateBatch validates batch constraints and individual readings.
func (v *Validator) ValidateBatch(batch *telemetryv1.TelemetryBatch, now time.Time) ([]*telemetryv1.TelemetryReading, []*telemetryv1.ValidationError) {
	if batch == nil || len(batch.Readings) == 0 {
		return nil, []*telemetryv1.ValidationError{
			{
				Field:     "readings",
				ErrorCode: "ERR_EMPTY_BATCH",
				Message:   "Telemetry batch must contain at least 1 reading",
			},
		}
	}

	if len(batch.Readings) > 1000 {
		return nil, []*telemetryv1.ValidationError{
			{
				Field:     "readings",
				ErrorCode: ErrCodeBatchTooLarge,
				Message:   fmt.Sprintf("Batch size %d exceeds maximum allowed limit of 1000", len(batch.Readings)),
			},
		}
	}

	var validReadings []*telemetryv1.TelemetryReading
	var validationErrors []*telemetryv1.ValidationError

	for _, r := range batch.Readings {
		if err := v.ValidateReading(r, now); err != nil {
			validationErrors = append(validationErrors, err)
		} else {
			validReadings = append(validReadings, r)
		}
	}

	return validReadings, validationErrors
}
