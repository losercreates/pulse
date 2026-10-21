package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/gateway/service"
	"github.com/losercreates/pulse/internal/gateway/validator"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockProducer struct {
	published      []*telemetryv1.TelemetryReading
	publishBatchFn func(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error)
}

func (m *mockProducer) Publish(ctx context.Context, reading *telemetryv1.TelemetryReading) error {
	m.published = append(m.published, reading)
	return nil
}

func (m *mockProducer) PublishBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	if m.publishBatchFn != nil {
		return m.publishBatchFn(ctx, readings)
	}
	m.published = append(m.published, readings...)
	return len(readings), nil
}

func (m *mockProducer) Ping(ctx context.Context) error {
	return nil
}

func (m *mockProducer) Close() {}

func TestIngestionService_ProcessReading(t *testing.T) {
	val := validator.New(5*time.Minute, 24*time.Hour)
	prod := &mockProducer{}
	svc := service.NewIngestionService(val, prod, nil)
	now := time.Now()

	t.Run("Valid Reading Processed", func(t *testing.T) {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "turbine-01",
			TimestampNs: now.UnixNano(),
			MetricName:  "temp",
			Value:       55.4,
		}

		receipt, err := svc.ProcessReading(context.Background(), reading, "test")
		require.NoError(t, err)
		assert.Equal(t, telemetryv1.IngestionReceipt_STATUS_ACCEPTED, receipt.Status)
		assert.Equal(t, int32(1), receipt.ProcessedCount)
		assert.Equal(t, int32(0), receipt.RejectedCount)
		assert.Len(t, prod.published, 1)
	})

	t.Run("Invalid Reading Rejected", func(t *testing.T) {
		prod.published = nil
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "", // invalid
			TimestampNs: now.UnixNano(),
			MetricName:  "temp",
			Value:       55.4,
		}

		receipt, err := svc.ProcessReading(context.Background(), reading, "test")
		require.NoError(t, err)
		assert.Equal(t, telemetryv1.IngestionReceipt_STATUS_REJECTED, receipt.Status)
		assert.Equal(t, int32(0), receipt.ProcessedCount)
		assert.Equal(t, int32(1), receipt.RejectedCount)
		assert.Empty(t, prod.published)
	})
}

func TestIngestionService_ProcessBatch(t *testing.T) {
	val := validator.New(5*time.Minute, 24*time.Hour)
	prod := &mockProducer{}
	svc := service.NewIngestionService(val, prod, nil)
	now := time.Now()

	t.Run("Batch Partial Success", func(t *testing.T) {
		prod.published = nil
		batch := &telemetryv1.TelemetryBatch{
			Readings: []*telemetryv1.TelemetryReading{
				{
					DeviceId:    "dev-01",
					TimestampNs: now.UnixNano(),
					MetricName:  "temp",
					Value:       20.0,
				},
				{
					DeviceId:    "", // invalid
					TimestampNs: now.UnixNano(),
					MetricName:  "temp",
					Value:       20.0,
				},
			},
		}

		receipt, err := svc.ProcessBatch(context.Background(), batch, "test")
		require.NoError(t, err)
		assert.Equal(t, telemetryv1.IngestionReceipt_STATUS_PARTIAL_SUCCESS, receipt.Status)
		assert.Equal(t, int32(1), receipt.ProcessedCount)
		assert.Equal(t, int32(1), receipt.RejectedCount)
		assert.Len(t, prod.published, 1)
	})
}
