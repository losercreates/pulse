package service

import (
	"context"
	"time"

	"github.com/losercreates/pulse/internal/gateway/metrics"
	"github.com/losercreates/pulse/internal/gateway/producer"
	"github.com/losercreates/pulse/internal/gateway/validator"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// IngestionService coordinates validation, metrics recording, and broker publication.
type IngestionService struct {
	validator *validator.Validator
	producer  producer.Producer
	metrics   *metrics.Metrics
}

// NewIngestionService creates a new IngestionService instance.
func NewIngestionService(val *validator.Validator, prod producer.Producer, m *metrics.Metrics) *IngestionService {
	return &IngestionService{
		validator: val,
		producer:  prod,
		metrics:   m,
	}
}

// ProcessReading validates a single telemetry reading and dispatches it to the broker.
func (s *IngestionService) ProcessReading(ctx context.Context, reading *telemetryv1.TelemetryReading, protocol string) (*telemetryv1.IngestionReceipt, error) {
	start := time.Now()
	now := start

	if s.metrics != nil {
		defer func() {
			s.metrics.IngestionLatencySeconds.WithLabelValues(protocol).Observe(time.Since(start).Seconds())
		}()
	}

	if valErr := s.validator.ValidateReading(reading, now); valErr != nil {
		if s.metrics != nil {
			s.metrics.IngestedRecordsTotal.WithLabelValues(protocol, "rejected").Inc()
			if valErr.ErrorCode == validator.ErrCodeClockSkewOutOfBounds {
				s.metrics.ClockSkewRejectionsTotal.Inc()
			} else {
				s.metrics.MalformedPayloadsTotal.Inc()
			}
		}

		return &telemetryv1.IngestionReceipt{
			Status:         telemetryv1.IngestionReceipt_STATUS_REJECTED,
			ProcessedCount: 0,
			RejectedCount:  1,
			ServerTimeNs:   now.UnixNano(),
			ErrorDetails:   []*telemetryv1.ValidationError{valErr},
		}, nil
	}

	if err := s.producer.Publish(ctx, reading); err != nil {
		if s.metrics != nil {
			s.metrics.IngestedRecordsTotal.WithLabelValues(protocol, "error").Inc()
		}
		return nil, err
	}

	if s.metrics != nil {
		s.metrics.IngestedRecordsTotal.WithLabelValues(protocol, "success").Inc()
	}

	return &telemetryv1.IngestionReceipt{
		Status:         telemetryv1.IngestionReceipt_STATUS_ACCEPTED,
		ProcessedCount: 1,
		RejectedCount:  0,
		ServerTimeNs:   now.UnixNano(),
	}, nil
}

// ProcessBatch validates a batch of telemetry readings and dispatches valid ones to the broker.
func (s *IngestionService) ProcessBatch(ctx context.Context, batch *telemetryv1.TelemetryBatch, protocol string) (*telemetryv1.IngestionReceipt, error) {
	start := time.Now()
	now := start

	if s.metrics != nil {
		defer func() {
			s.metrics.IngestionLatencySeconds.WithLabelValues(protocol).Observe(time.Since(start).Seconds())
		}()
	}

	validReadings, valErrs := s.validator.ValidateBatch(batch, now)
	rejectedCount := int32(len(valErrs))

	if s.metrics != nil {
		s.metrics.IngestedRecordsTotal.WithLabelValues(protocol, "rejected").Add(float64(rejectedCount))
	}

	if len(validReadings) == 0 {
		return &telemetryv1.IngestionReceipt{
			Status:         telemetryv1.IngestionReceipt_STATUS_REJECTED,
			ProcessedCount: 0,
			RejectedCount:  rejectedCount,
			ServerTimeNs:   now.UnixNano(),
			ErrorDetails:   valErrs,
		}, nil
	}

	processedCount, err := s.producer.PublishBatch(ctx, validReadings)
	if s.metrics != nil {
		s.metrics.IngestedRecordsTotal.WithLabelValues(protocol, "success").Add(float64(processedCount))
	}

	status := telemetryv1.IngestionReceipt_STATUS_ACCEPTED
	if rejectedCount > 0 {
		status = telemetryv1.IngestionReceipt_STATUS_PARTIAL_SUCCESS
	}

	return &telemetryv1.IngestionReceipt{
		Status:         status,
		ProcessedCount: int32(processedCount),
		RejectedCount:  rejectedCount,
		ServerTimeNs:   now.UnixNano(),
		ErrorDetails:   valErrs,
	}, err
}
