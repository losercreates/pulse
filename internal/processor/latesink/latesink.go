package latesink

import (
	"context"
	"fmt"
	"strconv"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// KafkaProducer is an abstraction over Franz-go client Produce for testability.
type KafkaProducer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// LateDataSink dispatches excessively late events to the dead-letter topic telemetry.late.
// Constraint: lateness_duration_ns: Latency beyond watermark (watermark - timestamp_ns).
type LateDataSink struct {
	producer KafkaProducer
	topic    string
}

// NewLateDataSink initializes a new LateDataSink.
func NewLateDataSink(producer KafkaProducer, topic string) *LateDataSink {
	if topic == "" {
		topic = "telemetry.late"
	}
	return &LateDataSink{
		producer: producer,
		topic:    topic,
	}
}

// PublishLate serializes the late record with metadata headers and dispatches it to Kafka.
func (s *LateDataSink) PublishLate(ctx context.Context, late *telemetryv1.LateTelemetryRecord) error {
	payload, err := proto.Marshal(late)
	if err != nil {
		return fmt.Errorf("failed to marshal late telemetry record: %w", err)
	}

	deviceID := ""
	if late.Reading != nil {
		deviceID = late.Reading.DeviceId
	}

	headers := []kgo.RecordHeader{
		{Key: "x-pulse-partition", Value: []byte(strconv.FormatInt(int64(late.PartitionId), 10))},
		{Key: "x-pulse-watermark-ns", Value: []byte(strconv.FormatInt(late.WatermarkNs, 10))},
		{Key: "x-pulse-lateness-ns", Value: []byte(strconv.FormatInt(late.LatenessNs, 10))},
	}

	record := &kgo.Record{
		Topic:   s.topic,
		Key:     []byte(deviceID),
		Value:   payload,
		Headers: headers,
	}

	if s.producer != nil {
		s.producer.Produce(ctx, record, func(r *kgo.Record, err error) {
			// Asynchronous error callback (can be logged or counted)
		})
	}

	return nil
}
