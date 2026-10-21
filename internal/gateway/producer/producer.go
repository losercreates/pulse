package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/losercreates/pulse/internal/gateway/config"
	"github.com/losercreates/pulse/internal/gateway/metrics"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Producer defines the contract for dispatching telemetry readings to the message broker.
type Producer interface {
	Publish(ctx context.Context, reading *telemetryv1.TelemetryReading) error
	PublishBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error)
	Ping(ctx context.Context) error
	Close()
}

// FranzProducer implements Producer using twmb/franz-go.
type FranzProducer struct {
	client  *kgo.Client
	topic   string
	metrics *metrics.Metrics
}

// New creates a new FranzProducer configured with Murmur2 key partitioning.
func New(cfg *config.Config, m *metrics.Metrics) (*FranzProducer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.TopicRaw),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProducerLinger(cfg.ProducerBatchLinger),
		kgo.RecordRetries(3),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize franz-go client: %w", err)
	}

	return &FranzProducer{
		client:  client,
		topic:   cfg.TopicRaw,
		metrics: m,
	}, nil
}

// Publish serializes a single reading to Protobuf and dispatches it with device_id as key.
func (p *FranzProducer) Publish(ctx context.Context, reading *telemetryv1.TelemetryReading) error {
	payload, err := proto.Marshal(reading)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry reading to proto: %w", err)
	}

	record := &kgo.Record{
		Topic:     p.topic,
		Key:       []byte(reading.DeviceId),
		Value:     payload,
		Timestamp: time.Unix(0, reading.TimestampNs),
	}

	start := time.Now()
	res := p.client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce record to Redpanda: %w", err)
	}

	if p.metrics != nil {
		p.metrics.BrokerPublishLatencySeconds.WithLabelValues(p.topic).Observe(time.Since(start).Seconds())
	}

	return nil
}

// PublishBatch serializes and dispatches a batch of readings concurrently.
func (p *FranzProducer) PublishBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	if len(readings) == 0 {
		return 0, nil
	}

	records := make([]*kgo.Record, 0, len(readings))
	for _, reading := range readings {
		payload, err := proto.Marshal(reading)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal reading for device %s: %w", reading.DeviceId, err)
		}

		records = append(records, &kgo.Record{
			Topic:     p.topic,
			Key:       []byte(reading.DeviceId),
			Value:     payload,
			Timestamp: time.Unix(0, reading.TimestampNs),
		})
	}

	start := time.Now()
	results := p.client.ProduceSync(ctx, records...)
	var firstErr error
	successCount := 0

	for _, res := range results {
		if err := res.Err; err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else {
			successCount++
		}
	}

	if p.metrics != nil {
		p.metrics.BrokerPublishLatencySeconds.WithLabelValues(p.topic).Observe(time.Since(start).Seconds())
	}

	if firstErr != nil {
		return successCount, fmt.Errorf("produce batch error (processed %d/%d): %w", successCount, len(readings), firstErr)
	}

	return successCount, nil
}

// Ping checks connectivity to the Redpanda broker cluster.
func (p *FranzProducer) Ping(ctx context.Context) error {
	err := p.client.Ping(ctx)
	if p.metrics != nil {
		if err == nil {
			p.metrics.BrokerConnected.Set(1)
		} else {
			p.metrics.BrokerConnected.Set(0)
		}
	}
	return err
}

// Close flushes buffered records and closes broker connections.
func (p *FranzProducer) Close() {
	p.client.Close()
}
