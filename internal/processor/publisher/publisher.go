package publisher

import (
	"context"
	"fmt"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// AggregatePublisher produces closed window aggregates to Redpanda topic telemetry.aggregates.
type AggregatePublisher struct {
	client *kgo.Client
	topic  string
}

// NewAggregatePublisher initializes a new AggregatePublisher.
func NewAggregatePublisher(client *kgo.Client, topic string) *AggregatePublisher {
	if topic == "" {
		topic = "telemetry.aggregates"
	}
	return &AggregatePublisher{
		client: client,
		topic:  topic,
	}
}

// PublishAggregate serializes the window aggregate and publishes it to Redpanda.
func (p *AggregatePublisher) PublishAggregate(ctx context.Context, agg *telemetryv1.WindowAggregate) error {
	payload, err := proto.Marshal(agg)
	if err != nil {
		return fmt.Errorf("failed to marshal window aggregate: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(agg.DeviceId),
		Value: payload,
	}

	if p.client != nil {
		p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
			// Asynchronous error handler
		})
	}
	return nil
}
