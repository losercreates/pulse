package detector

import (
	"context"
	"fmt"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// AlertPublisher interface abstracts alert event emission.
type AlertPublisher interface {
	PublishAlert(ctx context.Context, alert *telemetryv1.AlertEvent) error
}

// KafkaAlertPublisher produces detected AlertEvent records to Redpanda topic telemetry.alerts.
type KafkaAlertPublisher struct {
	client *kgo.Client
	topic  string
}

// NewKafkaAlertPublisher initializes an AlertPublisher backed by Franz-go.
func NewKafkaAlertPublisher(client *kgo.Client, topic string) *KafkaAlertPublisher {
	if topic == "" {
		topic = "telemetry.alerts"
	}
	return &KafkaAlertPublisher{
		client: client,
		topic:  topic,
	}
}

// PublishAlert serializes the AlertEvent and asynchronously emits it to Redpanda keyed by device_id.
func (p *KafkaAlertPublisher) PublishAlert(ctx context.Context, alert *telemetryv1.AlertEvent) error {
	payload, err := proto.Marshal(alert)
	if err != nil {
		return fmt.Errorf("failed to marshal alert event: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(alert.DeviceId),
		Value: payload,
	}

	if p.client != nil {
		p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
			// Asynchronous error callback
		})
	}
	return nil
}
