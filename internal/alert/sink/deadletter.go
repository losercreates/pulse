package sink

import (
	"context"
	"fmt"
	"sync"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// DeadLetterSink routes unroutable or permanently failed alerts to a dead-letter topic.
type DeadLetterSink struct {
	client *kgo.Client
	topic  string

	// Fallback in-memory storage for test/standalone environments without Redpanda
	mu       sync.Mutex
	fallback []*telemetryv1.AlertEvent
}

// NewDeadLetterSink creates a DeadLetterSink backed by topic.
func NewDeadLetterSink(client *kgo.Client, topic string) *DeadLetterSink {
	if topic == "" {
		topic = "telemetry.alerts.deadletter"
	}
	return &DeadLetterSink{
		client: client,
		topic:  topic,
	}
}

// Name returns the sink identifier.
func (s *DeadLetterSink) Name() string {
	return "deadletter"
}

// Deliver stores or publishes the failed alert event.
func (s *DeadLetterSink) Deliver(ctx context.Context, alert *telemetryv1.AlertEvent) error {
	if s.client != nil {
		data, err := proto.Marshal(alert)
		if err != nil {
			return fmt.Errorf("failed to marshal deadletter alert: %w", err)
		}

		record := &kgo.Record{
			Topic: s.topic,
			Key:   []byte(alert.DeviceId),
			Value: data,
		}

		// Asynchronous produce
		s.client.Produce(ctx, record, func(r *kgo.Record, err error) {
			// Delivery callback
		})
		return nil
	}

	// Fallback in-memory capture
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fallback = append(s.fallback, alert)
	return nil
}

// CapturedCount returns the number of captured deadletter alerts in-memory.
func (s *DeadLetterSink) CapturedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.fallback)
}

// GetCaptured returns a copy of captured deadletter alerts.
func (s *DeadLetterSink) GetCaptured() []*telemetryv1.AlertEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]*telemetryv1.AlertEvent, len(s.fallback))
	copy(copied, s.fallback)
	return copied
}
