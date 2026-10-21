package consumer

import (
	"context"
	"sync"
	"testing"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type mockAggregatePublisher struct {
	mu         sync.Mutex
	aggregates []*telemetryv1.WindowAggregate
}

func (m *mockAggregatePublisher) PublishAggregate(ctx context.Context, agg *telemetryv1.WindowAggregate) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.aggregates = append(m.aggregates, agg)
	return nil
}

func (m *mockAggregatePublisher) getAggregates() []*telemetryv1.WindowAggregate {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]*telemetryv1.WindowAggregate, len(m.aggregates))
	copy(copied, m.aggregates)
	return copied
}

type mockLateSink struct {
	mu          sync.Mutex
	lateRecords []*telemetryv1.LateTelemetryRecord
}

func (m *mockLateSink) PublishLate(ctx context.Context, late *telemetryv1.LateTelemetryRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lateRecords = append(m.lateRecords, late)
	return nil
}

func (m *mockLateSink) getLate() []*telemetryv1.LateTelemetryRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]*telemetryv1.LateTelemetryRecord, len(m.lateRecords))
	copy(copied, m.lateRecords)
	return copied
}

func makeRecord(t *testing.T, partition int32, offset int64, devID, metric string, val float64, tsNs int64) *kgo.Record {
	reading := &telemetryv1.TelemetryReading{
		DeviceId:    devID,
		MetricName:  metric,
		Value:       val,
		TimestampNs: tsNs,
	}
	bytes, err := proto.Marshal(reading)
	require.NoError(t, err)

	return &kgo.Record{
		Topic:     "telemetry.raw",
		Partition: partition,
		Offset:    offset,
		Key:       []byte(devID),
		Value:     bytes,
	}
}

func TestPartitionWorker_IngestionAndWatermark(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockPub := &mockAggregatePublisher{}
	mockLate := &mockLateSink{}

	worker := NewPartitionWorker(
		"telemetry.raw",
		0,
		5*time.Second,  // allowed lateness
		10*time.Second, // idle timeout
		10*time.Second, // tumbling window size
		60*time.Second, // sliding window size
		10*time.Second, // sliding slide
		mockPub,
		mockLate,
		nil,
		100,
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.Run(ctx)
	}()

	// Event 1: at 2s (in window [0, 10s))
	worker.Inbound() <- makeRecord(t, 0, 1, "turbine-1", "temperature", 50.0, int64(2*time.Second))

	// Event 2: at 8s (in window [0, 10s))
	worker.Inbound() <- makeRecord(t, 0, 2, "turbine-1", "temperature", 60.0, int64(8*time.Second))

	// Event 3: at 4s (out of order, but in window [0, 10s))
	worker.Inbound() <- makeRecord(t, 0, 3, "turbine-1", "temperature", 40.0, int64(4*time.Second))

	// Event 4: at 15s (watermark = 15s - 5s = 10s -> closes window [0, 10s))
	worker.Inbound() <- makeRecord(t, 0, 4, "turbine-1", "temperature", 70.0, int64(15*time.Second))

	// Wait for processing
	assert.Eventually(t, func() bool {
		aggs := mockPub.getAggregates()
		for _, a := range aggs {
			if a.WindowStartNs == 0 && a.WindowEndNs == int64(10*time.Second) {
				return a.Count == 3 && a.Sum == 150.0
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)

	// Now send a late event: at 8s (watermark is at 10s) -> should be rejected and sent to LateSink!
	worker.Inbound() <- makeRecord(t, 0, 5, "turbine-1", "temperature", 99.0, int64(8*time.Second))

	assert.Eventually(t, func() bool {
		lates := mockLate.getLate()
		return len(lates) == 1 && lates[0].Reading.DeviceId == "turbine-1"
	}, 2*time.Second, 50*time.Millisecond)

	cancel()
	<-done

	assert.Equal(t, int64(5), worker.HighOffset())
}
