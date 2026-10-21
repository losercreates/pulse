package bench_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/processor/consumer"
	"github.com/losercreates/pulse/internal/processor/window"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type noopPublisher struct {
	count atomic.Int64
}

func (p *noopPublisher) PublishAggregate(ctx context.Context, agg *telemetryv1.WindowAggregate) error {
	p.count.Add(1)
	return nil
}

type noopLateSink struct {
	count atomic.Int64
}

func (s *noopLateSink) PublishLate(ctx context.Context, late *telemetryv1.LateTelemetryRecord) error {
	s.count.Add(1)
	return nil
}

// BenchmarkWindowManager evaluates single-core window accumulation and boundary assignment.
func BenchmarkWindowManager(b *testing.B) {
	calc := window.NewBoundaryCalculator(10*time.Second, 60*time.Second, 10*time.Second)
	mgr := window.NewWindowManager(0, calc)

	baseTime := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts := baseTime + int64(i)*int64(time.Millisecond)
		devID := fmt.Sprintf("turbine-%d", i%100)
		mgr.ProcessReading(ts, devID, "vibration", 42.5)

		if i%10000 == 0 {
			_ = mgr.AdvanceWatermark(ts - int64(5*time.Second))
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
}

// BenchmarkPartitionWorkers_12Partitions evaluates concurrent ingestion throughput
// across 12 partition worker goroutines, targeting >= 50,000 events/sec.
func BenchmarkPartitionWorkers_12Partitions(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numPartitions := 12
	workers := make([]*consumer.PartitionWorker, numPartitions)
	pub := &noopPublisher{}
	late := &noopLateSink{}

	var wg sync.WaitGroup
	for p := 0; p < numPartitions; p++ {
		w := consumer.NewPartitionWorker(
			"telemetry.raw",
			int32(p),
			5*time.Second,
			10*time.Second,
			10*time.Second,
			60*time.Second,
			10*time.Second,
			pub,
			late,
			nil,
			100000,
		)
		workers[p] = w

		wg.Add(1)
		go func(worker *consumer.PartitionWorker) {
			defer wg.Done()
			worker.Run(ctx)
		}(w)
	}

	// Pre-generate serialized records
	numPrebaked := 1000
	prebakedRecords := make([]*kgo.Record, numPrebaked)
	now := time.Now().UnixNano()
	for i := 0; i < numPrebaked; i++ {
		reading := &telemetryv1.TelemetryReading{
			DeviceId:    fmt.Sprintf("device-%d", i),
			MetricName:  "temperature",
			Value:       75.5,
			TimestampNs: now + int64(i)*int64(10*time.Millisecond),
		}
		bytes, _ := proto.Marshal(reading)
		prebakedRecords[i] = &kgo.Record{
			Topic:     "telemetry.raw",
			Partition: int32(i % numPartitions),
			Offset:    int64(i),
			Key:       []byte(reading.DeviceId),
			Value:     bytes,
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			idx++
			rec := prebakedRecords[idx%numPrebaked]
			p := rec.Partition
			workers[p].Inbound() <- rec
		}
	})

	totalEvents := float64(b.N)
	b.ReportMetric(totalEvents/b.Elapsed().Seconds(), "events/sec")

	cancel()
	wg.Wait()
}
