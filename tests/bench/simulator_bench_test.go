package bench_test

import (
	"context"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/fleet"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

type noopSender struct{}

func (s *noopSender) SendBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	return len(readings), nil
}

func (s *noopSender) Close() error {
	return nil
}

func BenchmarkSimulator_FleetSignalGeneration(b *testing.B) {
	numDevices := 10000
	f := fleet.NewFleet(numDevices)
	devices := f.Devices()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		now := time.Now().UnixNano()
		elapsed := 1 * time.Second

		for pb.Next() {
			dev := devices[idx%numDevices]
			r := dev.NextReading(now, elapsed, idx)
			if r != nil {
				_ = r.Value
			}
			idx++
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
}

func BenchmarkSimulator_MicroBatchAssembly(b *testing.B) {
	numDevices := 10000
	f := fleet.NewFleet(numDevices)
	partitions := f.WorkerPartitions(16)
	sender := &noopSender{}

	ctx := context.Background()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		workerDevices := partitions[0]
		devCount := len(workerDevices)
		batchSize := 50
		batch := make([]*telemetryv1.TelemetryReading, 0, batchSize)
		idx := 0
		now := time.Now().UnixNano()

		for pb.Next() {
			batch = batch[:0]
			for i := 0; i < batchSize; i++ {
				dev := workerDevices[idx%devCount]
				r := dev.NextReading(now, 1*time.Second, idx)
				if r != nil {
					batch = append(batch, r)
				}
				idx++
			}
			_, _ = sender.SendBatch(ctx, batch)
		}
	})

	b.ReportMetric(float64(b.N*50)/b.Elapsed().Seconds(), "events/sec")
}
