package bench_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/gateway/service"
	"github.com/losercreates/pulse/internal/gateway/validator"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

type benchmarkNoopProducer struct {
	produced atomic.Int64
}

func (p *benchmarkNoopProducer) Publish(ctx context.Context, reading *telemetryv1.TelemetryReading) error {
	p.produced.Add(1)
	return nil
}

func (p *benchmarkNoopProducer) PublishBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	p.produced.Add(int64(len(readings)))
	return len(readings), nil
}

func (p *benchmarkNoopProducer) Ping(ctx context.Context) error {
	return nil
}

func (p *benchmarkNoopProducer) Close() {}

func BenchmarkIngestionThroughput_Single(b *testing.B) {
	val := validator.New(5*time.Minute, 24*time.Hour)
	prod := &benchmarkNoopProducer{}
	svc := service.NewIngestionService(val, prod, nil)

	ctx := context.Background()
	now := time.Now()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			reading := &telemetryv1.TelemetryReading{
				DeviceId:    fmt.Sprintf("dev-%d", i%1000),
				TimestampNs: now.UnixNano(),
				MetricName:  "vibration_rms",
				Value:       14.28,
				Unit:        "mm_s",
			}
			_, _ = svc.ProcessReading(ctx, reading, "bench")
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
}

func BenchmarkIngestionThroughput_Batch100(b *testing.B) {
	val := validator.New(5*time.Minute, 24*time.Hour)
	prod := &benchmarkNoopProducer{}
	svc := service.NewIngestionService(val, prod, nil)

	ctx := context.Background()
	now := time.Now()

	batch := &telemetryv1.TelemetryBatch{
		Readings: make([]*telemetryv1.TelemetryReading, 100),
	}
	for i := 0; i < 100; i++ {
		batch.Readings[i] = &telemetryv1.TelemetryReading{
			DeviceId:    fmt.Sprintf("dev-%d", i),
			TimestampNs: now.UnixNano(),
			MetricName:  "temperature_celsius",
			Value:       62.5,
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = svc.ProcessBatch(ctx, batch, "bench")
		}
	})

	totalEvents := float64(b.N * 100)
	b.ReportMetric(totalEvents/b.Elapsed().Seconds(), "events/sec")
}
