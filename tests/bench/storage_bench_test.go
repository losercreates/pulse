package bench_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/storage/engine"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// BenchmarkStorageEngine_BatchUpsert measures the batch write throughput of the
// sharded memory storage engine under composite key upsert operations. Target: >= 100,000 records/sec.
func BenchmarkStorageEngine_BatchUpsert(b *testing.B) {
	eng := engine.NewShardedMemoryEngine(16)
	defer eng.Close()

	ctx := context.Background()
	batchSize := 100
	batches := make([][]*telemetryv1.StoredAggregate, 100)
	baseTime := time.Now().UnixNano()

	for i := 0; i < 100; i++ {
		batch := make([]*telemetryv1.StoredAggregate, batchSize)
		for j := 0; j < batchSize; j++ {
			devID := fmt.Sprintf("turbine-%04d", (i*batchSize+j)%500)
			start := baseTime + int64((i*batchSize+j)*10_000_000_000)
			batch[j] = &telemetryv1.StoredAggregate{
				DeviceId:      devID,
				MetricName:    "vibration",
				WindowStartNs: start,
				WindowEndNs:   start + 10_000_000_000,
				Count:         100,
				Sum:           500.0,
				Min:           4.5,
				Max:           5.5,
				Mean:          5.0,
			}
		}
		batches[i] = batch
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := batches[i%100]
		_, _, _ = eng.PutAggregates(ctx, batch)
	}

	totalRecords := float64(b.N * batchSize)
	b.ReportMetric(totalRecords/b.Elapsed().Seconds(), "records/sec")
}

// BenchmarkStorageEngine_QueryTimeSeries measures historical time-range query latency
// over indexed aggregate intervals. Target: < 1ms per 1,000-point scan.
func BenchmarkStorageEngine_QueryTimeSeries(b *testing.B) {
	eng := engine.NewShardedMemoryEngine(16)
	defer eng.Close()

	ctx := context.Background()
	baseTime := int64(1_700_000_000_000_000_000)
	numPoints := 5000

	batch := make([]*telemetryv1.StoredAggregate, numPoints)
	for i := 0; i < numPoints; i++ {
		start := baseTime + int64(i*10_000_000_000)
		batch[i] = &telemetryv1.StoredAggregate{
			DeviceId:      "turbine-0001",
			MetricName:    "vibration",
			WindowStartNs: start,
			WindowEndNs:   start + 10_000_000_000,
			Count:         100,
			Sum:           500.0,
			Min:           4.5,
			Max:           5.5,
			Mean:          5.0,
		}
	}
	_, _, _ = eng.PutAggregates(ctx, batch)

	queryStart := baseTime
	queryEnd := baseTime + int64(1000*10_000_000_000) // 1,000 windows

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := eng.QueryTimeSeries(ctx, "turbine-0001", "vibration", queryStart, queryEnd, 1000)
		if err != nil || len(res) == 0 {
			b.Fatalf("query failed: err=%v len=%d", err, len(res))
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkStorageEngine_QueryRollup measures downsampled multi-bucket rollup computation
// over raw 10-second window aggregates. Target: < 5ms per 360-window rollup.
func BenchmarkStorageEngine_QueryRollup(b *testing.B) {
	eng := engine.NewShardedMemoryEngine(16)
	defer eng.Close()

	ctx := context.Background()
	baseTime := int64(1_700_000_000_000_000_000)
	numWindows := 360 // 1 hour of 10s windows

	batch := make([]*telemetryv1.StoredAggregate, numWindows)
	for i := 0; i < numWindows; i++ {
		start := baseTime + int64(i*10_000_000_000)
		batch[i] = &telemetryv1.StoredAggregate{
			DeviceId:      "turbine-0001",
			MetricName:    "vibration",
			WindowStartNs: start,
			WindowEndNs:   start + 10_000_000_000,
			Count:         100,
			Sum:           500.0 + float64(i%10),
			Min:           4.0,
			Max:           6.0,
			Mean:          5.0,
		}
	}
	_, _, _ = eng.PutAggregates(ctx, batch)

	stepNs := int64(60_000_000_000) // 1 minute rollup buckets (60 buckets)
	queryStart := baseTime
	queryEnd := baseTime + int64(numWindows*10_000_000_000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := eng.QueryRollup(ctx, "turbine-0001", "vibration", queryStart, queryEnd, stepNs)
		if err != nil || len(res) == 0 {
			b.Fatalf("rollup failed: err=%v len=%d", err, len(res))
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "rollups/sec")
}
