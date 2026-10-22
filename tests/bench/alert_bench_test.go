package bench_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/alert/config"
	"github.com/losercreates/pulse/internal/alert/detector"
	"github.com/losercreates/pulse/internal/alert/dispatcher"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

type noopAlertSink struct{}

func (s *noopAlertSink) Name() string {
	return "noop"
}

func (s *noopAlertSink) Deliver(ctx context.Context, alert *telemetryv1.AlertEvent) error {
	return nil
}

// BenchmarkStatisticalBaselineUpdate measures the latency of updating the online EWMA mean
// and rolling variance with variance floor guardrails. Target: < 100 ns per reading.
func BenchmarkStatisticalBaselineUpdate(b *testing.B) {
	base := detector.NewStatisticalBaseline("turbine-01", "vibration", 0.05, 0.0001, 30)
	ts := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := 50.0 + float64(i%10)
		base.Update(val, ts+int64(i))
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "updates/sec")
}

// BenchmarkAnomalyDetector_Evaluate measures partition-local baseline lookup,
// online statistical updates, and k-sigma anomaly band classification. Target: < 100 ns per reading.
func BenchmarkAnomalyDetector_Evaluate(b *testing.B) {
	det := detector.NewAnomalyDetector(0.05, 3.0, 0.0001, 30)
	ts := time.Now().UnixNano()

	// Pre-populate 100 series
	for d := 0; d < 100; d++ {
		devID := fmt.Sprintf("turbine-%03d", d)
		for s := 0; s < 35; s++ {
			det.Evaluate(devID, "temperature", 75.0, ts+int64(s))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		devID := fmt.Sprintf("turbine-%03d", i%100)
		val := 75.0 + float64(i%5)
		det.Evaluate(devID, "temperature", val, ts+int64(i))
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "evaluations/sec")
}

// BenchmarkAlertDispatcher_WorkerPool measures decoupled alert delivery throughput
// across an isolated concurrent worker pool. Target: >= 1,000 alerts/sec.
func BenchmarkAlertDispatcher_WorkerPool(b *testing.B) {
	cfg := &config.Config{
		WorkerPoolSize:          10,
		MaxRetries:              1,
		CircuitBreakerThreshold: 5,
		CircuitBreakerCoolOff:   30 * time.Second,
	}

	disp := dispatcher.NewAlertDispatcher(cfg, nil, &noopAlertSink{}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = disp.Start(ctx)
	}()

	alert := &telemetryv1.AlertEvent{
		AlertId:      "alert-bench",
		DeviceId:     "motor-01",
		MetricName:   "vibration",
		TriggerValue: 120.0,
		ZScore:       4.5,
		Severity:     telemetryv1.AlertSeverity_SEVERITY_WARNING,
		Status:       telemetryv1.AlertStatus_STATUS_FIRING,
		TimestampNs:  time.Now().UnixNano(),
		PartitionId:  0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		disp.Enqueue(alert)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "dispatches/sec")
}
