package dispatcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/alert/config"
	"github.com/losercreates/pulse/internal/alert/metrics"
	"github.com/losercreates/pulse/internal/alert/sink"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

type mockSink struct {
	name        string
	mu          sync.Mutex
	delivered   []*telemetryv1.AlertEvent
	failCount   int32
	alwaysFail  bool
	delayMillis time.Duration
}

func (m *mockSink) Name() string {
	return m.name
}

func (m *mockSink) Deliver(ctx context.Context, alert *telemetryv1.AlertEvent) error {
	if m.delayMillis > 0 {
		time.Sleep(m.delayMillis)
	}

	if m.alwaysFail {
		return errors.New("sink down")
	}

	if atomic.LoadInt32(&m.failCount) > 0 {
		atomic.AddInt32(&m.failCount, -1)
		return errors.New("transient sink error")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.delivered = append(m.delivered, alert)
	return nil
}

func (m *mockSink) getDelivered() []*telemetryv1.AlertEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]*telemetryv1.AlertEvent, len(m.delivered))
	copy(copied, m.delivered)
	return copied
}

func TestAlertDispatcher_SuccessDelivery(t *testing.T) {
	cfg := &config.Config{
		WorkerPoolSize:          4,
		MaxRetries:              2,
		CircuitBreakerThreshold: 5,
		CircuitBreakerCoolOff:   30 * time.Second,
	}

	primary := &mockSink{name: "webhook"}
	deadLetter := sink.NewDeadLetterSink(nil, "telemetry.alerts.deadletter")
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)

	disp := NewAlertDispatcher(cfg, nil, primary, deadLetter, m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = disp.Start(ctx)
	}()

	// Enqueue 10 alerts
	for i := 0; i < 10; i++ {
		alert := &telemetryv1.AlertEvent{
			AlertId:     "alert-001",
			DeviceId:    "pump-01",
			MetricName:  "pressure",
			Severity:    telemetryv1.AlertSeverity_SEVERITY_WARNING,
			Status:      telemetryv1.AlertStatus_STATUS_FIRING,
			TimestampNs: time.Now().UnixNano(),
		}
		disp.Enqueue(alert)
	}

	// Wait for workers to process
	assert.Eventually(t, func() bool {
		return len(primary.getDelivered()) == 10
	}, 2*time.Second, 50*time.Millisecond)

	assert.Equal(t, 0, deadLetter.CapturedCount())
	assert.Equal(t, StateClosed, disp.CircuitBreaker().State())

	cancel()
}

func TestAlertDispatcher_FailuresTripCircuitAndRouteToDeadLetter(t *testing.T) {
	cfg := &config.Config{
		WorkerPoolSize:          2,
		MaxRetries:              1, // Fast fail
		CircuitBreakerThreshold: 3,
		CircuitBreakerCoolOff:   30 * time.Second,
	}

	primary := &mockSink{name: "failing-webhook", alwaysFail: true}
	deadLetter := sink.NewDeadLetterSink(nil, "telemetry.alerts.deadletter")

	disp := NewAlertDispatcher(cfg, nil, primary, deadLetter, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = disp.Start(ctx)
	}()

	// Enqueue 5 alerts to failing sink
	for i := 0; i < 5; i++ {
		alert := &telemetryv1.AlertEvent{
			AlertId:     "alert-failing",
			DeviceId:    "motor-02",
			MetricName:  "temp",
			Severity:    telemetryv1.AlertSeverity_SEVERITY_CRITICAL,
			Status:      telemetryv1.AlertStatus_STATUS_FIRING,
			TimestampNs: time.Now().UnixNano(),
		}
		disp.Enqueue(alert)
	}

	// All 5 should be routed to dead-letter sink!
	assert.Eventually(t, func() bool {
		return deadLetter.CapturedCount() == 5
	}, 2*time.Second, 50*time.Millisecond)

	// Circuit breaker should have tripped to OPEN after 3 failures
	assert.Equal(t, StateOpen, disp.CircuitBreaker().State())
	assert.Equal(t, 0, len(primary.getDelivered()))

	cancel()
}
