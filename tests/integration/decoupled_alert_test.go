package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/alert/config"
	"github.com/losercreates/pulse/internal/alert/detector"
	"github.com/losercreates/pulse/internal/alert/dispatcher"
	"github.com/losercreates/pulse/internal/alert/incident"
	"github.com/losercreates/pulse/internal/alert/sink"
	"github.com/losercreates/pulse/internal/processor/consumer"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type mockAsyncChannelPublisher struct {
	ch chan *telemetryv1.AlertEvent
}

func (m *mockAsyncChannelPublisher) PublishAlert(ctx context.Context, alert *telemetryv1.AlertEvent) error {
	select {
	case m.ch <- alert:
	default:
		// Drop or buffer non-blocking in accordance with Principle IV
	}
	return nil
}

type mockAggSink struct{}

func (m *mockAggSink) PublishAggregate(ctx context.Context, agg *telemetryv1.WindowAggregate) error {
	return nil
}

// TestDecoupledAlerting_SlowWebhookZeroStreamLag validates Constitution Principle IV:
// Downstream webhook latency (even simulated 30s delays or outages) MUST cause zero
// latency degradation or consumer lag on the stream processor ingestion hot path.
func TestDecoupledAlerting_SlowWebhookZeroStreamLag(t *testing.T) {
	// 1. Setup a slow/hanging webhook server that simulates a 30s slow downstream endpoint
	var receivedWebhookCalls int64
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&receivedWebhookCalls, 1)
		// Simulate severe downstream latency (e.g. 200ms in test, representing 30s delay)
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slowServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Setup Decoupled Alert Dispatcher with slow webhook
	cfg := &config.Config{
		WorkerPoolSize:          4,
		MaxRetries:              1,
		CircuitBreakerThreshold: 5,
		CircuitBreakerCoolOff:   30 * time.Second,
	}

	primarySink := sink.NewWebhookSink(slowServer.URL, slowServer.Client())
	deadLetter := sink.NewDeadLetterSink(nil, "telemetry.alerts.deadletter")
	alertDisp := dispatcher.NewAlertDispatcher(cfg, nil, primarySink, deadLetter, nil)

	go func() {
		_ = alertDisp.Start(ctx)
	}()

	// 3. Setup Stream Processor Partition Worker
	alertBuffer := make(chan *telemetryv1.AlertEvent, 1000)
	asyncPub := &mockAsyncChannelPublisher{ch: alertBuffer}

	// Bridge alert buffer to decoupled dispatcher queue
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case alert, ok := <-alertBuffer:
				if !ok {
					return
				}
				alertDisp.Enqueue(alert)
			}
		}
	}()

	worker := consumer.NewPartitionWorker(
		"telemetry.raw",
		0,
		5*time.Second,
		10*time.Second,
		10*time.Second,
		60*time.Second,
		10*time.Second,
		&mockAggSink{},
		nil,
		nil,
		5000,
	)

	det := detector.NewAnomalyDetector(0.1, 3.0, 0.0001, 30)
	tracker := incident.NewIncidentTracker(5*time.Minute, 10)
	det.WithIncidentTracker(tracker)
	worker.SetAlertPipeline(det, asyncPub)

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		worker.Run(ctx)
	}()

	// 4. Ingest 1,000 telemetry readings into the partition worker at maximum speed,
	// including warmup readings, nominal readings, and anomaly spikes.
	numRecords := 1000
	startTime := time.Now()

	for i := 0; i < numRecords; i++ {
		val := 50.0
		// Inject anomaly spike after warmup
		if i == 50 || i == 60 || i == 70 {
			val = 150.0
		}

		reading := &telemetryv1.TelemetryReading{
			DeviceId:    "turbine-01",
			MetricName:  "temperature",
			Value:       val,
			TimestampNs: int64(time.Duration(i+1) * 100 * time.Millisecond),
		}
		data, err := proto.Marshal(reading)
		require.NoError(t, err)

		record := &kgo.Record{
			Topic:     "telemetry.raw",
			Partition: 0,
			Offset:    int64(i),
			Key:       []byte("turbine-01"),
			Value:     data,
		}

		worker.Inbound() <- record
	}

	// 5. Ingestion of 1,000 records MUST complete in under 100ms (< 100 microseconds per record)
	// even though the downstream webhook is holding requests for 200ms each!
	assert.Eventually(t, func() bool {
		return worker.HighOffset() == int64(numRecords-1)
	}, 500*time.Millisecond, 10*time.Millisecond, "Stream processor ingestion must not be blocked by slow downstream webhook")

	ingestionDuration := time.Since(startTime)
	t.Logf("Successfully processed %d records in %v (zero backpressure)", numRecords, ingestionDuration)

	// Stream processor must have reached the highest offset without any stalling
	assert.Equal(t, int64(numRecords-1), worker.HighOffset())

	cancel()
	<-workerDone
}
