package dispatcher

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/losercreates/pulse/internal/alert/config"
	"github.com/losercreates/pulse/internal/alert/metrics"
	"github.com/losercreates/pulse/internal/alert/sink"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// AlertDispatcher consumes AlertEvent records and orchestrates resilient dispatching
// across external sinks with exponential backoff, circuit breaking, and dead-lettering.
// Enforces Constitution Principle IV: Decoupled Asynchronous Alert Dispatching.
type AlertDispatcher struct {
	cfg            *config.Config
	client         *kgo.Client
	primarySink    sink.Sink
	deadLetterSink sink.Sink
	circuitBreaker *CircuitBreaker
	metrics        *metrics.Metrics
	queue          chan *telemetryv1.AlertEvent
	workerWg       sync.WaitGroup
	stopOnce       sync.Once
}

// NewAlertDispatcher initializes an AlertDispatcher.
func NewAlertDispatcher(
	cfg *config.Config,
	client *kgo.Client,
	primarySink sink.Sink,
	deadLetterSink sink.Sink,
	m *metrics.Metrics,
) *AlertDispatcher {
	if cfg.WorkerPoolSize <= 0 {
		cfg.WorkerPoolSize = 10
	}

	cb := NewCircuitBreaker(cfg.CircuitBreakerThreshold, cfg.CircuitBreakerCoolOff)

	return &AlertDispatcher{
		cfg:            cfg,
		client:         client,
		primarySink:    primarySink,
		deadLetterSink: deadLetterSink,
		circuitBreaker: cb,
		metrics:        m,
		queue:          make(chan *telemetryv1.AlertEvent, cfg.WorkerPoolSize*100),
	}
}

// CircuitBreaker returns the active circuit breaker instance.
func (d *AlertDispatcher) CircuitBreaker() *CircuitBreaker {
	return d.circuitBreaker
}

// Enqueue submits an alert directly into the dispatcher queue (useful for testing or direct ingestion).
func (d *AlertDispatcher) Enqueue(alert *telemetryv1.AlertEvent) bool {
	select {
	case d.queue <- alert:
		return true
	default:
		return false
	}
}

// Start spawns the worker pool and begins polling Franz-go fetches if client is configured.
func (d *AlertDispatcher) Start(ctx context.Context) error {
	// 1. Spawn concurrent worker pool
	for i := 0; i < d.cfg.WorkerPoolSize; i++ {
		d.workerWg.Add(1)
		go d.workerLoop(ctx, i)
	}

	// 2. Poll Franz-go consumer loop if client is present
	if d.client != nil {
		go d.pollConsumer(ctx)
	}

	<-ctx.Done()
	d.Stop()
	return nil
}

// Stop gracefully drains the dispatch queue and shuts down workers.
func (d *AlertDispatcher) Stop() {
	d.stopOnce.Do(func() {
		close(d.queue)
		d.workerWg.Wait()
		if d.client != nil {
			d.client.Close()
		}
	})
}

func (d *AlertDispatcher) pollConsumer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := d.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			var alert telemetryv1.AlertEvent
			if err := proto.Unmarshal(record.Value, &alert); err != nil {
				log.Printf("Failed to unmarshal AlertEvent from partition %d: %v", record.Partition, err)
				continue
			}

			select {
			case d.queue <- &alert:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (d *AlertDispatcher) workerLoop(ctx context.Context, workerID int) {
	defer d.workerWg.Done()

	for alert := range d.queue {
		d.dispatchAlert(ctx, alert)
	}
}

func (d *AlertDispatcher) dispatchAlert(ctx context.Context, alert *telemetryv1.AlertEvent) {
	if d.primarySink == nil {
		return
	}

	sinkName := d.primarySink.Name()
	start := time.Now()

	err := RetryWithBackoff(
		ctx,
		d.cfg.MaxRetries,
		50*time.Millisecond,
		1*time.Second,
		d.circuitBreaker,
		func(c context.Context) error {
			return d.primarySink.Deliver(c, alert)
		},
	)

	duration := time.Since(start).Seconds()

	if d.metrics != nil {
		d.metrics.CircuitBreakerState.WithLabelValues(sinkName).Set(float64(d.circuitBreaker.State()))
	}

	if err != nil {
		if d.metrics != nil {
			d.metrics.AlertsDispatchedTotal.WithLabelValues(sinkName, "failure").Inc()
		}

		// Route to dead-letter sink on unrecoverable failure
		if d.deadLetterSink != nil {
			_ = d.deadLetterSink.Deliver(ctx, alert)
			if d.metrics != nil {
				d.metrics.AlertsDispatchedTotal.WithLabelValues(d.deadLetterSink.Name(), "deadletter").Inc()
			}
		}
		return
	}

	if d.metrics != nil {
		d.metrics.AlertsDispatchedTotal.WithLabelValues(sinkName, "success").Inc()
		d.metrics.AlertDispatchLatencySeconds.WithLabelValues(sinkName).Observe(duration)
	}
}
