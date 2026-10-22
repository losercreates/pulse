package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/losercreates/pulse/internal/alert/detector"
	"github.com/losercreates/pulse/internal/processor/metrics"
	"github.com/losercreates/pulse/internal/processor/watermark"
	"github.com/losercreates/pulse/internal/processor/window"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// AggregatePublisher emits closed window aggregates to Redpanda topic telemetry.aggregates.
type AggregatePublisher interface {
	PublishAggregate(ctx context.Context, agg *telemetryv1.WindowAggregate) error
}

// LateSink routes excessively late events to the dead-letter topic telemetry.late.
type LateSink interface {
	PublishLate(ctx context.Context, late *telemetryv1.LateTelemetryRecord) error
}

// PartitionWorker runs a dedicated loop for a single Redpanda partition.
// In accordance with Constitution Principle I, all window, watermark, and anomaly baseline state
// is strictly partition-local with zero mutexes or locks on the ingestion hot path.
type PartitionWorker struct {
	topic         string
	partitionID   int32
	inbound       chan *kgo.Record
	watermark     *watermark.WatermarkTracker
	windowMgr     *window.WindowManager
	aggPublisher  AggregatePublisher
	lateSink      LateSink
	detector      *detector.AnomalyDetector
	alertPub      detector.AlertPublisher
	metrics       *metrics.Metrics
	idleCheckTick time.Duration

	mu              sync.Mutex
	highOffset      int64
	lastEmittedAggs int64
}

// NewPartitionWorker creates a dedicated partition worker.
func NewPartitionWorker(
	topic string,
	partitionID int32,
	allowedLateness time.Duration,
	idleTimeout time.Duration,
	tumblingSize time.Duration,
	slidingSize time.Duration,
	slidingSlide time.Duration,
	aggPublisher AggregatePublisher,
	lateSink LateSink,
	m *metrics.Metrics,
	bufferSize int,
) *PartitionWorker {
	if bufferSize <= 0 {
		bufferSize = 4096
	}

	calc := window.NewBoundaryCalculator(tumblingSize, slidingSize, slidingSlide)
	wmTracker := watermark.NewTracker(partitionID, allowedLateness, idleTimeout)
	winMgr := window.NewWindowManager(partitionID, calc)

	return &PartitionWorker{
		topic:         topic,
		partitionID:   partitionID,
		inbound:       make(chan *kgo.Record, bufferSize),
		watermark:     wmTracker,
		windowMgr:     winMgr,
		aggPublisher:  aggPublisher,
		lateSink:      lateSink,
		metrics:       m,
		idleCheckTick: 1 * time.Second,
		highOffset:    -1,
	}
}

// Inbound returns the worker's record ingestion channel.
func (w *PartitionWorker) Inbound() chan<- *kgo.Record {
	return w.inbound
}

// PartitionID returns the worker's partition number.
func (w *PartitionWorker) PartitionID() int32 {
	return w.partitionID
}

// HighOffset returns the highest offset consumed by this partition worker.
func (w *PartitionWorker) HighOffset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.highOffset
}

// SetAlertPipeline injects partition-local anomaly detection and decoupled alert publishing.
func (w *PartitionWorker) SetAlertPipeline(det *detector.AnomalyDetector, pub detector.AlertPublisher) {
	w.detector = det
	w.alertPub = pub
}

// Run executes the partition-local consumption loop until ctx is cancelled.
func (w *PartitionWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.idleCheckTick)
	defer ticker.Stop()

	partStr := fmt.Sprintf("%d", w.partitionID)

	for {
		select {
		case <-ctx.Done():
			w.flushAndExit(ctx)
			return

		case record, ok := <-w.inbound:
			if !ok {
				w.flushAndExit(ctx)
				return
			}
			w.processRecord(ctx, record, partStr)

		case <-ticker.C:
			w.checkIdleAndAdvance(ctx, partStr)
		}
	}
}

func (w *PartitionWorker) processRecord(ctx context.Context, record *kgo.Record, partStr string) {
	w.mu.Lock()
	if record.Offset > w.highOffset {
		w.highOffset = record.Offset
	}
	w.mu.Unlock()

	var reading telemetryv1.TelemetryReading
	if err := proto.Unmarshal(record.Value, &reading); err != nil {
		return
	}

	if w.metrics != nil {
		w.metrics.ProcessedEventsTotal.WithLabelValues(partStr).Inc()
	}

	eventTimeNs := reading.TimestampNs

	// Check for late data beyond current watermark
	if w.watermark.IsLate(eventTimeNs) {
		if w.metrics != nil {
			w.metrics.LateEventsTotal.WithLabelValues(partStr).Inc()
		}
		if w.lateSink != nil {
			lateRecord := &telemetryv1.LateTelemetryRecord{
				Reading:     &reading,
				PartitionId: w.partitionID,
				WatermarkNs: w.watermark.CurrentWatermark(),
				LatenessNs:  w.watermark.LatenessDuration(eventTimeNs),
			}
			_ = w.lateSink.PublishLate(ctx, lateRecord)
		}
		return
	}

	// Advance event-time clock and incorporate into open windows
	w.watermark.ProcessEvent(eventTimeNs)
	w.windowMgr.ProcessReading(eventTimeNs, reading.DeviceId, reading.MetricName, reading.Value)

	// Real-time statistical anomaly detection with zero hot-path locks (Constitution Principle I & IV)
	if w.detector != nil && w.alertPub != nil {
		if alert := w.detector.ProcessReading(reading.DeviceId, reading.MetricName, reading.Value, eventTimeNs, w.partitionID); alert != nil {
			_ = w.alertPub.PublishAlert(ctx, alert)
		}
	}

	// Close any windows expired by the advanced watermark
	emitted := w.windowMgr.AdvanceWatermark(w.watermark.CurrentWatermark())
	w.publishAggregates(ctx, emitted, partStr)

	// Update observability metrics
	if w.metrics != nil {
		lagSeconds := float64(time.Now().UnixNano()-w.watermark.CurrentWatermark()) / 1e9
		if lagSeconds < 0 {
			lagSeconds = 0
		}
		w.metrics.WatermarkLagSeconds.WithLabelValues(partStr).Set(lagSeconds)
		w.metrics.OpenWindowsCount.WithLabelValues(partStr, "all").Set(float64(w.windowMgr.OpenWindowsCount()))
	}
}

func (w *PartitionWorker) checkIdleAndAdvance(ctx context.Context, partStr string) {
	if advanced, newWM := w.watermark.CheckIdleTimeout(time.Now()); advanced {
		emitted := w.windowMgr.AdvanceWatermark(newWM)
		w.publishAggregates(ctx, emitted, partStr)

		if w.metrics != nil {
			lagSeconds := float64(time.Now().UnixNano()-newWM) / 1e9
			if lagSeconds < 0 {
				lagSeconds = 0
			}
			w.metrics.WatermarkLagSeconds.WithLabelValues(partStr).Set(lagSeconds)
			w.metrics.OpenWindowsCount.WithLabelValues(partStr, "all").Set(float64(w.windowMgr.OpenWindowsCount()))
		}
	}
}

func (w *PartitionWorker) publishAggregates(ctx context.Context, aggregates []*telemetryv1.WindowAggregate, partStr string) {
	for _, agg := range aggregates {
		if w.aggPublisher != nil {
			_ = w.aggPublisher.PublishAggregate(ctx, agg)
		}
		if w.metrics != nil {
			w.metrics.EmittedAggregatesTotal.WithLabelValues(partStr, "aggregate").Inc()
		}
	}
}

func (w *PartitionWorker) flushAndExit(ctx context.Context) {
	partStr := fmt.Sprintf("%d", w.partitionID)
	emitted := w.windowMgr.FlushAll()
	w.publishAggregates(ctx, emitted, partStr)
}
