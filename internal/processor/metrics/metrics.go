package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds registered Prometheus metrics for the Stream Processing Engine.
type Metrics struct {
	WatermarkLagSeconds        *prometheus.GaugeVec
	OpenWindowsCount           *prometheus.GaugeVec
	ProcessedEventsTotal       *prometheus.CounterVec
	LateEventsTotal            *prometheus.CounterVec
	EmittedAggregatesTotal     *prometheus.CounterVec
	WindowCloseLatencySeconds  *prometheus.HistogramVec
	ConsumerLag                *prometheus.GaugeVec
}

// New creates and registers Prometheus metrics for the stream processor.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		WatermarkLagSeconds: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "stream",
				Name:      "watermark_lag_seconds",
				Help:      "Current event-time watermark lag behind wall clock in seconds per partition.",
			},
			[]string{"partition"},
		),
		OpenWindowsCount: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "stream",
				Name:      "open_windows_count",
				Help:      "Number of currently open, unexpired aggregation windows per partition.",
			},
			[]string{"partition", "window_type"},
		),
		ProcessedEventsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "stream",
				Name:      "events_processed_total",
				Help:      "Total count of incoming telemetry events processed per partition.",
			},
			[]string{"partition"},
		),
		LateEventsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "stream",
				Name:      "late_events_total",
				Help:      "Total count of events rejected because their event-time is older than the current watermark.",
			},
			[]string{"partition"},
		),
		EmittedAggregatesTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "stream",
				Name:      "emitted_aggregates_total",
				Help:      "Total count of closed window aggregate records emitted per partition.",
			},
			[]string{"partition", "window_type"},
		),
		WindowCloseLatencySeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pulse",
				Subsystem: "stream",
				Name:      "window_close_latency_seconds",
				Help:      "Latency between watermark crossing window_end and aggregate emission in seconds.",
				Buckets:   []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.0},
			},
			[]string{"window_type"},
		),
		ConsumerLag: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "stream",
				Name:      "consumer_lag",
				Help:      "Consumer offset lag behind partition high watermark.",
			},
			[]string{"partition"},
		),
	}
}
