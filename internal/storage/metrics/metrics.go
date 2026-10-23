package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds registered Prometheus metrics for the ChronoDB Storage Sink & Query Service.
type Metrics struct {
	StorageWritesTotal       *prometheus.CounterVec
	BatchWriteLatencySeconds *prometheus.HistogramVec
	QueryLatencySeconds      *prometheus.HistogramVec
	ConsumerLagSeconds       *prometheus.GaugeVec
	ActiveShardsGauge        prometheus.Gauge
	StoredRecordsGauge       prometheus.Gauge
}

// New creates and registers Prometheus metrics for the storage subsystem.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		StorageWritesTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "storage",
				Name:      "writes_total",
				Help:      "Total count of window aggregates written to ChronoDB storage by status.",
			},
			[]string{"status"},
		),
		BatchWriteLatencySeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pulse",
				Subsystem: "storage",
				Name:      "batch_write_latency_seconds",
				Help:      "Latency of storage batch writes and disk fsyncs in seconds.",
				Buckets:   []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
			},
			[]string{"operation"},
		),
		QueryLatencySeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pulse",
				Subsystem: "storage",
				Name:      "query_latency_seconds",
				Help:      "Latency of historical telemetry queries in seconds.",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0},
			},
			[]string{"endpoint"},
		),
		ConsumerLagSeconds: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "storage",
				Name:      "consumer_lag_seconds",
				Help:      "Estimated aggregate consumer lag relative to wall clock in seconds.",
			},
			[]string{"partition"},
		),
		ActiveShardsGauge: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "storage",
				Name:      "active_shards_total",
				Help:      "Current count of active storage shards.",
			},
		),
		StoredRecordsGauge: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "storage",
				Name:      "stored_records_total",
				Help:      "Total unique window aggregates currently stored.",
			},
		),
	}
}
