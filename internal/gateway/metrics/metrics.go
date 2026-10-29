package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds the registered Prometheus metrics for the Ingestion Gateway.
type Metrics struct {
	IngestedRecordsTotal        *prometheus.CounterVec
	IngestedBytesTotal          *prometheus.CounterVec
	ClockSkewRejectionsTotal    prometheus.Counter
	MalformedPayloadsTotal      prometheus.Counter
	IngestionLatencySeconds     *prometheus.HistogramVec
	BrokerPublishLatencySeconds *prometheus.HistogramVec
	BrokerConnected             prometheus.Gauge

	// Milestone 009 Security Metrics
	AuthSuccessesTotal *prometheus.CounterVec
	AuthFailuresTotal  *prometheus.CounterVec
}

// New creates and registers Prometheus metrics for the gateway.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		IngestedRecordsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "ingest",
				Name:      "records_total",
				Help:      "Total count of telemetry readings processed by the gateway.",
			},
			[]string{"protocol", "status"},
		),
		IngestedBytesTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "ingest",
				Name:      "bytes_total",
				Help:      "Total payload bytes processed by the gateway.",
			},
			[]string{"protocol"},
		),
		ClockSkewRejectionsTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "ingest",
				Name:      "clock_skew_rejected_total",
				Help:      "Total count of readings rejected due to clock skew out of bounds.",
			},
		),
		MalformedPayloadsTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "ingest",
				Name:      "malformed_payloads_total",
				Help:      "Total count of readings rejected due to malformed payload fields.",
			},
		),
		IngestionLatencySeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pulse",
				Subsystem: "ingest",
				Name:      "duration_seconds",
				Help:      "End-to-end ingestion request duration in seconds.",
				Buckets:   []float64{0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0},
			},
			[]string{"protocol"},
		),
		BrokerPublishLatencySeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pulse",
				Subsystem: "broker",
				Name:      "publish_duration_seconds",
				Help:      "Latency for Franz-go broker produce acknowledgement.",
				Buckets:   []float64{0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.1},
			},
			[]string{"topic"},
		),
		BrokerConnected: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "broker",
				Name:      "connected",
				Help:      "Broker connectivity status (1 = connected, 0 = disconnected).",
			},
		),
		AuthSuccessesTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "ingest",
				Name:      "auth_successes_total",
				Help:      "Total count of successfully authenticated ingestion requests.",
			},
			[]string{"protocol", "method"},
		),
		AuthFailuresTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "ingest",
				Name:      "auth_failures_total",
				Help:      "Total count of failed or rejected authentication and spoofing attempts.",
			},
			[]string{"protocol", "reason"},
		),
	}
}
