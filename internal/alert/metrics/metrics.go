package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds registered Prometheus metrics for Anomaly Detection & Alert Dispatching.
type Metrics struct {
	AnomaliesDetectedTotal      *prometheus.CounterVec
	ActiveFiringAlertsGauge     *prometheus.GaugeVec
	AlertsDispatchedTotal       *prometheus.CounterVec
	AlertDispatchLatencySeconds *prometheus.HistogramVec
	CircuitBreakerState         *prometheus.GaugeVec
	AlertRetriesTotal           *prometheus.CounterVec
}

// New creates and registers Prometheus metrics for the alert subsystem.
func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		AnomaliesDetectedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "anomaly",
				Name:      "detections_total",
				Help:      "Total count of statistical anomalies detected.",
			},
			[]string{"severity", "partition"},
		),
		ActiveFiringAlertsGauge: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "alert",
				Name:      "active_firing_total",
				Help:      "Current count of active firing alert incidents per partition.",
			},
			[]string{"partition"},
		),
		AlertsDispatchedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "alert",
				Name:      "dispatched_total",
				Help:      "Total count of alert dispatch attempts by target sink and outcome status.",
			},
			[]string{"sink", "status"},
		),
		AlertDispatchLatencySeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pulse",
				Subsystem: "alert",
				Name:      "dispatch_latency_seconds",
				Help:      "Latency of alert delivery to external sinks in seconds.",
				Buckets:   []float64{0.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
			},
			[]string{"sink"},
		),
		CircuitBreakerState: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "alert",
				Name:      "circuit_breaker_state",
				Help:      "Current circuit breaker status per sink (0 = CLOSED, 1 = OPEN, 2 = HALF_OPEN).",
			},
			[]string{"sink"},
		),
		AlertRetriesTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "alert",
				Name:      "retries_total",
				Help:      "Total count of alert dispatch retry attempts by target sink.",
			},
			[]string{"sink"},
		),
	}
}
