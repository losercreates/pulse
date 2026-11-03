package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// BridgeMetrics holds Prometheus metrics for pulse-bridge.
type BridgeMetrics struct {
	ReadingsIngestedTotal *prometheus.CounterVec
	IngestionErrorsTotal  *prometheus.CounterVec
	ForwardedTotal        *prometheus.CounterVec
	ForwardLatencySeconds prometheus.Histogram
	SpoolBufferedCount    prometheus.Gauge
	SpoolDiskBytes        prometheus.Gauge
	SpoolWritesTotal      prometheus.Counter
	SpoolReadsTotal       prometheus.Counter
	ConnectedStatus       *prometheus.GaugeVec
	OPCUASubscriptions    *prometheus.GaugeVec
	OPCUAMonitoredItems   *prometheus.GaugeVec
	OPCUANotificationsTotal *prometheus.CounterVec
	OPCUAConnectionState  *prometheus.GaugeVec
	OPCUAErrorsTotal      *prometheus.CounterVec
	OPCUABrowseDurationSeconds *prometheus.HistogramVec
}

// NewBridgeMetrics registers and returns BridgeMetrics.
func NewBridgeMetrics(reg prometheus.Registerer) *BridgeMetrics {
	factory := promauto.With(reg)

	return &BridgeMetrics{
		ReadingsIngestedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "readings_ingested_total",
				Help:      "Total count of readings ingested by protocol adapters.",
			},
			[]string{"adapter", "protocol"},
		),
		IngestionErrorsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "ingestion_errors_total",
				Help:      "Total protocol decoding and ingestion errors.",
			},
			[]string{"adapter", "protocol", "error_type"},
		),
		ForwardedTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "forwarded_total",
				Help:      "Total telemetry readings forwarded to gateway.",
			},
			[]string{"status"},
		),
		ForwardLatencySeconds: factory.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "forward_latency_seconds",
				Help:      "Latency of gRPC batch forwarding in seconds.",
				Buckets:   []float64{0.001, 0.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0},
			},
		),
		SpoolBufferedCount: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "spool_buffered_count",
				Help:      "Number of telemetry records currently spooled on disk.",
			},
		),
		SpoolDiskBytes: factory.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "spool_disk_bytes",
				Help:      "Total disk bytes utilized by the offline spool buffer.",
			},
		),
		SpoolWritesTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "spool_writes_total",
				Help:      "Total count of readings written to offline disk spool.",
			},
		),
		SpoolReadsTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "spool_reads_total",
				Help:      "Total count of readings replayed from offline disk spool.",
			},
		),
		ConnectedStatus: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "bridge",
				Name:      "connected_status",
				Help:      "Connection state (1 = connected, 0 = disconnected).",
			},
			[]string{"target"},
		),
		OPCUASubscriptions: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "bridge_opcua",
				Name:      "active_subscriptions",
				Help:      "Count of active OPC-UA server subscriptions.",
			},
			[]string{"adapter"},
		),
		OPCUAMonitoredItems: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "bridge_opcua",
				Name:      "monitored_items_total",
				Help:      "Total registered monitored items across all sessions.",
			},
			[]string{"adapter"},
		),
		OPCUANotificationsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "bridge_opcua",
				Name:      "notifications_total",
				Help:      "Total data change notifications received from server.",
			},
			[]string{"adapter", "status"},
		),
		OPCUAConnectionState: factory.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "pulse",
				Subsystem: "bridge_opcua",
				Name:      "connection_state",
				Help:      "Connection state (0=Disconnected, 1=Connecting, 2=Connected, 3=Subscribed).",
			},
			[]string{"adapter"},
		),
		OPCUAErrorsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "pulse",
				Subsystem: "bridge_opcua",
				Name:      "errors_total",
				Help:      "Counter of OPC-UA protocol, network, and timeout errors.",
			},
			[]string{"adapter", "type"},
		),
		OPCUABrowseDurationSeconds: factory.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pulse",
				Subsystem: "bridge_opcua",
				Name:      "browse_duration_seconds",
				Help:      "Latency distribution of address space discovery browsing.",
				Buckets:   []float64{0.005, 0.010, 0.025, 0.050, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
			},
			[]string{"adapter"},
		),
	}
}

