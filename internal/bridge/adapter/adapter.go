package adapter

import (
	"context"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// AdapterStatus captures runtime operational diagnostics for a protocol adapter.
type AdapterStatus struct {
	Connected        bool   `json:"connected"`
	MessagesReceived uint64 `json:"messages_received"`
	ErrorsTotal      uint64 `json:"errors_total"`
	LastError        string `json:"last_error,omitempty"`
}

// ProtocolAdapter defines the lifecycle and message streaming interface for edge protocol drivers
// (such as MQTT v3/v5, Sparkplug B, and Modbus TCP).
type ProtocolAdapter interface {
	// Name returns the unique adapter identifier (e.g. "mqtt-primary", "modbus-plc-01").
	Name() string

	// Start connects to the industrial fieldbus/broker and begins streaming telemetry.
	Start(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) error

	// Stop terminates connection loops and releases network resources.
	Stop() error

	// Status returns health and operational diagnostic metrics.
	Status() AdapterStatus
}

// OfflineSpooler persists unacknowledged telemetry records to local disk during network disconnects
// and re-reads them in chronological sequence upon reconnection.
type OfflineSpooler interface {
	// Write appends a batch of telemetry records to the local spool.
	Write(readings []*telemetryv1.TelemetryReading) error

	// ReadNext retrieves the next available FIFO batch from disk. Returns nil or empty slice when empty.
	ReadNext(maxRecords int) ([]*telemetryv1.TelemetryReading, error)

	// Ack commits the read offset, permitting disk segment cleanup.
	Ack(count int) error

	// BufferedCount returns the total number of unread records currently spooled on disk.
	BufferedCount() int64

	// Close flushes open segment handles and commits state.
	Close() error
}

// GatewayForwarder manages authenticated client-side streaming to pulse-gateway over mTLS.
type GatewayForwarder interface {
	// ForwardBatch sends a batch of telemetry readings over active gRPC stream.
	ForwardBatch(ctx context.Context, batch []*telemetryv1.TelemetryReading) (*telemetryv1.IngestionReceipt, error)

	// IsConnected reports whether the upstream gateway is reachable.
	IsConnected() bool

	// Close terminates the gRPC connection pool.
	Close() error
}

