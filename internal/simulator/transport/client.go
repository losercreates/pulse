package transport

import (
	"context"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// Sender abstracts the client transmission protocol to the Ingestion Gateway.
type Sender interface {
	// SendBatch dispatches a slice of telemetry readings to the gateway.
	// Returns the number of successfully delivered readings and any error.
	SendBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error)

	// Close cleanly terminates network connections and streams.
	Close() error
}
