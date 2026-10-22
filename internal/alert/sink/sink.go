package sink

import (
	"context"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// Sink defines an abstract destination for delivering alert events.
type Sink interface {
	Name() string
	Deliver(ctx context.Context, alert *telemetryv1.AlertEvent) error
}
