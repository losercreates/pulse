package engine

import (
	"context"
	"fmt"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// FormatCompositeKey formats the deterministic primary key for idempotent deduplication.
// Format: {device_id}#{metric_name}#{window_start_ns}#{window_end_ns}
func FormatCompositeKey(deviceID, metricName string, startNs, endNs int64) string {
	return fmt.Sprintf("%s#%s#%d#%d", deviceID, metricName, startNs, endNs)
}

// ChronoEngine abstracts the polyglot time-series storage engine.
type ChronoEngine interface {
	// PutAggregates writes a batch of window aggregates idempotently. Returns (insertedCount, updatedCount, error).
	PutAggregates(ctx context.Context, aggs []*telemetryv1.StoredAggregate) (int, int, error)

	// QueryTimeSeries retrieves records matching (deviceID, metricName, startNs, endNs) sorted chronologically.
	QueryTimeSeries(ctx context.Context, deviceID, metricName string, startNs, endNs int64, limit int) ([]*telemetryv1.StoredAggregate, error)

	// QueryRollup groups and re-aggregates window intervals into coarser step buckets.
	QueryRollup(ctx context.Context, deviceID, metricName string, startNs, endNs, stepNs int64) ([]*telemetryv1.RollupBucket, error)

	// GetDeviceCatalog lists discovered active devices and recorded metrics.
	GetDeviceCatalog(ctx context.Context, limit int) ([]*telemetryv1.DeviceCatalogEntry, error)

	// TotalRecords returns the total number of unique window aggregates stored.
	TotalRecords() int

	// Close cleanly terminates the storage engine.
	Close() error
}
