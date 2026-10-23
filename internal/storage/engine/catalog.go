package engine

import (
	"context"
	"sort"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// GetDeviceCatalog aggregates active devices, metric sets, and first/last seen bounds across all shards.
func (e *ShardedMemoryEngine) GetDeviceCatalog(ctx context.Context, limit int) ([]*telemetryv1.DeviceCatalogEntry, error) {
	if limit <= 0 {
		limit = 1000
	}

	var entries []*telemetryv1.DeviceCatalogEntry

	for _, shard := range e.shards {
		shard.mu.RLock()
		for devID, cat := range shard.catalog {
			var metricsList []string
			for m := range cat.metrics {
				metricsList = append(metricsList, m)
			}
			sort.Strings(metricsList)

			entries = append(entries, &telemetryv1.DeviceCatalogEntry{
				DeviceId:    devID,
				Metrics:     metricsList,
				FirstSeenNs: cat.firstSeen,
				LastSeenNs:  cat.lastSeen,
			})
		}
		shard.mu.RUnlock()
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].DeviceId < entries[j].DeviceId
	})

	if len(entries) > limit {
		entries = entries[:limit]
	}

	return entries, nil
}
