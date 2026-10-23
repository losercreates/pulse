package engine

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

type seriesKey struct {
	deviceID   string
	metricName string
}

func formatSeriesKey(deviceID, metricName string) string {
	return fmt.Sprintf("%s#%s", deviceID, metricName)
}

type catalogItem struct {
	deviceID  string
	metrics   map[string]bool
	firstSeen int64
	lastSeen  int64
}

type storageShard struct {
	mu      sync.RWMutex
	records map[string]*telemetryv1.StoredAggregate
	series  map[string][]*telemetryv1.StoredAggregate
	catalog map[string]*catalogItem
}

func newStorageShard() *storageShard {
	return &storageShard{
		records: make(map[string]*telemetryv1.StoredAggregate),
		series:  make(map[string][]*telemetryv1.StoredAggregate),
		catalog: make(map[string]*catalogItem),
	}
}

// ShardedMemoryEngine implements ChronoEngine in memory with deterministic device-based sharding.
type ShardedMemoryEngine struct {
	numShards int
	shards    []*storageShard
}

// NewShardedMemoryEngine initializes a sharded in-memory storage engine.
func NewShardedMemoryEngine(numShards int) *ShardedMemoryEngine {
	if numShards <= 0 {
		numShards = 16
	}

	shards := make([]*storageShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = newStorageShard()
	}

	return &ShardedMemoryEngine{
		numShards: numShards,
		shards:    shards,
	}
}

func (e *ShardedMemoryEngine) getShard(deviceID string) *storageShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(deviceID))
	idx := h.Sum32() % uint32(e.numShards)
	return e.shards[idx]
}

// PutAggregates writes a batch of window aggregates idempotently.
// Enforces Constitution Principle III: composite primary key deduplication.
func (e *ShardedMemoryEngine) PutAggregates(ctx context.Context, aggs []*telemetryv1.StoredAggregate) (int, int, error) {
	if len(aggs) == 0 {
		return 0, 0, nil
	}

	// Group by shard to minimize lock contention
	byShard := make(map[*storageShard][]*telemetryv1.StoredAggregate)
	for _, agg := range aggs {
		shard := e.getShard(agg.DeviceId)
		byShard[shard] = append(byShard[shard], agg)
	}

	totalInserted := 0
	totalUpdated := 0

	for shard, records := range byShard {
		shard.mu.Lock()
		for _, agg := range records {
			compKey := FormatCompositeKey(agg.DeviceId, agg.MetricName, agg.WindowStartNs, agg.WindowEndNs)
			sKey := formatSeriesKey(agg.DeviceId, agg.MetricName)

			if _, exists := shard.records[compKey]; exists {
				// Idempotent upsert: update existing entry
				shard.records[compKey] = agg
				// Update in sorted series slice
				seriesList := shard.series[sKey]
				for i, item := range seriesList {
					if item.WindowStartNs == agg.WindowStartNs && item.WindowEndNs == agg.WindowEndNs {
						seriesList[i] = agg
						break
					}
				}
				totalUpdated++
			} else {
				// New entry insertion
				shard.records[compKey] = agg
				seriesList := shard.series[sKey]

				// Insert maintaining sorted order by WindowStartNs
				idx := sort.Search(len(seriesList), func(i int) bool {
					return seriesList[i].WindowStartNs >= agg.WindowStartNs
				})
				seriesList = append(seriesList, nil)
				copy(seriesList[idx+1:], seriesList[idx:])
				seriesList[idx] = agg
				shard.series[sKey] = seriesList

				// Update catalog metadata
				cat, existsCat := shard.catalog[agg.DeviceId]
				if !existsCat {
					cat = &catalogItem{
						deviceID:  agg.DeviceId,
						metrics:   make(map[string]bool),
						firstSeen: agg.WindowStartNs,
						lastSeen:  agg.WindowEndNs,
					}
					shard.catalog[agg.DeviceId] = cat
				}
				cat.metrics[agg.MetricName] = true
				if agg.WindowStartNs < cat.firstSeen {
					cat.firstSeen = agg.WindowStartNs
				}
				if agg.WindowEndNs > cat.lastSeen {
					cat.lastSeen = agg.WindowEndNs
				}

				totalInserted++
			}
		}
		shard.mu.Unlock()
	}

	return totalInserted, totalUpdated, nil
}

// QueryTimeSeries retrieves records matching (deviceID, metricName, startNs, endNs) sorted chronologically.
func (e *ShardedMemoryEngine) QueryTimeSeries(ctx context.Context, deviceID, metricName string, startNs, endNs int64, limit int) ([]*telemetryv1.StoredAggregate, error) {
	if limit <= 0 {
		limit = 1000
	}

	shard := e.getShard(deviceID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	sKey := formatSeriesKey(deviceID, metricName)
	seriesList, exists := shard.series[sKey]
	if !exists || len(seriesList) == 0 {
		return []*telemetryv1.StoredAggregate{}, nil
	}

	// Binary search for startNs
	startIdx := sort.Search(len(seriesList), func(i int) bool {
		return seriesList[i].WindowStartNs >= startNs
	})

	var results []*telemetryv1.StoredAggregate
	for i := startIdx; i < len(seriesList); i++ {
		item := seriesList[i]
		if item.WindowStartNs >= endNs {
			break
		}
		results = append(results, item)
		if len(results) >= limit {
			break
		}
	}

	return results, nil
}

// QueryRollup is implemented in rollup.go.
// GetDeviceCatalog is implemented in catalog.go.

// TotalRecords returns the total number of unique window aggregates stored across all shards.
func (e *ShardedMemoryEngine) TotalRecords() int {
	total := 0
	for _, shard := range e.shards {
		shard.mu.RLock()
		total += len(shard.records)
		shard.mu.RUnlock()
	}
	return total
}

// Close terminates the engine.
func (e *ShardedMemoryEngine) Close() error {
	return nil
}
