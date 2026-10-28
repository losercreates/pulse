package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// RecoverShard inspects the shard directory, loads existing SSTable readers, and replays all uncommitted WAL segments.
func RecoverShard(shard *durableShard) (int, error) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 1. Discover and open all existing .sst files in sorted order
	entries, err := os.ReadDir(shard.shardDir)
	if err != nil {
		return 0, fmt.Errorf("failed to read shard dir %s during recovery: %w", shard.shardDir, err)
	}

	var sstFiles []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "segment-") && strings.HasSuffix(name, ".sst") {
			sstFiles = append(sstFiles, name)
		}
	}
	sort.Strings(sstFiles)

	for _, sstName := range sstFiles {
		sstPath := filepath.Join(shard.shardDir, sstName)
		reader, err := OpenSSTableReader(sstPath)
		if err != nil {
			// Skip corrupt or incomplete segment
			continue
		}
		shard.sstables = append(shard.sstables, reader)

		// Populate series index and catalog from SSTable
		recs, err := reader.ReadAllRecords()
		if err == nil {
			for _, rec := range recs {
				sKey := formatSeriesKey(rec.DeviceId, rec.MetricName)
				shard.series[sKey] = append(shard.series[sKey], rec)
				shard.updateCatalogLocked(rec)
			}
		}
	}

	// Sort recovered series slices
	for sKey, list := range shard.series {
		sort.Slice(list, func(i, j int) bool {
			return list[i].WindowStartNs < list[j].WindowStartNs
		})
		shard.series[sKey] = list
	}

	// 2. Replay all WAL records into memtable and in-memory series
	replayed, err := shard.wal.Replay(func(record *telemetryv1.StoredAggregate) error {
		compKey := FormatCompositeKey(record.DeviceId, record.MetricName, record.WindowStartNs, record.WindowEndNs)
		sKey := formatSeriesKey(record.DeviceId, record.MetricName)

		shard.memtable[compKey] = record

		// Maintain sorted series
		seriesList := shard.series[sKey]
		idx := sort.Search(len(seriesList), func(i int) bool {
			return seriesList[i].WindowStartNs >= record.WindowStartNs
		})
		if idx < len(seriesList) && seriesList[idx].WindowStartNs == record.WindowStartNs && seriesList[idx].WindowEndNs == record.WindowEndNs {
			seriesList[idx] = record
		} else {
			seriesList = append(seriesList, nil)
			copy(seriesList[idx+1:], seriesList[idx:])
			seriesList[idx] = record
			shard.series[sKey] = seriesList
		}

		// Update catalog
		shard.updateCatalogLocked(record)
		return nil
	})
	if err != nil {
		return replayed, fmt.Errorf("failed to replay WAL for shard %d: %w", shard.shardID, err)
	}

	return replayed, nil
}
