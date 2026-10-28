package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// CompactShard performs a multi-way merge across all SSTable segments in a shard,
// eliminates superseded key revisions, evicts records older than retentionTTL,
// and atomically replaces old segments with a single compacted segment.
func CompactShard(shard *durableShard, retentionTTL time.Duration) (int64, error) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if len(shard.sstables) == 0 {
		return 0, nil
	}

	var cutoffNs int64 = 0
	if retentionTTL > 0 {
		cutoffNs = time.Now().Add(-retentionTTL).UnixNano()
	}

	// Calculate initial size of SSTables
	var initialBytes int64 = 0
	for _, reader := range shard.sstables {
		if fi, err := os.Stat(reader.Path()); err == nil {
			initialBytes += fi.Size()
		}
	}

	// 1. Multi-way merge: read records in chronological segment order
	// Newer SSTables overwrite older records for the same composite key
	merged := make(map[string]*telemetryv1.StoredAggregate)

	for _, reader := range shard.sstables {
		records, err := reader.ReadAllRecords()
		if err != nil {
			continue
		}
		for _, rec := range records {
			// TTL Pruning: discard records older than cutoff
			if cutoffNs > 0 && rec.WindowEndNs < cutoffNs {
				continue
			}
			key := FormatCompositeKey(rec.DeviceId, rec.MetricName, rec.WindowStartNs, rec.WindowEndNs)
			merged[key] = rec
		}
	}

	// 2. Prepare merged list
	mergedRecords := make([]*telemetryv1.StoredAggregate, 0, len(merged))
	for _, rec := range merged {
		mergedRecords = append(mergedRecords, rec)
	}

	// Sort chronologically
	sort.Slice(mergedRecords, func(i, j int) bool {
		ki := FormatCompositeKey(mergedRecords[i].DeviceId, mergedRecords[i].MetricName, mergedRecords[i].WindowStartNs, mergedRecords[i].WindowEndNs)
		kj := FormatCompositeKey(mergedRecords[j].DeviceId, mergedRecords[j].MetricName, mergedRecords[j].WindowStartNs, mergedRecords[j].WindowEndNs)
		return ki < kj
	})

	oldReaders := shard.sstables
	oldPaths := make([]string, len(oldReaders))
	for i, r := range oldReaders {
		oldPaths[i] = r.Path()
		_ = r.Close()
	}

	// If no records remain (e.g. all pruned by TTL)
	if len(mergedRecords) == 0 {
		for _, p := range oldPaths {
			_ = os.Remove(p)
		}
		shard.sstables = nil
		// Rebuild series from active memtable
		shard.rebuildSeriesLocked()
		return initialBytes, nil
	}

	// 3. Write new compacted SSTable
	shard.sstSeq++
	compactedFileName := fmt.Sprintf("segment-compacted-%d-%03d.sst", time.Now().Unix(), shard.sstSeq)
	compactedPath := filepath.Join(shard.shardDir, compactedFileName)

	if err := WriteSSTable(compactedPath, mergedRecords); err != nil {
		// Reopen old readers if write failed
		var recoveredReaders []*SSTableReader
		for _, p := range oldPaths {
			if r, err := OpenSSTableReader(p); err == nil {
				recoveredReaders = append(recoveredReaders, r)
			}
		}
		shard.sstables = recoveredReaders
		return 0, fmt.Errorf("compaction failed to write new SSTable %s: %w", compactedPath, err)
	}

	// 4. Remove old SSTable files
	for _, p := range oldPaths {
		_ = os.Remove(p)
	}

	newReader, err := OpenSSTableReader(compactedPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open compacted SSTable %s: %w", compactedPath, err)
	}
	shard.sstables = []*SSTableReader{newReader}

	// 5. Rebuild in-memory series index
	shard.rebuildSeriesLocked()

	// Calculate reclaimed bytes
	var finalBytes int64 = 0
	if fi, err := os.Stat(compactedPath); err == nil {
		finalBytes = fi.Size()
	}

	reclaimed := initialBytes - finalBytes
	if reclaimed < 0 {
		reclaimed = 0
	}
	return reclaimed, nil
}

// rebuildSeriesLocked reconstructs shard.series from the active memtable and current SSTables.
func (s *durableShard) rebuildSeriesLocked() {
	s.series = make(map[string][]*telemetryv1.StoredAggregate)

	// Add records from SSTables
	for _, reader := range s.sstables {
		records, err := reader.ReadAllRecords()
		if err == nil {
			for _, rec := range records {
				sKey := formatSeriesKey(rec.DeviceId, rec.MetricName)
				s.series[sKey] = append(s.series[sKey], rec)
			}
		}
	}

	// Add/overwrite from active memtable
	for _, rec := range s.memtable {
		sKey := formatSeriesKey(rec.DeviceId, rec.MetricName)
		seriesList := s.series[sKey]
		idx := sort.Search(len(seriesList), func(i int) bool {
			return seriesList[i].WindowStartNs >= rec.WindowStartNs
		})
		if idx < len(seriesList) && seriesList[idx].WindowStartNs == rec.WindowStartNs && seriesList[idx].WindowEndNs == rec.WindowEndNs {
			seriesList[idx] = rec
		} else {
			seriesList = append(seriesList, nil)
			copy(seriesList[idx+1:], seriesList[idx:])
			seriesList[idx] = rec
			s.series[sKey] = seriesList
		}
	}

	// Ensure all series are sorted
	for sKey, list := range s.series {
		sort.Slice(list, func(i, j int) bool {
			return list[i].WindowStartNs < list[j].WindowStartNs
		})
		s.series[sKey] = list
	}
}

// CompactAll executes compaction across all shards in the durable engine.
func (e *DurableEngine) CompactAll() (int64, error) {
	var totalReclaimed int64 = 0
	for _, shard := range e.shards {
		reclaimed, err := CompactShard(shard, e.cfg.RetentionTTL)
		if err != nil {
			return totalReclaimed, err
		}
		totalReclaimed += reclaimed
	}
	return totalReclaimed, nil
}

// TotalDiskUsage calculates the total bytes occupied by data files on disk.
func (e *DurableEngine) TotalDiskUsage() (int64, error) {
	var totalBytes int64 = 0
	err := filepath.Walk(e.cfg.DataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			totalBytes += info.Size()
		}
		return nil
	})
	return totalBytes, err
}
