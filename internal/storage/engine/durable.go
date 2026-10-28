package engine

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"path/filepath"
	"sort"
	"sync"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// DurableConfig specifies configuration options for the durable LSM storage engine.
type DurableConfig struct {
	DataDir            string
	NumShards          int
	MemtableLimit      int
	RetentionTTL       time.Duration
	CompactionInterval time.Duration
}

type durableShard struct {
	shardID       int
	shardDir      string
	wal           *WALManager
	mu            sync.RWMutex
	memtable      map[string]*telemetryv1.StoredAggregate
	series        map[string][]*telemetryv1.StoredAggregate
	catalog       map[string]*catalogItem
	sstables      []*SSTableReader
	memtableLimit int
	sstSeq        uint64
}

func (s *durableShard) updateCatalogLocked(agg *telemetryv1.StoredAggregate) {
	cat, exists := s.catalog[agg.DeviceId]
	if !exists {
		cat = &catalogItem{
			deviceID:  agg.DeviceId,
			metrics:   make(map[string]bool),
			firstSeen: agg.WindowStartNs,
			lastSeen:  agg.WindowEndNs,
		}
		s.catalog[agg.DeviceId] = cat
	}
	cat.metrics[agg.MetricName] = true
	if agg.WindowStartNs < cat.firstSeen {
		cat.firstSeen = agg.WindowStartNs
	}
	if agg.WindowEndNs > cat.lastSeen {
		cat.lastSeen = agg.WindowEndNs
	}
}

// flushMemtableLocked writes the current active memtable to a new SSTable segment.
// Assumes s.mu is held by caller.
func (s *durableShard) flushMemtableLocked() error {
	if len(s.memtable) == 0 {
		return nil
	}

	records := make([]*telemetryv1.StoredAggregate, 0, len(s.memtable))
	for _, rec := range s.memtable {
		records = append(records, rec)
	}

	s.sstSeq++
	sstFileName := fmt.Sprintf("segment-%d-%03d.sst", time.Now().Unix(), s.sstSeq)
	sstPath := filepath.Join(s.shardDir, sstFileName)

	if err := WriteSSTable(sstPath, records); err != nil {
		return fmt.Errorf("failed to write SSTable %s: %w", sstPath, err)
	}

	reader, err := OpenSSTableReader(sstPath)
	if err != nil {
		return fmt.Errorf("failed to open newly written SSTable %s: %w", sstPath, err)
	}
	s.sstables = append(s.sstables, reader)

	// Rotate WAL
	if err := s.wal.Rotate(); err != nil {
		return fmt.Errorf("failed to rotate WAL after SSTable flush: %w", err)
	}

	// Reset active memtable
	s.memtable = make(map[string]*telemetryv1.StoredAggregate)

	// Clean up checkpointed WAL
	_ = s.wal.PurgeCheckpointed()

	return nil
}

// DurableEngine implements ChronoEngine with durable WAL, SSTables, and crash recovery.
type DurableEngine struct {
	cfg       DurableConfig
	lock      *DirLock
	shards    []*durableShard
	stopCh    chan struct{}
	wg        sync.WaitGroup
	closed    bool
	muCatalog sync.Mutex
}

// NewDurableEngine initializes the durable engine, acquiring directory locks and recovering state.
func NewDurableEngine(cfg DurableConfig) (*DurableEngine, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("DataDir must not be empty")
	}
	if cfg.NumShards <= 0 {
		cfg.NumShards = 16
	}
	if cfg.MemtableLimit <= 0 {
		cfg.MemtableLimit = 5000
	}
	if cfg.CompactionInterval <= 0 {
		cfg.CompactionInterval = 5 * time.Minute
	}

	// 1. Initialize directory layout
	if err := EnsureLayout(cfg.DataDir, cfg.NumShards); err != nil {
		return nil, fmt.Errorf("failed to initialize directory layout: %w", err)
	}

	// 2. Acquire advisory flock on LOCK file
	dirLock, err := AcquireDirLock(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire directory lock: %w", err)
	}

	// 3. Load persistent device catalog
	savedCatalog, _ := LoadCatalog(cfg.DataDir)

	engine := &DurableEngine{
		cfg:    cfg,
		lock:   dirLock,
		shards: make([]*durableShard, cfg.NumShards),
		stopCh: make(chan struct{}),
	}

	// 4. Initialize shards & recover from WAL / SSTables
	for i := 0; i < cfg.NumShards; i++ {
		shardDir := ShardDir(cfg.DataDir, i)
		walMgr, err := OpenWALManager(shardDir)
		if err != nil {
			_ = dirLock.Release()
			return nil, fmt.Errorf("failed to open WAL for shard %d: %w", i, err)
		}

		shard := &durableShard{
			shardID:       i,
			shardDir:      shardDir,
			wal:           walMgr,
			memtable:      make(map[string]*telemetryv1.StoredAggregate),
			series:        make(map[string][]*telemetryv1.StoredAggregate),
			catalog:       make(map[string]*catalogItem),
			memtableLimit: cfg.MemtableLimit,
		}

		// Pre-populate catalog from savedCatalog for devices mapping to this shard
		if savedCatalog != nil {
			for devID, item := range savedCatalog {
				h := fnv.New32a()
				_, _ = h.Write([]byte(devID))
				if int(h.Sum32()%uint32(cfg.NumShards)) == i {
					shard.catalog[devID] = item
				}
			}
		}

		// Recover shard state (SSTables + WAL replay)
		if _, err := RecoverShard(shard); err != nil {
			_ = dirLock.Release()
			return nil, fmt.Errorf("failed to recover shard %d: %w", i, err)
		}

		engine.shards[i] = shard
	}

	// 5. Start background periodic tasks (catalog persistence and compaction)
	engine.wg.Add(1)
	go engine.backgroundWorker()

	return engine, nil
}

func (e *DurableEngine) getShard(deviceID string) *durableShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(deviceID))
	idx := h.Sum32() % uint32(e.cfg.NumShards)
	return e.shards[idx]
}

// PutAggregates writes a batch of window aggregates idempotently.
// Durability Guarantee: Synchronously appends to WAL before acknowledging.
func (e *DurableEngine) PutAggregates(ctx context.Context, aggs []*telemetryv1.StoredAggregate) (int, int, error) {
	if len(aggs) == 0 {
		return 0, 0, nil
	}

	byShard := make(map[*durableShard][]*telemetryv1.StoredAggregate)
	for _, agg := range aggs {
		shard := e.getShard(agg.DeviceId)
		byShard[shard] = append(byShard[shard], agg)
	}

	totalInserted := 0
	totalUpdated := 0

	for shard, records := range byShard {
		// 1. Durably append to WAL and fsync
		if err := shard.wal.AppendBatch(records); err != nil {
			return totalInserted, totalUpdated, fmt.Errorf("failed to append WAL for shard %d: %w", shard.shardID, err)
		}

		// 2. Commit to in-memory memtable and series index
		shard.mu.Lock()
		for _, agg := range records {
			compKey := FormatCompositeKey(agg.DeviceId, agg.MetricName, agg.WindowStartNs, agg.WindowEndNs)
			sKey := formatSeriesKey(agg.DeviceId, agg.MetricName)

			seriesList := shard.series[sKey]
			idx := sort.Search(len(seriesList), func(i int) bool {
				return seriesList[i].WindowStartNs >= agg.WindowStartNs
			})
			exists := idx < len(seriesList) && seriesList[idx].WindowStartNs == agg.WindowStartNs && seriesList[idx].WindowEndNs == agg.WindowEndNs

			if exists {
				shard.memtable[compKey] = agg
				seriesList[idx] = agg
				totalUpdated++
			} else {
				shard.memtable[compKey] = agg
				seriesList = append(seriesList, nil)
				copy(seriesList[idx+1:], seriesList[idx:])
				seriesList[idx] = agg
				shard.series[sKey] = seriesList

				shard.updateCatalogLocked(agg)
				totalInserted++
			}
		}

		// Check memtable capacity
		if len(shard.memtable) >= shard.memtableLimit {
			_ = shard.flushMemtableLocked()
		}
		shard.mu.Unlock()
	}

	return totalInserted, totalUpdated, nil
}

// QueryTimeSeries retrieves records matching (deviceID, metricName, startNs, endNs) sorted chronologically.
func (e *DurableEngine) QueryTimeSeries(ctx context.Context, deviceID, metricName string, startNs, endNs int64, limit int) ([]*telemetryv1.StoredAggregate, error) {
	if limit <= 0 {
		limit = 1000
	}

	shard := e.getShard(deviceID)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Map keyed by WindowStartNs for deduplication across SSTables and active memtable
	mergedMap := make(map[int64]*telemetryv1.StoredAggregate)

	// 1. Read from SSTables (older to newer)
	for _, reader := range shard.sstables {
		sstRecords, err := reader.ScanRange(deviceID, metricName, startNs, endNs)
		if err == nil {
			for _, rec := range sstRecords {
				mergedMap[rec.WindowStartNs] = rec
			}
		}
	}

	// 2. Read from in-memory series (latest data overrides SSTables)
	sKey := formatSeriesKey(deviceID, metricName)
	if seriesList, exists := shard.series[sKey]; exists {
		startIdx := sort.Search(len(seriesList), func(i int) bool {
			return seriesList[i].WindowStartNs >= startNs
		})
		for i := startIdx; i < len(seriesList); i++ {
			item := seriesList[i]
			if item.WindowStartNs >= endNs {
				break
			}
			mergedMap[item.WindowStartNs] = item
		}
	}

	// 3. Extract and sort chronologically
	results := make([]*telemetryv1.StoredAggregate, 0, len(mergedMap))
	for _, rec := range mergedMap {
		results = append(results, rec)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].WindowStartNs < results[j].WindowStartNs
	})

	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// QueryRollup groups and re-aggregates window intervals into coarser step buckets.
func (e *DurableEngine) QueryRollup(ctx context.Context, deviceID, metricName string, startNs, endNs, stepNs int64) ([]*telemetryv1.RollupBucket, error) {
	if stepNs <= 0 {
		stepNs = 60_000_000_000 // default 1 minute
	}

	records, err := e.QueryTimeSeries(ctx, deviceID, metricName, startNs, endNs, 100000)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return []*telemetryv1.RollupBucket{}, nil
	}

	var buckets []*telemetryv1.RollupBucket
	var currentBucket *telemetryv1.RollupBucket

	for _, rec := range records {
		bucketStart := (rec.WindowStartNs / stepNs) * stepNs
		bucketEnd := bucketStart + stepNs

		if currentBucket == nil || currentBucket.BucketStartNs != bucketStart {
			if currentBucket != nil {
				if currentBucket.Count > 0 {
					currentBucket.Mean = currentBucket.Sum / float64(currentBucket.Count)
				}
				buckets = append(buckets, currentBucket)
			}

			currentBucket = &telemetryv1.RollupBucket{
				BucketStartNs: bucketStart,
				BucketEndNs:   bucketEnd,
				Count:         rec.Count,
				Sum:           rec.Sum,
				Min:           rec.Min,
				Max:           rec.Max,
			}
		} else {
			currentBucket.Count += rec.Count
			currentBucket.Sum += rec.Sum
			currentBucket.Min = math.Min(currentBucket.Min, rec.Min)
			currentBucket.Max = math.Max(currentBucket.Max, rec.Max)
		}
	}

	if currentBucket != nil {
		if currentBucket.Count > 0 {
			currentBucket.Mean = currentBucket.Sum / float64(currentBucket.Count)
		}
		buckets = append(buckets, currentBucket)
	}

	return buckets, nil
}

// GetDeviceCatalog lists discovered active devices and recorded metrics across all shards.
func (e *DurableEngine) GetDeviceCatalog(ctx context.Context, limit int) ([]*telemetryv1.DeviceCatalogEntry, error) {
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

// TotalRecords returns the total number of unique records across all shards.
func (e *DurableEngine) TotalRecords() int {
	total := 0
	for _, shard := range e.shards {
		shard.mu.RLock()
		total += len(shard.memtable)
		for _, sst := range shard.sstables {
			total += int(sst.RecordCount())
		}
		shard.mu.RUnlock()
	}
	return total
}

// FlushAll forces all shards to write active memtables to SSTable segments.
func (e *DurableEngine) FlushAll() error {
	for _, shard := range e.shards {
		shard.mu.Lock()
		if err := shard.flushMemtableLocked(); err != nil {
			shard.mu.Unlock()
			return err
		}
		shard.mu.Unlock()
	}
	return e.saveCatalog()
}

func (e *DurableEngine) saveCatalog() error {
	e.muCatalog.Lock()
	defer e.muCatalog.Unlock()

	combined := make(map[string]*catalogItem)
	for _, shard := range e.shards {
		shard.mu.RLock()
		for devID, item := range shard.catalog {
			combined[devID] = item
		}
		shard.mu.RUnlock()
	}

	return SaveCatalog(e.cfg.DataDir, combined)
}

func (e *DurableEngine) backgroundWorker() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.cfg.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			_ = e.saveCatalog()
			_, _ = e.CompactAll()
		}
	}
}

// Close flushes active memtables, commits checkpointed WAL state, and releases directory locks.
func (e *DurableEngine) Close() error {
	e.muCatalog.Lock()
	defer e.muCatalog.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	close(e.stopCh)
	e.wg.Wait()

	// Flush active memtables and close file handles
	for _, shard := range e.shards {
		shard.mu.Lock()
		_ = shard.flushMemtableLocked()
		_ = shard.wal.Close()
		for _, sst := range shard.sstables {
			_ = sst.Close()
		}
		shard.mu.Unlock()
	}

	// Persist device catalog
	combined := make(map[string]*catalogItem)
	for _, shard := range e.shards {
		for devID, item := range shard.catalog {
			combined[devID] = item
		}
	}
	_ = SaveCatalog(e.cfg.DataDir, combined)

	// Release directory advisory lock
	if e.lock != nil {
		_ = e.lock.Release()
	}

	return nil
}
