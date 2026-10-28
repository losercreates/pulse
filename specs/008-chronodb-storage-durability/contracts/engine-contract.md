# Contract: ChronoDB Durable Storage Engine Interfaces

**Feature**: `008-chronodb-storage-durability`  
**Date**: 2026-10-28  

---

## 1. ChronoEngine Interface (Primary Storage Contract)

The durable storage engine must fulfill the existing `ChronoEngine` contract without modifying downstream HTTP or gRPC servers:

```go
type ChronoEngine interface {
    // PutAggregates writes a batch of window aggregates idempotently.
    // Durability Guarantee: Returns nil only after the batch is appended to active WAL and synced to disk.
    PutAggregates(ctx context.Context, aggs []*telemetryv1.StoredAggregate) (insertedCount int, updatedCount int, err error)

    // QueryTimeSeries retrieves records matching (deviceID, metricName, startNs, endNs) sorted chronologically.
    // Reads across active memtables and immutable SSTable segments with snapshot isolation.
    QueryTimeSeries(ctx context.Context, deviceID, metricName string, startNs, endNs int64, limit int) ([]*telemetryv1.StoredAggregate, error)

    // QueryRollup groups and re-aggregates window intervals into coarser step buckets.
    QueryRollup(ctx context.Context, deviceID, metricName string, startNs, endNs, stepNs int64) ([]*telemetryv1.RollupBucket, error)

    // GetDeviceCatalog lists discovered active devices and recorded metrics.
    GetDeviceCatalog(ctx context.Context, limit int) ([]*telemetryv1.DeviceCatalogEntry, error)

    // TotalRecords returns the total number of unique window aggregates stored across all shards.
    TotalRecords() int

    // Close flushes active memtables, commits checkpointed WAL state, and releases directory locks.
    Close() error
}
```

---

## 2. WALManager Interface (Durability Contract)

```go
type WALManager interface {
    // AppendBatch encodes and synchronously appends a batch of records with CRC32 framing.
    AppendBatch(records []*telemetryv1.StoredAggregate) error

    // Sync flushes kernel buffers to physical storage (fsync).
    Sync() error

    // Replay reads all valid frames from disk, invoking callback on each record.
    // Trailing truncated or corrupt frames are discarded and reported.
    Replay(fn func(record *telemetryv1.StoredAggregate) error) (replayedCount int, err error)

    // Rotate seals the active WAL file and opens a fresh log segment.
    Rotate() error

    // Close releases file handles.
    Close() error
}
```

---

## 3. SSTableManager Interface (Compaction & Read Contract)

```go
type SSTableManager interface {
    // FlushMemtable writes an in-memory sorted collection into an immutable .sst file with index and bloom filter.
    FlushMemtable(records []*telemetryv1.StoredAggregate) (string, error)

    // SearchPoint performs a key lookup across SSTables using bloom filter and sparse index.
    SearchPoint(compositeKey string) (*telemetryv1.StoredAggregate, bool, error)

    // ScanRange performs an iterator scan between [startNs, endNs] across relevant SSTable segments.
    ScanRange(deviceID, metricName string, startNs, endNs int64) ([]*telemetryv1.StoredAggregate, error)

    // Compact merges multiple overlapping SSTable segments, eliminates superseded keys, and purges records older than TTL.
    Compact(retentionTTL time.Duration) (reclaimedBytes int64, err error)
}
```
