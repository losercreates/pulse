# Research & Technical Decisions: Milestone 008 — ChronoDB Storage Durability & Persistent Volume Orchestration

**Feature**: `008-chronodb-storage-durability`  
**Date**: 2026-10-28  
**Status**: Completed  

---

## 1. Storage Engine Architecture & Design Trade-offs

### Context & Problem
ChronoDB in Milestone 004 was implemented as an in-memory sharded storage engine (`ShardedMemoryEngine`). While fast and thread-safe, all data in memory is lost whenever a `chronod` pod is restarted, rescheduled, or upgraded. Re-reading from offset 0 on Kafka across 12 partitions would take significant time and cause memory exhaustion. Production readiness requires durable disk persistence with zero data loss on crashes.

### Evaluated Alternatives

| Approach | Durability | CGO / Footprint | Query Latency | Compaction & Recovery | Decision |
|---|---|---|---|---|---|
| **A. External Time-Series DB (ClickHouse / Timescale)** | High | Requires running separate multi-node cluster; heavy ops burden | Sub-second | Managed by external database | Rejected: Violates lightweight single-binary operational simplicity of Pulse. |
| **B. Heavy Go LSM (Pebble / BadgerDB)** | High | Pure Go, but introduces large dependency trees (30+ transitive modules) | Low | Generic key-value compaction, high memory overhead for small keys | Evaluated as reference design. |
| **C. Specialized Pure-Go LSM with WAL & SSTable Segments** | High | **Zero external dependencies**, $<1\text{MB}$ binary delta, static `CGO_ENABLED=0`, distroless compatible | **$< 2\text{ms}$ point / $< 50\text{ms}$ range** | **Time-partitioned SSTable compaction with TTL pruning** | **Selected** |

### Decision Rationale
We implement a high-performance, embedded, pure-Go **Log-Structured Merge-tree (LSM) storage engine with Write-Ahead Logging (WAL)**:
- **Zero New External Dependencies**: Maintains our pure static compilation (`CGO_ENABLED=0`) and distroless container footprint ($< 50\text{MB}$).
- **Exact Interface Match**: Perfectly implements `ChronoEngine`, enabling zero-downtime drop-in replacement for `ShardedMemoryEngine`.
- **Time-Series Optimized**: Encodes keys as composite strings `(device_id#metric#start_ns#end_ns)` with sorted time ordering, enabling efficient contiguous time-range disk scans and fast downsampling rollups.
- **Deterministic 16-Shard Architecture**: Shards data on disk under `/var/lib/pulse/chronodb/shard-{00..15}/`, ensuring lock-free concurrency across shards matching Constitution Principle I & V.

---

## 2. Write-Ahead Logging (WAL) & Crash Recovery Protocol

### WAL Binary Framing Specification
Every write operation to a shard must append to an active WAL file (`wal-<epoch>.log`) before the in-memory memtable is updated:

```text
+----------------+----------------+----------------+--------------------------+
| Magic (4 bytes)| Length (4 bytes)| CRC32 (4 bytes)| Serialized Payload (N B) |
| 0x50554C53     | Big-Endian uint| IEEE Checksum  | Protobuf StoredAggregate |
+----------------+----------------+----------------+--------------------------+
```

### Durability Guarantees
- **Batch Fsync**: `PutAggregates` writes the entire batch of aggregates into the shard's active WAL, calls `os.File.Sync()`, and only then updates the memtable.
- **Offset Commit Alignment**: The Kafka consumer sink commits partition offsets to Redpanda *only* after `PutAggregates` returns success. This guarantees **At-Least-Once Delivery** and **Idempotency** (Constitution Principle III).

### Crash Recovery Algorithm
1. **Directory Discovery**: On engine boot, locate all shard directories `shard-00` through `shard-15`.
2. **SSTable Index Loading**: Open all immutable `.sst` files, read their tail sparse indexes into memory.
3. **WAL Replay**:
   - For each active WAL log, iterate through frames sequentially.
   - Verify CRC32 checksum for each frame.
   - **Torn Write Handling**: If an incomplete or corrupted frame is encountered at the end of the WAL (from a crash mid-`fsync`), truncate the corrupted trailing bytes, log a recovery warning, and retain all previous valid frames.
   - Insert recovered records into the active memtable and update the device catalog.
4. **Readiness**: Reopen a fresh WAL segment for new incoming writes.

---

## 3. SSTable Lifecycle, Flushing & Compaction

### Memtable Flush Lifecycle
- Each shard maintains an active in-memory memtable (sorted slice with fast binary search).
- When memtable size reaches **4 MB** (or 5,000 aggregates), or on graceful daemon termination, the memtable is frozen and flushed to a new immutable SSTable file:
  `shard-XX/segment-<timestamp>-<seq>.sst`
- A fresh WAL segment is initiated, and the old WAL segment is marked checkpointed.

### SSTable File Format
```text
+-------------------------------------------------------+
| Data Block 0: Key-Value Sorted Aggregate Records      |
+-------------------------------------------------------+
| Data Block 1: Key-Value Sorted Aggregate Records      |
+-------------------------------------------------------+
| ...                                                   |
+-------------------------------------------------------+
| Sparse Index Block: [Key, Offset, Length] Entries     |
+-------------------------------------------------------+
| Bloom Filter Block: Fast non-membership checks        |
+-------------------------------------------------------+
| Trailer (32 bytes): [IndexOffset, BloomOffset, Magic] |
+-------------------------------------------------------+
```

### Compaction & TTL Retention
- **Background Compaction Worker**: Runs asynchronously every 5 minutes (or when segment count $> 8$).
- **Multi-Way Merge**: Reads multiple sorted `.sst` files concurrently, performing an $N$-way merge sort.
- **Idempotent Deduplication**: If multiple records share the identical composite key `(device_id, metric, window_start, window_end)`, only the latest record is preserved.
- **TTL Eviction**: Records older than the retention threshold (default: 14 days) are dropped during compaction, reclaiming disk space automatically.

---

## 4. Kubernetes StatefulSet & PersistentVolumeClaim Orchestration

### Why StatefulSet Replaces Deployment for ChronoDB
- **Stateless Deployments**: When a pod restarts in a `Deployment`, Kubernetes can mount a brand-new ephemeral volume or fail if multiple pods try to claim the same `ReadWriteOnce` volume.
- **StatefulSet Guarantees**:
  - Stable Pod DNS: `pulse-chronod-0.pulse-chronod-headless.default.svc.cluster.local`.
  - Dedicated PVC Binding: `data-pulse-chronod-0` persistently binds to pod ordinal `0`.
  - Ordered Rolling Updates: Safe termination of old pods and detachment of EBS/GCP disks before new pods start, eliminating multi-attach lock deadlocks.

### Helm Architecture Updates
- Manifest: `deployments/helm/pulse/templates/chronod-statefulset.yaml` replacing `chronod-deployment.yaml`.
- Headless Service: `chronod-headless-service.yaml` for deterministic peer discovery.
- `volumeClaimTemplates`:
  ```yaml
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ "ReadWriteOnce" ]
        storageClassName: {{ .Values.chronod.persistence.storageClass | quote }}
        resources:
          requests:
            storage: {{ .Values.chronod.persistence.size | default "10Gi" }}
  ```
- Non-Root Security:
  ```yaml
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    fsGroup: 65532
  ```

---

## 5. Risk Assessment & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| **Disk Exhaustion** | Storage crash & consumer stall | Prometheus alert on $>85\%$ disk utilization; readiness probe fails on $>95\%$ capacity, applying backpressure to Redpanda. |
| **High Compaction I/O Spikes** | Degraded read latency | Background compaction runs with rate-limited I/O and reads use immutable snapshot handles. |
| **Recovery Latency on Huge WAL** | Slow pod boot | Memtables flush aggressively on graceful SIGTERM, keeping WAL sizes minimal ($<10\text{MB}$). |
