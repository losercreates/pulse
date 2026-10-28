# Feature Specification: Milestone 008 — ChronoDB Storage Durability & Persistent Volume Orchestration

**Feature Branch**: `008-chronodb-storage-durability`  
**Created**: 2026-10-28  
**Status**: Draft  
**Input**: User description: "Create milestone 008: ChronoDB Storage Durability & Persistent Volume Orchestration with embedded LSM disk-backed storage engine, Write-Ahead Logging (WAL), crash recovery protocol, device catalog persistence, and Kubernetes StatefulSet PVC storage orchestration"

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Embedded Disk-Backed LSM Storage Engine & WAL Durability (Priority: P1) 🎯 MVP

As a Time-Series Database Operator, I need window aggregates and device telemetry to be durably committed to an embedded log-structured merge-tree (LSM) disk-backed storage engine with Write-Ahead Logging (WAL) before consumer offsets are acknowledged, so that telemetry data survives daemon crashes, power loss, and ungraceful termination without data loss or file corruption.

**Why this priority**: Durability is the single highest operational risk for any storage sink. Storing aggregates purely in RAM makes historical queries volatile and dependent on broker retention. Persisting writes to disk with WAL guarantees data survivability.

**Independent Test**: Write 10,000 window aggregates across multiple devices to the storage engine; verify records are durably synced to disk SSTables and WAL; terminate the process abruptly (`kill -9`); inspect disk storage and verify 100% of acknowledged records remain intact and readable.

**Acceptance Scenarios**:
1. **Given** incoming window aggregates from the stream processor, **When** the storage sink processes a batch, **Then** all aggregates are committed to an on-disk Write-Ahead Log (WAL) with synchronous write guarantees before consumer offsets are committed to the message broker.
2. **Given** written records in active memtables, **When** memory limits or flush intervals are reached, **Then** records are immutably sorted and written into on-disk SSTable files with bloom filters and sparse indexing.
3. **Given** multiple device telemetry streams, **When** writing records to storage, **Then** data is deterministically partitioned across storage shards on disk based on device identifier hashes.

---

### User Story 2 - Crash Recovery Protocol & Replay Invariance (Priority: P1)

As a Data Analyst and Reliability Engineer, I need the storage service upon startup to automatically recover its in-memory sparse index and device catalog from disk SSTables and WAL segments, seamlessly resuming ingestion from the last committed offset with zero duplicated records or corrupted rollups.

**Why this priority**: Fast, deterministic recovery ensures high availability and eliminates data inconsistencies after container rescheduling, crash loops, or platform rebalances.

**Independent Test**: Simulate random mid-batch process crashes during active synthetic load; restart the storage service; query the REST and gRPC endpoints across time windows spanning the crash; verify that query results match expected aggregates with exactly 0 duplicated records and 100% device catalog consistency (Constitution Principle III).

**Acceptance Scenarios**:
1. **Given** an ungraceful daemon shutdown with unflushed memtable data, **When** the storage service starts, **Then** it scans the WAL, replays all uncheckpointed operations, and reconstructs the active index in under 5 seconds.
2. **Given** previously committed window aggregates, **When** duplicate aggregates are replayed from the message broker during recovery or consumer group rebalance, **Then** idempotent upserts overwrite identical composite keys `(device_id, metric, window_start, window_end)` with zero duplicated metrics.
3. **Given** recovered storage shards, **When** inspecting the device catalog, **Then** device metadata (first seen timestamp, last seen timestamp, metric names) matches historical disk state exactly.

---

### User Story 3 - Kubernetes StatefulSet & PersistentVolumeClaim Orchestration (Priority: P2)

As a Kubernetes Platform Engineer, I need the storage service packaged and deployed as a Kubernetes StatefulSet with dedicated PersistentVolumeClaims (PVCs) for each storage shard pod, so that storage pods maintain stable network identities, dedicated volume attachments, and persistent state across pod rescheduling, node evictions, and rolling upgrades.

**Why this priority**: Stateless Kubernetes Deployments re-attach ephemeral storage on pod recreation, resulting in data loss across rescheduling events. A StatefulSet with volume claim templates guarantees permanent storage attachment.

**Independent Test**: Deploy the storage service via Helm on a Kubernetes cluster with persistent volume provisioning; ingest aggregated data; delete a storage pod; verify that the rescheduled replacement pod re-attaches the identical PVC, restores state in under 5 seconds, and responds to queries with zero data loss.

**Acceptance Scenarios**:
1. **Given** a Kubernetes cluster deployment, **When** installing or upgrading via Helm, **Then** the storage service deploys as a `StatefulSet` with parameterized `volumeClaimTemplates` specifying storage capacity, storage class, and `ReadWriteOnce` access mode.
2. **Given** a rolling upgrade or node draining operation, **When** a storage pod terminates and is rescheduled on an alternative node, **Then** the existing PersistentVolume is automatically remounted at `/var/lib/pulse/chronodb` without permission conflicts.
3. **Given** non-root container security policies (`UID 65532`), **When** PersistentVolumes are mounted, **Then** volume permissions and filesystem ownership match the non-root container user (`fsGroup: 65532`).

---

### User Story 4 - Background Compaction, Disk Quotas & Time-to-Live (TTL) Pruning (Priority: P2)

As a Storage Administrator, I need automated background SSTable compaction and TTL-based data retention policies so that disk utilization remains bounded, obsolete rollups and tombstones are reclaimed, and read amplification is minimized over long-running production workloads.

**Why this priority**: Unbounded append-only disk storage eventually leads to disk exhaustion and degraded read performance due to file proliferation.

**Independent Test**: Ingest aggregates configured with a short retention window (e.g., 7 days) and heavy overwrites; trigger background compaction; verify disk space reclamation, tombstone removal, and query latency across valid ranges remains under 2 seconds.

**Acceptance Scenarios**:
1. **Given** accumulated SSTable files on disk, **When** background compaction runs, **Then** overlapping SSTable ranges are merged, obsolete revisions are discarded, and bloom filters are regenerated.
2. **Given** historical telemetry older than the configured retention TTL, **When** compaction cycles execute, **Then** expired records are permanently evicted and disk space is returned to the filesystem.
3. **Given** active time-range queries during heavy background compaction, **When** clients query time-series data, **Then** snapshot isolation guarantees consistent query results without locking the ingestion hot path.

---

## Edge Cases

- **Abrupt Termination Mid-Fsync**: A crash occurring while the OS is flushing a WAL block must not corrupt earlier committed blocks; the recovery scanner must detect the incomplete trailing record via CRC checksum verification, discard the partial record, and preserve all prior committed transactions.
- **Corrupted Disk Block / Storage Media Failure**: If an SSTable block suffers corruption, the engine must isolate the affected block, return an error for the specific corrupted key range, and avoid crashing the entire daemon.
- **Disk Full / Volume Capacity Exhaustion**: If the PersistentVolume reaches $>95\%$ capacity, the storage engine must refuse new writes with a clear backpressure signal, transition `/healthz` readiness probe to failing (503 Service Unavailable), and prevent consumer offset advancement until capacity is freed.
- **Multi-Attach Storage Locks in Kubernetes**: When a pod is rescheduled across nodes, cloud volume attachments (e.g. AWS EBS or GCP Persistent Disk) may require detach/attach time; the StatefulSet must enforce single-replica pod lifecycle to prevent split-brain multi-mount corruption.
- **Concurrent Queries on Active SSTables**: High-frequency reading while compaction is rewriting SSTable files must use snapshot handles to prevent "file not found" or partial read errors.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Storage engine MUST persist all written window aggregates to disk using an embedded log-structured merge-tree (LSM) storage engine.
- **FR-002**: Storage engine MUST append every batch write to a durable Write-Ahead Log (WAL) with `fsync` synchronization before returning success to the consumer sink.
- **FR-003**: Storage engine MUST maintain deterministic device-based sharding across dedicated on-disk directory structures.
- **FR-004**: Storage engine MUST execute automatic crash recovery on startup, replaying uncheckpointed WAL records and rebuilding in-memory index structures in $<5$ seconds.
- **FR-005**: Storage engine MUST persist the device catalog (first seen, last seen, metric names per device) across process restarts.
- **FR-006**: Storage engine MUST enforce composite key idempotency `(device_id, metric, window_start, window_end)` on disk, ensuring repeated upserts overwrite previous values without duplicate storage or corrupted aggregates.
- **FR-007**: Storage engine MUST execute automated background compaction to merge SSTables, prune expired tombstones, and minimize read amplification.
- **FR-008**: Storage engine MUST support configurable retention TTL (Time-To-Live) and automatically evict aggregates older than the retention window during compaction.
- **FR-009**: Helm chart MUST template the storage service as a Kubernetes `StatefulSet` with `volumeClaimTemplates` specifying storage class, capacity, and `ReadWriteOnce` access mode.
- **FR-010**: Helm chart MUST support configurable PVC storage sizes, storage classes, and retention settings via `values.yaml`.
- **FR-011**: Storage engine MUST expose disk utilization, write throughput, compaction duration, and recovery duration metrics via Prometheus `/metrics`.
- **FR-012**: Readiness probe `/healthz` MUST check disk writeability and database lock status, failing probe if disk is full or storage engine is in error state.

---

### Key Entities *(include if feature involves data)*

- **DurableWindowAggregate**: Represents the persistent on-disk record containing composite key `(device_id, metric, window_start, window_end)`, statistical rollups (count, sum, min, max, mean, variance), tags, and commit timestamp.
- **DeviceCatalogEntry**: Represents device metadata tracking device identifiers, observed metric names, first seen timestamp, and last seen timestamp stored in a persistent catalog index.
- **StorageShardDescriptor**: Represents an on-disk sharded partition managing its own LSM directory, WAL logs, SSTables, and lockfile.
- **CompactionPolicy**: Configuration entity defining max SSTable levels, compaction triggers, tombstone grace periods, and data retention TTL.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Zero data loss across process termination or ungraceful shutdown (`SIGKILL`), with 100% of acknowledged aggregates recoverable on restart.
- **SC-002**: Startup crash recovery completes in under 5 seconds for databases containing up to 1,000,000 historical records.
- **SC-003**: Idempotent replay of 10,000 previously committed records results in exactly 0 duplicate entries and 0 variance in downstream query responses.
- **SC-004**: Time-series query latency across 24-hour ranges remains under 2 seconds (p95) even while concurrent background compaction is executing.
- **SC-005**: Kubernetes pod rescheduling preserves 100% of data across rolling restarts via PersistentVolumeClaim re-attachment without requiring Kafka consumer group offset resets.
- **SC-006**: Disk footprint remains bounded within configured retention quotas with background compaction reclaiming at least 95% of tombstoned or expired data.

---

## Assumptions

- **Local Fast Storage**: Underlying Kubernetes cluster provides low-latency block storage (e.g. SSD/NVMe or cloud persistent disks with standard IOPS) for PersistentVolumes.
- **File Locking**: Storage directories are locked by the active process using flock/file locks to prevent concurrent process corruption on shared mounts.
- **Backward Compatibility**: The existing HTTP REST and gRPC query contracts defined in `specs/004-chronodb-storage-query/contracts/` remain unchanged.
- **Zero-Lock Ingestion**: Disk writes occur within the dedicated consumer sink worker pool, preserving the zero-lock guarantees of the ingestion hot path (Constitution Principle I & V).
