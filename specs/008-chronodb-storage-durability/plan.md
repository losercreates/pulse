# Implementation Plan: Milestone 008 — ChronoDB Storage Durability & Persistent Volume Orchestration

**Branch**: `008-chronodb-storage-durability` | **Date**: 2026-10-28 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/008-chronodb-storage-durability/spec.md`

---

## Summary

Implement an embedded, disk-backed Log-Structured Merge-tree (LSM) storage engine with synchronous Write-Ahead Logging (WAL) and crash recovery for ChronoDB, replacing the purely ephemeral in-memory map. Ensure every window aggregate batch write is durably synchronized to disk before Kafka consumer offsets are committed, satisfying Constitution Principle III. Upgrade Kubernetes packaging for ChronoDB from a stateless Deployment to a StatefulSet with PersistentVolumeClaims (`ReadWriteOnce`), guaranteeing storage preservation across pod rescheduling and node maintenance.

---

## Technical Context

**Language/Version**: Go 1.26+ (pure Go, zero CGo, static compilation `CGO_ENABLED=0`).

**Primary Dependencies**: Standard library (`os`, `sync`, `hash/crc32`, `encoding/binary`), Protocol Buffers `google.golang.org/protobuf`, Kafka client `github.com/twmb/franz-go`, and Prometheus `github.com/prometheus/client_golang`.

**Storage**: Embedded on-disk LSM-tree storage engine with Write-Ahead Logging (`wal-*.log`), immutable SSTables (`*.sst`), and serialized catalog index (`catalog.bin`), mounted via Kubernetes PersistentVolumeClaim at `/var/lib/pulse/chronodb`.

**Testing**: Go unit tests with `-race`, crash & replay invariance integration tests (`TestDurableStorage_CrashAndRecoveryInvariance`), and Helm template linting.

**Target Platform**: Linux containers (`gcr.io/distroless/static-debian12:nonroot`, UID `65532:65532`) and Kubernetes 1.28+.

**Project Type**: Embedded storage engine daemon (`chronod`) and Kubernetes Helm StatefulSet chart.

**Performance Goals**: $< 5\text{ms}$ write latency with synchronous WAL `fsync`, $< 5\text{s}$ crash recovery time for 1,000,000 records, and $< 2\text{s}$ p95 query latency across 24-hour ranges during active compaction.

**Constraints**: Zero data loss across `SIGKILL`, zero CGo dependencies, $< 50\text{MB}$ distroless container image footprint, and strict adherence to Pulse Constitution Principles I, III, and V.

**Scale/Scope**: 16 deterministic device hash shards per node, scaling up to 100,000 active devices with configurable 14-day retention TTL.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Compliance Status | Analysis & Evidence |
|---|:---:|---|
| **I. Partition-Local State & Zero-Lock Ingestion** | ✅ PASS | ChronoDB is downstream of stream processing. Sharding is partitioned strictly by device ID hash across 16 independent disk directories; ingestion hot path remains 100% lock-free. |
| **II. Event-Time Watermarking** | ✅ PASS | Only closed window aggregates emitted past event-time watermarks are consumed and persisted to disk. |
| **III. Idempotent Ingestion & At-Least-Once Delivery** | ✅ PASS | Writes append to WAL and call `fsync` *before* consumer offsets are committed to Redpanda. Replayed duplicates overwrite identical composite keys `(device_id, metric, window_start, window_end)` without variance. |
| **IV. Decoupled Asynchronous Side-Effects** | ✅ PASS | Storage engine writes run in isolated consumer worker pool; downstream storage stalls propagate backpressure via Kafka consumer lag rather than blocking stream processing or alerting. |
| **V. Explicit Polyglot Storage Boundary** | ✅ PASS | Implements existing `ChronoEngine` Go interface; preserves thin boundary for future C++ ChronoDB process binding via gRPC/IPC. |

---

## Project Structure

### Documentation (this feature)

```text
specs/008-chronodb-storage-durability/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output: Storage engine design, WAL framing & recovery protocol
├── data-model.md        # Phase 1 output: On-disk directory hierarchy, WAL framing, SSTables
├── quickstart.md        # Phase 1 output: Local testing, crash recovery validation, Helm StatefulSet
├── contracts/           # Phase 1 output: ChronoEngine, WALManager, and StatefulSet contracts
│   ├── engine-contract.md
│   └── statefulset-contract.md
├── checklists/
│   └── requirements.md  # Specification quality checklist
└── tasks.md             # Phase 2 output (/speckit-tasks command - generated next)
```

### Source Code (repository root)

```text
cmd/chronod/
└── main.go                                  # Storage daemon CLI entrypoint & config parsing

internal/storage/
├── config/
│   └── config.go                            # Added DataDir, RetentionDays, CompactionInterval
├── consumer/
│   ├── sink.go                              # Batch sink invoking durable PutAggregates & offset commits
│   └── sink_test.go                         # Consumer batch flushing tests
├── engine/
│   ├── engine.go                            # ChronoEngine interface & composite key formatting
│   ├── memory.go                            # Existing in-memory engine (retained for testing/fallback)
│   ├── durable.go                           # NEW: Embedded LSM durable storage engine
│   ├── wal.go                               # NEW: Binary Write-Ahead Log manager with CRC32 & fsync
│   ├── sstable.go                           # NEW: Immutable SSTable writer, reader & sparse index
│   ├── compaction.go                        # NEW: Background multi-way merge compaction & TTL eviction
│   ├── recovery.go                          # NEW: Crash recovery protocol replaying WAL segments
│   ├── catalog.go                           # Persistent device catalog index
│   └── durable_test.go                      # Unit tests for WAL, SSTables, recovery, and compaction
└── server/
    ├── http.go                              # REST query & /healthz disk space probe
    └── grpc.go                              # gRPC ChronoService implementation

deployments/helm/pulse/
├── values.yaml                              # Updated with chronod.persistence parameters
└── templates/
    ├── chronod-statefulset.yaml             # NEW: StatefulSet with volumeClaimTemplates
    ├── chronod-headless-service.yaml        # NEW: Headless service for pod DNS resolution
    └── chronod-service.yaml                 # ClusterIP service for query routing

tests/integration/
└── durable_storage_test.go                  # Integration tests for SIGKILL crash & replay invariance
```

**Structure Decision**: The durable LSM engine seamlessly extends `internal/storage/engine/` under the existing `ChronoEngine` interface contract. Kubernetes deployment updates transition `chronod` into a `StatefulSet` with dedicated PVCs.

---

## Complexity Tracking

> **No Constitution violations detected.** All architectural gates passed cleanly.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| *None* | N/A | N/A |
