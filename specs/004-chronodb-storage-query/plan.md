# Implementation Plan: ChronoDB Polyglot Storage Sink & Historical Query Engine

**Branch**: `004-chronodb-storage-query` | **Date**: 2026-10-23 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/004-chronodb-storage-query/spec.md`

---

## Summary

Build the ChronoDB polyglot storage sink and historical telemetry query engine for the Pulse platform. The subsystem ingests closed window aggregates from Redpanda topic `telemetry.aggregates` (12 partitions) via a dedicated Franz-go consumer group, connects to ChronoDB instances across a defined gRPC polyglot boundary sharded deterministically by device identifier, and enforces 100% idempotent upserts keyed by `(device_id, metric, window_start, window_end)` (Constitution Principles III & V). Storage write backpressure propagates naturally via consumer lag without unbounded memory growth. A dual-transport query interface (HTTP/JSON on `:8084` and gRPC on `:50054`) serves raw time-range interval lookups, dynamic downsampled rollups (e.g. 1m, 5m, 1h), and active device/metric catalogs with sub-50ms p95 latencies.

---

## Technical Context

**Language/Version**: Go 1.23+

**Primary Dependencies**:
- `github.com/twmb/franz-go` (v1.22+): Kafka consumer & producer client for `telemetry.aggregates`.
- `google.golang.org/protobuf`: Protobuf v3 serialization for `WindowAggregate` and `chronodb.proto`.
- `google.golang.org/grpc`: High-performance RPC polyglot process boundary for ChronoDB storage engine.
- `github.com/prometheus/client_golang`: Streaming metrics exporter for write throughput, query latencies, and consumer lag.
- Standard `net/http`: REST API query engine.

**Storage**:
- Redpanda topic `telemetry.aggregates` (12 partitions).
- ChronoDB sharded time-series storage engine (in-memory columnar store + WAL with gRPC service boundary).

**Testing**: Standard Go `testing`, `testify/assert`, idempotency replay chaos tests, property-based query tests, and throughput benchmarks.

**Target Platform**: Linux / macOS server.

**Project Type**: Polyglot time-series storage sink & historical analytical query service.

**Performance Goals**:
- Storage sink sustains $\ge 50,000\text{ aggregate writes/sec}$ under continuous load.
- p95 time-range query latency $< 50\text{ ms}$ for 24-hour single-device window queries.
- Dynamic analytical downsampled rollups (7-day window) complete in $< 200\text{ ms}$.
- Storage stalls/flushes propagate backpressure into Redpanda consumer lag within 2 seconds.

**Constraints**:
- Strict idempotent upsert semantics on `(device_id, metric, window_start, window_end)` (Constitution Principle III).
- Explicit polyglot boundary with device ID range/hash sharding (Constitution Principle V).
- Storage write pauses must never crash or allocate unbounded memory.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle / Gate | Compliance Analysis | Status |
| :--- | :--- | :---: |
| **I. Partition-Local State & Zero-Lock Ingestion Hot Path** | Storage sink consumes independently from `telemetry.aggregates` without touching the gateway or stream processor partition workers. | **PASS** |
| **II. Event-Time Watermarking** | Stored records are immutable closed windows governed by stream processor watermarks; storage engine never closes windows using wall-clock time. | **PASS** |
| **III. Idempotent Ingestion & At-Least-Once Delivery** | **Directly enforced**. Storage engine indexes records by `(device_id, metric_name, window_start_ns, window_end_ns)`. Offset commits to Redpanda are issued only after durable storage fsync. Replay of identical aggregates yields identical state with zero duplicate counts or sums. | **PASS** |
| **IV. Decoupled Asynchronous Side-Effects** | Storage writes are completely decoupled from alert dispatching and ingestion gateway. | **PASS** |
| **V. Explicit Polyglot Storage Boundary** | **Directly enforced**. ChronoDB is accessed across a defined gRPC boundary sharded deterministically by device ID hash. Storage write backpressure propagates naturally via consumer lag. | **PASS** |

---

## Project Structure

### Documentation (this feature)

```text
specs/004-chronodb-storage-query/
├── spec.md                  # Feature Specification
├── plan.md                  # This file (Implementation Plan)
├── research.md              # Phase 0 Architectural Decisions & Polyglot Analysis
├── data-model.md            # Phase 1 Domain Entities & Storage Sharding Schemas
├── quickstart.md            # Phase 1 Validation & End-to-End Verification Guide
├── contracts/
│   ├── chronodb.proto       # Protobuf v3 ChronoDB Service & Query Contract
│   └── query-api.md         # REST HTTP/JSON Query API Specification
└── checklists/
    └── requirements.md      # Specification Quality Checklist (16/16 verified)
```

### Source Code (repository root layout)

```text
pulse/
├── cmd/
│   ├── gateway/main.go
│   ├── processor/main.go
│   ├── alertd/main.go
│   └── chronod/main.go             # ChronoDB Storage Sink & Query Service Daemon
├── proto/
│   └── v1/
│       ├── telemetry.pb.go
│       ├── aggregates.pb.go
│       ├── alerts.pb.go
│       └── chronodb.pb.go          # Generated from contracts/chronodb.proto
├── internal/
│   ├── storage/
│   │   ├── config/
│   │   │   └── config.go           # Broker, ports (:8084, :50054), batch sizes, shard count
│   │   ├── engine/
│   │   │   ├── engine.go           # Columnar time-series engine interface
│   │   │   ├── memory.go           # In-memory sharded columnar engine with composite key index
│   │   │   ├── rollup.go           # Downsampled dynamic rollup & aggregation math
│   │   │   └── engine_test.go      # Idempotency and rollup unit tests
│   │   ├── consumer/
│   │   │   ├── sink.go             # Franz-go consumer loop on telemetry.aggregates
│   │   │   └── sink_test.go        # Batch ingestion and backpressure tests
│   │   ├── server/
│   │   │   ├── http.go             # REST API router (/api/v1/query, /rollup, /devices, /metrics)
│   │   │   ├── grpc.go             # ChronoDBService gRPC implementation
│   │   │   └── server_test.go      # API integration tests
│   │   └── metrics/
│   │       └── metrics.go          # Prometheus metrics (write rate, query latency, consumer lag)
├── deployments/
│   └── docker-compose.yml          # Topic telemetry.aggregates (12 partitions) verified
└── tests/
    ├── integration/
    │   └── idempotent_storage_test.go # Chaos test: crash/rebalance replay produces zero duplicates
    └── bench/
        └── storage_bench_test.go      # Benchmark for aggregate ingestion and query throughput
```

---

## Complexity Tracking

> **Constitution Check: 0 violations. All principles strictly satisfied.**

| Principle | Justification / Architectural Choice |
| :--- | :--- |
| **Principle III (Idempotency)** | Natural composite primary key `device_id#metric_name#start_ns#end_ns` ensures deterministic deduplication under consumer replay. |
| **Principle V (Polyglot Boundary)** | gRPC boundary provides process isolation, clean multi-shard network routing, and avoids CGo cross-compilation complications. |
