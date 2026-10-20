<!--
Sync Impact Report:
- Version change: initial template -> 1.0.0
- Bump Rationale: Initial ratification of the Pulse project constitution.
- Principles established:
  1. I. Partition-Local State & Zero-Lock Ingestion Hot Path
  2. II. Event-Time Watermarking (Zero Wall-Clock Closes)
  3. III. Idempotent Ingestion & At-Least-Once Delivery
  4. IV. Decoupled Asynchronous Side-Effects
  5. V. Explicit Polyglot Storage Boundary
- Added Sections:
  - Operational & Tech Constraints
  - Testing Standards & Quality Gates
- Removed Sections: None
- Deferred Items / Follow-up TODOs: None
-->

# Pulse — Real-Time Telemetry Ingestion & Stream Processing Platform Constitution

## Core Principles

### I. Partition-Local State & Zero-Lock Ingestion Hot Path
Devices are partitioned strictly by `device_id` across Redpanda topics to guarantee per-device
ordering. The concurrency model enforces exactly one goroutine per Kafka/Redpanda partition
consumer. Windowing state, watermark tracking, and anomaly detection stats MUST be partition-local.
Cross-goroutine locking on the message ingestion hot path is strictly prohibited.

### II. Event-Time Watermarking (Zero Wall-Clock Closes)
Window closures are governed strictly by event-time watermarks (`max_event_time - allowed_lateness`),
never by wall-clock arrival time. Open windows MUST be tracked in a close-time priority queue and
emitted deterministically when the watermark advances past `window_end`. Events arriving after window
closure (late data beyond allowed lateness) MUST NOT mutate closed windows or corrupt published
aggregates; they MUST be routed to a dedicated late-data sink with observability metrics.

### III. Idempotent Ingestion & At-Least-Once Delivery
Consumer offsets are committed to Redpanda only after window state and aggregates are safely checkpointed
and written. Writes to ChronoDB instances MUST be idempotent upserts keyed by
`(device_id, metric, window_start, window_end)`. Reprocessing after a consumer crash or rebalance
MUST recompute identical window results with zero double-counting or data loss.

### IV. Decoupled Asynchronous Side-Effects
Anomaly detection uses partition-local statistical EWMA and rolling standard deviation bands with
defensive guardrails (minimum variance threshold before alerting to prevent broken-sensor false-positive
storms). Alert dispatching (webhooks, notifications) MUST execute in an isolated worker pool with
exponential backoff retry. Downstream alert sinks or webhook outages MUST NEVER block, lag, or
backpressure stream ingestion.

### V. Explicit Polyglot Storage Boundary
Go is the standard language for Ingestion Gateway, Stream Processor, and Alert Dispatcher.
ChronoDB (C++) is accessed strictly across a defined polyglot boundary (thin CGo or gRPC process
boundary) sharded by device ID range. Storage write backpressure MUST propagate naturally via
Redpanda consumer lag rather than unbounded in-memory queuing.

## Operational & Tech Constraints

- **Language & Runtime**: Go 1.23+ for Ingestion Gateway & Stream Processor; C++ for ChronoDB storage
  engine boundary.
- **Broker**: Redpanda (Kafka v2+ API compatible) with ordered-per-key, replayable log semantics.
- **Schemas**: Protocol Buffers v3 with backwards/forwards schema compatibility and default-value handling.
- **Storage**: ChronoDB instances sharded by device ID ranges.
- **Performance Targets**:
  - Sustain >= 50,000 events/sec ingestion throughput.
  - Window-close-to-query p95 latency < 2s for 10-second tumbling windows.
  - Consumer lag bounded < 5s under sustained target load.
  - Bounded memory footprint with deterministic window eviction.

## Testing Standards & Quality Gates

- **Deterministic Unit Testing**: Watermark calculation, window closure priority queues, and EWMA
  banding must be validated with unit tests including out-of-order event streams.
- **Property-Based Testing**: Aggregation outputs must be mathematically invariant to event arrival
  order within the allowed lateness bound.
- **Chaos Testing**: Automated tests must repeatedly inject stream processor crashes mid-window to
  verify zero lost or double-counted aggregates under rebalance and recovery.
- **Fleet Simulation**: Integration tests must run against a simulated device fleet generating high-frequency,
  bursty, and offline-recovery profiles.

## Governance

This Constitution supersedes all informal practices across the repository. All subsequent Spec Kit
artifacts—including feature specifications (`/speckit-specify`), architectural plans (`/speckit-plan`),
and task lists (`/speckit-tasks`)—must explicitly verify compliance against these principles.

Any amendment or exemption to watermarking semantics, partition isolation, or storage idempotency
requires formal documentation, a semantic version bump, and an updated sync impact report.

**Version**: 1.0.0 | **Ratified**: 2026-10-20 | **Last Amended**: 2026-10-20
