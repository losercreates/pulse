# Implementation Plan: Stream Processing Engine and Event-Time Watermarking

**Branch**: `002-stream-processor-core` | **Date**: 2026-10-21 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/002-stream-processor-core/spec.md`

## Summary

Build the core stream-processing engine for Pulse. This engine consumes the partitioned `telemetry.raw` message stream from Redpanda, assigning exactly one worker goroutine per partition with partition-local memory and zero cross-thread locking on the hot processing path. It implements monotonic event-time watermarking (`max_event_time - allowed_lateness`) with an idle partition timeout defense, maintains an $O(1)$ priority queue of active tumbling and sliding windows, computes single-pass running statistics with HyperLogLog cardinality sketches, routes late data beyond the allowed lateness bound to a dead-letter sink (`telemetry.late`), and ensures at-least-once idempotent recovery.

## Technical Context

**Language/Version**: Go 1.23+

**Primary Dependencies**:
- `github.com/twmb/franz-go` (v1.22+): Partition consumer group with cooperative sticky rebalance protocol.
- `google.golang.org/protobuf`: Protobuf serialization for incoming readings and outgoing aggregates.
- `github.com/prometheus/client_golang`: Streaming metrics exporter for watermark lag, open window count, and late data rate.

**Storage**: Redpanda (Topics: `telemetry.raw` [source], `telemetry.aggregates` [sink], `telemetry.late` [dead-letter]).

**Testing**: Standard Go `testing`, `testify/assert`, property-based tests for out-of-order invariance, and Go benchmark suites.

**Target Platform**: Linux / macOS server.

**Project Type**: Stateful real-time streaming engine.

**Performance Goals**:
- Sustain $\ge 50,000\text{ events/sec}$ throughput across 12 partitions.
- Window-close-to-emission latency $p95 < 50\text{ ms}$.

**Constraints**:
- Strictly partition-local state (Constitution Principle I).
- Zero wall-clock window closures; closures governed strictly by event-time watermarks (Constitution Principle II).
- Immutability of closed windows: late arrivals routed to dead-letter sink without mutating closed aggregates (Constitution Principle II).
- Offset commit coordinated with window emission for idempotent recovery (Constitution Principle III).

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle / Gate | Compliance Analysis | Status |
| :--- | :--- | :---: |
| **I. Partition-Local State & Zero-Lock Hot Path** | Exactly one worker goroutine per assigned Kafka/Redpanda partition. Window accumulators, watermark trackers, and min-heap priority queues are partition-local with zero cross-goroutine mutexes on record ingestion. | **PASS** |
| **II. Event-Time Watermarking** | Windows are closed strictly when partition event-time watermark advances past `window_end`. Late events ($< \text{watermark}$) are diverted to `telemetry.late` dead-letter sink, preserving closed window immutability. | **PASS** |
| **III. Idempotent Ingestion & Delivery** | Offset commit happens after window aggregate emission. Deterministic primary key `(device_id, metric, window_start, window_end)` ensures idempotent downstream upserts. | **PASS** |
| **IV. Decoupled Asynchronous Side-Effects** | Stream processor emits clean aggregates to `telemetry.aggregates`. Statistical anomaly detection and alert dispatching remain isolated in Milestone 003. | **PASS** |
| **V. Polyglot Storage Boundary** | Milestone 002 writes aggregates to Redpanda topic `telemetry.aggregates`. Direct ChronoDB C++ storage boundary is deferred to Milestone 004. | **PASS** |

---

## Project Structure

### Documentation (this feature)

```text
specs/002-stream-processor-core/
├── spec.md                  # Feature Specification
├── plan.md                  # This file (Implementation Plan)
├── research.md              # Phase 0 Tech Decisions & Rationale
├── data-model.md            # Phase 1 Domain Entities & Schemas
├── quickstart.md            # Phase 1 Validation & Run Guide
├── contracts/
│   └── aggregates.proto     # Protobuf v3 WindowAggregate and LateTelemetryRecord
└── checklists/
    └── requirements.md      # Specification Quality Checklist
```

### Source Code (repository root layout)

```text
pulse/
├── cmd/
│   ├── gateway/main.go
│   └── processor/
│       └── main.go                 # Stream processor application entry point
├── proto/
│   └── v1/
│       ├── telemetry.pb.go
│       ├── telemetry_grpc.pb.go
│       └── aggregates.pb.go        # Generated from aggregates.proto
├── internal/
│   ├── processor/
│   │   ├── config/
│   │   │   └── config.go           # Allowed lateness, window configs, topics
│   │   ├── watermark/
│   │   │   ├── watermark.go        # Watermark tracker, clock, idle timeout
│   │   │   └── watermark_test.go   # Watermark progression & out-of-order tests
│   │   ├── window/
│   │   │   ├── window.go           # Tumbling/sliding boundary calculations
│   │   │   ├── priority_queue.go   # Min-heap priority queue for open windows
│   │   │   ├── accumulator.go      # Single-pass stats (count, sum, min, max, avg, HLL)
│   │   │   └── window_test.go      # Aggregator & priority queue unit tests
│   │   ├── consumer/
│   │   │   ├── consumer.go         # 1 goroutine per partition worker loop
│   │   │   └── consumer_test.go    # Partition consumer lifecycle tests
│   │   ├── latesink/
│   │   │   └── latesink.go         # Dead-letter dispatcher to telemetry.late
│   │   └── metrics/
│   │       └── metrics.go          # Streaming Prometheus metrics instrumentation
│   └── gateway/ ...
├── deployments/
│   └── docker-compose.yml          # Updated with telemetry.aggregates and telemetry.late
├── tests/
│   ├── property/
│   │   └── order_invariance_test.go # Property test: aggregate invariance to arrival order
│   └── bench/
│       └── processor_bench_test.go # Stream processor throughput benchmark
```

**Structure Decision**: Adds `cmd/processor` binary entrypoint and `internal/processor/` packages (watermark, window, consumer, latesink, metrics) alongside the existing gateway.

---

## Complexity Tracking

> **Constitution Check: 0 violations. All principles satisfied.**

| Violation | Why Needed | Simpler Alternative Rejected Because |
| :--- | :--- | :--- |
| *None* | Architecture strictly complies with Constitution Principles I–V | N/A |
