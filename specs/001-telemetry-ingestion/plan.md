# Implementation Plan: Telemetry Ingestion and Ingestion Gateway

**Branch**: `001-telemetry-ingestion` | **Date**: 2026-10-20 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-telemetry-ingestion/spec.md`

## Summary

Build the foundational ingestion tier for the Pulse platform. This milestone establishes a local single-node Redpanda messaging cluster pre-configured with 12 partitions on the `telemetry.raw` topic, defines the binary Protobuf v3 schema for industrial telemetry, and deploys a high-throughput Go Ingestion Gateway exposing concurrent gRPC and HTTP/JSON endpoints. The gateway strictly validates payloads (guarding against clock skew and malformed data) and partitions events to Redpanda using `device_id` as the partition key.

## Technical Context

**Language/Version**: Go 1.23+

**Primary Dependencies**:
- `github.com/twmb/franz-go` (v1.17+): Pure Go Kafka client with zero CGo and high-throughput memory pooling.
- `google.golang.org/grpc` & `google.golang.org/protobuf`: High-performance binary RPC and serialization.
- `github.com/prometheus/client_golang`: Native metrics exporter for latency histograms and counters.

**Storage**: Redpanda (Kafka-compatible message log, topic: `telemetry.raw`, 12 partitions).

**Testing**: Standard Go `testing`, `testify/assert`, and Go benchmarking tools (`go test -bench`).

**Target Platform**: Linux / macOS server (Docker containerized or local binary).

**Project Type**: High-throughput streaming ingestion web & RPC service.

**Performance Goals**:
- Sustain $\ge 50,000\text{ events/sec}$ throughput under concurrent load.
- $p99$ acknowledgement latency $< 10\text{ ms}$.

**Constraints**:
- Strictly partition by `device_id` into Redpanda (Constitution Principle I).
- Reject clock skew $> 5$ minutes into the future to preserve downstream watermark integrity (Constitution Principle II).
- Bounded memory usage via connection limits and buffer pooling.

**Scale/Scope**: Milestone 001 covers ingestion, serialization, schema validation, and broker dispatching.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle / Gate | Compliance Analysis | Status |
| :--- | :--- | :---: |
| **I. Partition-Local State & Zero-Lock Ingestion** | Gateway sets `record.Key = []byte(reading.DeviceId)`. Franz-go Murmur2 hash partitioner deterministically routes all events for a device to the same partition. Ingestion gateway is stateless with no locks on the hot path. | **PASS** |
| **II. Event-Time Watermarking** | Gateway validates event timestamps and enforces clock-skew guardrails (max 5m future drift, max 24h past buffer), shielding downstream watermark engines from corrupted timestamps. | **PASS** |
| **III. Idempotent Ingestion & Delivery** | Prepares binary Protobuf payload preserving original device event-time nanoseconds (`timestamp_ns`) so downstream consumers can compute idempotent upsert keys. | **PASS** |
| **IV. Decoupled Asynchronous Side-Effects** | Gateway contains zero downstream side-effects; all stream processing, anomaly detection, and alert dispatching are deferred to Milestone 002 & 003. | **PASS** |
| **V. Polyglot Storage Boundary** | Milestone 001 writes to Redpanda. Direct ChronoDB integration is isolated behind the stream processor adapter in Milestone 004. | **PASS** |

---

## Project Structure

### Documentation (this feature)

```text
specs/001-telemetry-ingestion/
├── spec.md                  # Feature Specification
├── plan.md                  # This file (Implementation Plan)
├── research.md              # Phase 0 Tech Decisions & Rationale
├── data-model.md            # Phase 1 Domain Entities & Schemas
├── quickstart.md            # Phase 1 Validation & Run Guide
├── contracts/
│   ├── telemetry.proto      # Protobuf v3 gRPC Service & Message Definitions
│   └── http-api.md          # REST API & Observability Contracts
└── checklists/
    └── requirements.md      # Specification Quality Checklist
```

### Source Code (repository root layout)

```text
pulse/
├── cmd/
│   └── gateway/
│       └── main.go                 # Gateway binary entry point (HTTP & gRPC)
├── proto/
│   └── v1/
│       ├── telemetry.pb.go         # Generated Protobuf message bindings
│       └── telemetry_grpc.pb.go    # Generated gRPC server & client stubs
├── internal/
│   └── gateway/
│       ├── config/
│       │   └── config.go           # Environment variables & flags loader
│       ├── validator/
│       │   ├── validator.go        # Schema, clock-skew, and numeric validator
│       │   └── validator_test.go   # Unit tests for validation edge cases
│       ├── producer/
│       │   ├── producer.go         # Franz-go Kafka producer with device_id partition key
│       │   └── producer_test.go    # Producer unit & mock tests
│       ├── service/
│       │   └── ingestion.go        # Unified ingestion service business logic
│       ├── server/
│       │   ├── http.go             # HTTP/JSON server (/api/v1/telemetry, /healthz, /metrics)
│       │   └── grpc.go             # gRPC IngestionService implementation
│       └── metrics/
│           └── metrics.go          # Prometheus metrics instrumentation
├── deployments/
│   └── docker-compose.yml          # Redpanda single-node container + topic provisioning
├── tests/
│   ├── integration/
│   │   └── gateway_test.go         # End-to-end integration test with Redpanda
│   └── bench/
│       └── throughput_test.go      # Concurrent 50,000 events/sec benchmark
├── go.mod
├── go.sum
└── Makefile                        # Build, proto gen, run, and test targets
```

**Structure Decision**: Standard Go service layout using `cmd/` for entrypoints, `internal/` for non-exported business logic, `proto/` for generated protobufs, and `deployments/` for Docker orchestration.

---

## Complexity Tracking

> **Constitution Check: 0 violations. All principles satisfied.**

| Violation | Why Needed | Simpler Alternative Rejected Because |
| :--- | :--- | :--- |
| *None* | Architecture strictly complies with Constitution Principles I–V | N/A |
