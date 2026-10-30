# Implementation Plan: Milestone 010 — Industrial Edge Bridge & Complex Event Processing (CEP) Engine

**Branch**: `010-edge-bridge-cep` | **Date**: 2026-10-30 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/010-edge-bridge-cep/spec.md`

---

## Summary

Implement the Industrial Edge Protocol Bridge (`pulse-bridge`) and the in-memory Complex Event Processing (CEP) Engine in `pulse-processor` conforming strictly to Constitution Principles I–V:
1. **Industrial Edge Bridge Daemon (`pulse-bridge`)**: Standalone, lightweight Go edge daemon deployable on factory IPCs and edge gateways. Ingests MQTT v3.1.1/v5.0, Sparkplug B, and Modbus TCP telemetry; decodes diverse payloads into canonical Protobuf `TelemetryReading` structures; and streams them securely to `pulse-gateway` over mTLS gRPC (`:50051`).
2. **Store-and-Forward Offline Edge Buffering**: Resilient dual-tier buffer (in-memory ring buffer + append-only disk FIFO log) preserving 100% of telemetry during edge WAN disconnects, flushing in chronological event-time order upon reconnection to maintain deterministic watermark progression.
3. **Partition-Local Complex Event Processing (CEP)**: Zero-lock stateful pattern matching running directly inside `pulse-processor` worker goroutines (Constitution Principle I). Evaluates multi-stage temporal sequences (`EventA FOLLOWED_BY EventB WITHIN ΔT`) and cross-metric arithmetic formulas (e.g. `power_mw / fuel_flow < 0.70`).
4. **Watermark-Governed Expiration**: Partially matched sequence candidate states expire deterministically as partition watermarks advance past window deadlines (Constitution Principle II).
5. **Composite Incident Synthesis & Live Rule Reloading**: Collapses cascading multi-sensor alarms into single root-cause `CompositeIncidentEvent` notifications sent to `alertd` (Constitution Principle IV); supports dynamic rule updates via atomic registry swaps without stream processor downtime.

---

## Technical Context

**Language/Version**: Go 1.26+ (pure Go, zero CGo, static compilation `CGO_ENABLED=0`).

**Primary Dependencies**: Standard library (`sync`, `sync/atomic`, `os`, `path/filepath`, `encoding/json`, `encoding/binary`), Google gRPC (`google.golang.org/grpc`, `credentials`), Protobuf (`google.golang.org/protobuf`), and Prometheus (`github.com/prometheus/client_golang`).

**Storage**: Local append-only disk FIFO spool (`/var/lib/pulse/bridge/` or `/tmp/pulse-bridge-spool/`) for offline edge buffering; partition-local in-memory candidate tables for CEP state.

**Testing**: Unit tests with `-race`, offline buffer fault injection tests, sequence matcher event-time ordering tests, cross-metric formula evaluation tests, and full end-to-end multi-service pipeline integration tests.

**Target Platform**: Linux edge gateways / factory servers (x86_64, ARM64), distroless containers (`gcr.io/distroless/static-debian12:nonroot`, UID `65532:65532`), and Kubernetes 1.28+.

**Project Type**: Standalone edge binary (`cmd/bridge`), internal streaming analytics engine (`internal/processor/cep`), and shared protocol contracts.

**Performance Goals**:
- `pulse-bridge`: $\ge 25,000\text{ readings/sec}$ protocol normalization and gRPC streaming throughput with $< 2\text{ms}$ bridge latency overhead.
- CEP Engine: Evaluate sequence patterns across $50,000\text{ events/sec}$ with $< 0.1\text{ms}$ per-event processing overhead and zero cross-goroutine lock contention.
- Offline Buffer: Zero data loss during up to 1 hour of factory LAN network partitioning.

**Constraints**:
- Zero-lock hot-path stream processing (Constitution Principle I).
- Event-time watermark progression without wall-clock dependency (Constitution Principle II).
- Zero CGo dependencies (`CGO_ENABLED=0`).
- Completely on-premises / air-gapped; zero external cloud vendor dependencies.

**Scale/Scope**: Supporting hundreds of edge bridge instances connecting to thousands of factory PLCs and sensors, streaming into 12-partition stream processing pipelines.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Compliance Status | Analysis & Evidence |
|---|:---:|---|
| **I. Partition-Local State & Zero-Lock Ingestion** | ✅ PASS | The CEP engine executes strictly inside individual partition worker goroutines. Because telemetry is partitioned by `device_id` via Murmur2 hashing, all correlated metrics for an asset arrive at the exact same worker. Candidate state is partition-local with zero mutex contention on the hot path. |
| **II. Event-Time Watermarking** | ✅ PASS | Temporal sequences (`EventA FOLLOWED_BY EventB WITHIN ΔT`) evaluate event timestamps (`timestamp_ns`). Unmatched candidate states are expired deterministically when the partition event-time watermark advances past the window deadline. |
| **III. Idempotent Ingestion & At-Least-Once Delivery** | ✅ PASS | Offline buffer backfill replays records into the gateway with their original event timestamps and deterministic composite primary keys. Replayed batches maintain idempotent upserts in ChronoDB. |
| **IV. Decoupled Asynchronous Side-Effects** | ✅ PASS | When composite incidents trigger, the CEP engine produces alert events to Redpanda topic `telemetry.alerts`. Outbound dispatching and webhook latency remain fully isolated in `alertd`. |
| **V. Explicit Polyglot Storage Boundary** | ✅ PASS | Edge bridge interacts exclusively with `pulse-gateway` across standard Protobuf gRPC interfaces; CEP results flow through standard Redpanda topics without bypassing architectural boundaries. |

---

## Project Structure

### Documentation (this feature)

```text
specs/010-edge-bridge-cep/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output: Protocol decoding, disk FIFO spooling, and NFA pattern matching
├── data-model.md        # Phase 1 output: EdgeBridgeConfig, SpoolManifest, CEPRule, SequenceCandidate
├── quickstart.md        # Phase 1 output: Local testing guide for bridge and CEP rules
├── contracts/           # Phase 1 output: Interface definitions
│   ├── bridge-contract.md
│   └── cep-contract.md
├── checklists/
│   └── requirements.md  # Quality checklist
└── tasks.md             # Phase 2 output (/speckit-tasks command - generated next)
```

### Source Code Layout

```text
cmd/bridge/
└── main.go              # Entrypoint for standalone pulse-bridge daemon

internal/bridge/
├── config/
│   └── config.go        # EdgeBridgeConfig parser and environment loading
├── adapter/
│   ├── adapter.go       # ProtocolAdapter interface definition
│   ├── mqtt.go          # MQTT v3/v5 client driver and JSON topic extractor
│   ├── sparkplug.go     # Sparkplug B Protobuf payload decoder
│   └── modbus.go        # Modbus TCP cyclic polling scheduler and register decoder
├── spooler/
│   ├── spooler.go       # Disk-backed FIFO ring buffer implementation
│   └── manifest.go      # Binary framing (PUBR) and manifest index tracker
├── forwarder/
│   └── forwarder.go     # Authenticated mTLS gRPC streaming client to pulse-gateway
├── metrics/
│   └── metrics.go       # Edge bridge Prometheus metrics
└── bridge_test.go       # Unit & integration tests for protocol drivers and spooler

internal/processor/cep/
├── engine.go            # Partition-local CEPEngine and NFA candidate state evaluation
├── rule.go              # CEPRule models and RuleCompiler parser
├── expression.go        # Cross-metric arithmetic expression evaluator
├── candidate.go         # In-flight SequenceCandidate lifecycle and watermark eviction
├── synthesizer.go       # CompositeIncidentEvent synthesis and alert formatting
├── reload.go            # Atomic rule registry swap and admin reload handler
└── cep_test.go          # Unit & property tests for temporal sequences and expressions

internal/processor/consumer/
└── consumer.go          # Integrated CEPEngine into partition worker loop
```
