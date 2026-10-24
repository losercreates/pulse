# Implementation Plan: Milestone 005 — High-Throughput Device Fleet Load Simulator & Chaos Harness

**Branch**: `005-fleet-load-simulator` | **Date**: 2026-10-24 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/005-fleet-load-simulator/spec.md`

---

## Summary

Build a high-performance, standalone device fleet simulator and chaos load-harness CLI (`cmd/simulator`) capable of instantiating up to $10,000+$ distinct industrial IoT devices (turbines, induction motors, hydraulic pumps, generators) emitting correlated multi-metric telemetry (`vibration`, `bearing_temp`, `rotor_rpm`, `power_mw`, `oil_pressure`) at rates exceeding $50,000\text{ events/sec}$. The simulator supports dual transports (multiplexed client-side gRPC streaming on `:50051` and HTTP/JSON batching on `:8080`), implements 4 distinct traffic profiles (**steady-state**, **bursty spikes**, **offline-recovery backfills**, and **sensor anomaly chaos**), and runs active canary latency probes against ChronoDB (`:8084`) to mathematically verify the platform's Constitution SLA mandates ($\ge 50,000\text{ events/sec}$, window-close-to-query $\text{p95} < 2\text{s}$, consumer lag $< 5\text{s}$).

---

## Technical Context

**Language/Version**: Go 1.23+ (consistent with Pulse runtime)  
**Primary Dependencies**: `google.golang.org/grpc`, `proto/v1` (Pulse Protobuf contracts), standard library (`net/http`, `sync`, `sync/atomic`, `math/rand/v2`)  
**Storage**: N/A (Load generator client; queries ChronoDB HTTP REST endpoint `:8084` for end-to-end latency verification)  
**Testing**: Go standard `testing`, `testify/assert`, `testify/require`, and throughput benchmarks in `tests/bench/`  
**Target Platform**: Linux (Ubuntu, WSL2, GitHub Actions CI)  
**Project Type**: CLI executable (`cmd/simulator`) and internal simulation library (`internal/simulator/...`)  
**Performance Goals**:
- Sustain $\ge 50,000\text{ events/sec}$ client generation throughput without dropped packets
- Scale smoothly to $10,000$ concurrent simulated devices
- Memory footprint bounded $< 200\text{ MB}$ RSS under full load
- End-to-end canary latency verification with p95 $< 2,000\text{ ms}$
**Constraints**:
- Must not exhaust OS ephemeral ports or TCP sockets (requires connection pooling and gRPC HTTP/2 stream multiplexing)
- Generated timestamps must conform to Gateway validation bounds ($[-24\text{h}, +5\text{m}]$ relative to host time)
- Zero external third-party heavy dependencies; compile into a single static binary `bin/simulator`
**Scale/Scope**: 1 CLI entrypoint, 10 internal packages, 1 comprehensive benchmark suite

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle / Gate | Compliance Analysis | Status |
| :--- | :--- | :---: |
| **I. Partition-Local State & Zero-Lock Ingestion** | Simulator assigns stable, deterministic device identifiers (`turbine-XXXX`, `motor-XXXX`, `pump-XXXX`) that hash uniformly across Redpanda's 12 partitions via Gateway Murmur2 partitioning, preserving partition-local per-device event ordering. | **PASS** |
| **II. Event-Time Watermarking** | Offline recovery chaos profile deliberately pauses devices, accumulates 30–60s of buffered readings, and bursts out-of-order backfill into the pipeline to verify that monotonic watermarks reject late events to `telemetry.late` without mutating closed windows. | **PASS** |
| **III. Idempotent Ingestion** | Canary latency probes verify that window aggregates are persisted without missing or duplicated data. | **PASS** |
| **IV. Decoupled Asynchronous Side-Effects** | Anomaly chaos profile deliberately injects $z \ge 3.0$ spikes and flatline sequences to verify real-time statistical baseline evaluation and decoupled alert dispatching without pipeline stalls. | **PASS** |
| **V. Explicit Polyglot Storage Boundary** | Bursty traffic profile stress-tests storage backpressure propagation into Redpanda consumer lag. | **PASS** |
| **Performance Target § Throughput** | Direct client generation and measurement of sustained $\ge 50,000\text{ events/sec}$. | **PASS** |
| **Performance Target § Latency** | Synthetic canary probes sample and verify window-close-to-query p95 latency $< 2.0\text{s}$. | **PASS** |

---

## Project Structure

### Documentation (this feature)

```text
specs/005-fleet-load-simulator/
├── spec.md                  # Feature Specification (completed)
├── checklists/
│   └── requirements.md      # Spec Quality Checklist (16/16 PASS)
├── plan.md                  # This Implementation Plan (/speckit-plan)
├── research.md              # Phase 0 Research Decisions
├── data-model.md            # Phase 1 Data Model & State Machines
├── contracts/
│   ├── simulator-cli.md     # Phase 1 CLI Contract & Synopsis
│   └── report-schema.json   # Phase 1 JSON Report Contract Schema
└── quickstart.md            # Phase 1 Runnable Validation Guide
```

### Source Code (repository layout)

```text
cmd/
├── simulator/
│   └── main.go              # Standalone CLI entrypoint (flags, signal trapping, dashboard)

internal/
└── simulator/
    ├── config/
    │   └── config.go        # CLI flags and environment variable loader
    ├── fleet/
    │   ├── archetype.go     # Industrial device archetypes (turbine, motor, pump, generator)
    │   ├── fleet.go         # Virtual device topology manager and device state partitioner
    │   └── fleet_test.go    # Unit tests for fleet creation and device distribution
    ├── signal/
    │   ├── signal.go        # Physical sinusoidal + Gaussian white noise signal generator
    │   └── signal_test.go   # Signal distribution and boundary tests
    ├── pacing/
    │   ├── limiter.go       # High-precision token bucket rate limiter & micro-batcher
    │   └── limiter_test.go  # Pacing accuracy and throughput stability tests
    ├── transport/
    │   ├── client.go        # Transport client interface (Sender)
    │   ├── grpc.go          # Client-side gRPC streaming sender (:50051)
    │   └── http.go          # Pipelined HTTP/JSON batch sender (:8080)
    ├── chaos/
    │   ├── chaos.go         # Traffic profile coordinator (bursts, offline backfill, anomalies)
    │   └── chaos_test.go    # Unit tests for burst multipliers and anomaly generation
    ├── canary/
    │   └── probe.go         # End-to-end canary probe emitter & ChronoDB poller
    ├── metrics/
    │   └── tracker.go       # Lock-free atomic throughput, latency, and error counter
    └── report/
        └── summary.go       # Live terminal renderer and JSON report generator

tests/
└── bench/
    └── simulator_bench_test.go # Benchmark measuring local generation throughput (>100k events/sec)
```

**Structure Decision**: Standard Go project structure extending `cmd/` with the `simulator` executable and isolating load generation components inside `internal/simulator/` without leaking into production server modules.

---

## Complexity Tracking

> *No constitutional violations. All designs adhere to project standards.*

| Violation | Why Needed | Simpler Alternative Rejected Because |
| :--- | :--- | :--- |
| *None* | *N/A* | *N/A* |
