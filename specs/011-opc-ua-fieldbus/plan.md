# Implementation Plan: Milestone 011 — Native OPC-UA Binary Fieldbus Adapter

**Branch**: `011-opc-ua-fieldbus` | **Date**: 2026-11-03 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/011-opc-ua-fieldbus/spec.md`

---

## Summary

Implement a native, pure Go binary OPC-UA fieldbus adapter for `pulse-bridge` conforming strictly to Constitution Principles I–V:
1. **Direct OPC-UA Binary Protocol (`opc.tcp://`)**: Pure Go client driver in `internal/bridge/adapter/opcua/` implementing binary framing (`HEL`/`ACK`, `OPN`, `MSG`), SecureChannel creation, Session management, and keep-alive heartbeats without external CGo or runtime C dependencies.
2. **Real-Time Monitored Item Subscriptions**: Server-side subscriptions (`CreateSubscription`, `CreateMonitoredItems`, and pipelined `PublishRequest` loops) streaming `DataChangeNotification` frames directly into the bridge's unified telemetry pipeline, eliminating wasteful cyclic polling.
3. **Dynamic Address Space Browsing**: Recursive `Browse` service with cycle detection and glob path filtering (e.g. `Objects/Turbine/*/*`) for automatic tag discovery and auto-subscription on startup.
4. **Data Normalization**: Translates OPC-UA primitive variants into canonical `TelemetryReading` Protobuf messages, preserving `SourceTimestamp` and capturing OPC-UA `StatusCode` flags.
5. **Store-and-Forward Spooler Integration**: Full decoupling via the bridge `Coordinator`, automatically redirecting incoming OPC-UA telemetry into the local append-only disk FIFO spooler (`PUBR` CRC32 framing) during upstream gateway disconnects, and flushing in strict chronological order upon reconnect.

---

## Technical Context

**Language/Version**: Go 1.26+ (pure Go, zero CGo, static compilation `CGO_ENABLED=0`).

**Primary Dependencies**: Standard library (`sync`, `sync/atomic`, `net`, `encoding/binary`, `crypto/tls`, `crypto/rsa`, `crypto/sha256`, `time`), Google gRPC (`google.golang.org/grpc`), Protobuf (`google.golang.org/protobuf`), and Prometheus (`github.com/prometheus/client_golang`).

**Storage**: Existing disk-backed FIFO spooler (`internal/bridge/spooler/`) with `PUBR` 16-byte binary framing and IEEE CRC32 checksums for offline edge buffering.

**Testing**: Unit tests with `-race`, mock OPC-UA binary server tests, handshake and framing tests, subscription notification decode tests, address space browsing tests, and gateway disconnect spooling tests.

**Target Platform**: Linux edge gateways / industrial IPCs (x86_64, ARM64), distroless containers (`gcr.io/distroless/static-debian12:nonroot`, UID `65532:65532`), and Kubernetes 1.28+.

**Project Type**: Edge daemon protocol adapter (`internal/bridge/adapter/opcua`), configuration parser, and metrics instrumentation.

**Performance Goals**:
- Sustain $\ge 25,000\text{ notifications/sec}$ processing throughput per bridge instance.
- Memory consumption $\le 100\text{MB}$ resident set size under 1,000 active monitored items.
- Sub-15ms notification-to-gateway dispatch latency.
- Sub-5s dynamic address space browsing across 5,000 nodes.

**Constraints**:
- Strict zero-CGo requirement (`CGO_ENABLED=0`).
- Completely on-premises / air-gapped; zero external cloud vendor dependencies.
- Re-use existing `spooler` and `forwarder` abstractions without breaking MQTT or Modbus adapters.

**Scale/Scope**: Industrial manufacturing plants, wind turbines, power generation facilities with dozens of OPC-UA servers and thousands of monitored sensors.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Compliance Status | Analysis & Evidence |
|---|:---:|---|
| **I. Partition-Local State & Zero-Lock Ingestion** | ✅ PASS | The OPC-UA adapter runs at the edge perimeter and emits canonical `TelemetryReading` records with `device_id`. Ingestion Gateway hashes `device_id` via Murmur2 to route to dedicated partition workers with zero lock contention. |
| **II. Event-Time Watermarking** | ✅ PASS | The adapter decodes the PLC's native `SourceTimestamp` into `timestamp_ns`. Stream Processor watermarks advance monotonically based on event timestamps, never wall clock. |
| **III. Idempotent Ingestion & At-Least-Once Delivery** | ✅ PASS | Offline disk spooler replays backfilled records with original timestamps and composite primary keys `(device_id, metric, window_start, window_end)` ensuring idempotent upserts in ChronoDB. |
| **IV. Decoupled Asynchronous Side-Effects** | ✅ PASS | Upstream webhook delays or alert dispatcher load have zero impact on edge bridge ingestion or OPC-UA subscription processing. |
| **V. Explicit Polyglot Storage Boundary** | ✅ PASS | Edge bridge streams strictly via gRPC Protobuf to `pulse-gateway`. ChronoDB storage engine boundary remains strictly isolated. |

---

## Project Structure

### Documentation (this feature)

```text
specs/011-opc-ua-fieldbus/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output: Binary protocol framing, subscriptions, browsing
├── data-model.md        # Phase 1 output: OPCUAAdapterConfig, MonitoredItem, SessionState
├── quickstart.md        # Phase 1 output: Local testing and verification guide
├── contracts/           # Phase 1 output: Wire and interface contracts
│   └── opcua-adapter-contract.md
├── checklists/
│   └── requirements.md  # Quality checklist
└── tasks.md             # Phase 2 output (/speckit-tasks command - generated next)
```

### Source Code Layout

```text
internal/bridge/
├── adapter/
│   ├── adapter.go          # ProtocolAdapter interface definition
│   ├── modbus.go           # Modbus TCP adapter
│   ├── mqtt.go             # MQTT adapter
│   ├── sparkplug.go        # Sparkplug B adapter
│   └── opcua/              # Native OPC-UA Binary Fieldbus Adapter
│       ├── adapter.go      # ProtocolAdapter lifecycle, Start/Stop/Status
│       ├── binary.go       # Binary framing (HEL/ACK, OPN, MSG, encodings)
│       ├── channel.go      # SecureChannel and Session state machine
│       ├── subscription.go # Subscription and Monitored Item management
│       ├── browse.go       # Dynamic address space recursive crawler
│       ├── types.go        # NodeID, Variant, StatusCode, DataValue decoders
│       └── adapter_test.go # Comprehensive unit and mock server tests
├── config/
│   └── config.go           # Augmented with OPCUAAdapterConfig definitions
└── metrics/
    └── metrics.go          # Augmented with OPC-UA Prometheus gauges/counters
```

---

## Complexity Tracking

> **No Constitution violations detected.** Architecture adheres strictly to all 5 core platform principles and maintains 100% pure Go static compilation with zero cloud dependencies.
