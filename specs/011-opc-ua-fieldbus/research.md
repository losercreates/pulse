# Phase 0 Research: Milestone 011 — Native OPC-UA Binary Fieldbus Adapter

**Branch**: `011-opc-ua-fieldbus`  
**Date**: 2026-11-03  
**Spec**: [spec.md](spec.md)

---

## 1. Protocol Architecture & Wire Encoding

### Decision
Implement a pure Go binary driver for OPC-UA (`opc.tcp://`) in `internal/bridge/adapter/opcua`, utilizing standard library primitives (`net.Conn`, `encoding/binary`, `crypto/tls`, `crypto/rsa`, `crypto/sha256`) to handle client-side binary communication with industrial PLCs.

### Rationale
- **Zero CGo Invariant**: Retains Pulse's strict `CGO_ENABLED=0` static compilation guarantee. Distroless containers remain under 35MB with zero external runtime dependencies.
- **Client-Side Telemetry Scope**: Pulse only requires client services: Hello/Acknowledge handshake, Secure Channel setup, Session creation/activation, Subscription management, Monitored Item creation, Publish response dispatching, and recursive Node browsing. A focused pure Go implementation avoids the footprint and memory overhead of sprawling full-stack OPC-UA server frameworks.
- **High Ingestion Throughput**: Memory-buffered binary decoders eliminate heap allocations during high-frequency `DataChangeNotification` bursts.

### Alternatives Considered
- **C-based `open62541` via CGo**: Rejected. Introduces dynamic link dependencies, complicates cross-compilation across x86_64 and ARM64 edge architectures, and prevents deploying onto distroless scratch containers.
- **Full third-party OPC-UA stacks**: Evaluated. Many introduce extensive enterprise abstractions and heavy memory footprints. A lean, modular native adapter pattern (matching `modbus.go` and `mqtt.go`) ensures low latency and seamless integration with the bridge's channel pipeline.

---

## 2. Event-Driven Subscriptions vs. Cyclic Polling

### Decision
Rely primarily on OPC-UA server-side Subscriptions (`CreateSubscription`, `CreateMonitoredItems`, and pipelined `PublishRequest` loops) rather than cyclic Read requests.

### Rationale
- **Bandwidth & CPU Conservation**: In industrial automation, telemetry values often remain within steady-state bands for minutes before transitioning. Subscriptions ensure that PLC CPUs and edge network links transmit data only when physical values change (or when keep-alive heartbeats trigger).
- **Sub-Millisecond Temporal Fidelity**: Cyclic polling introduces sample quantization delay equal to the polling interval (e.g. 100ms–1000ms). In contrast, monitored items capture sensor transitions at the PLC's internal scan rate and transmit with source nanosecond timestamps.
- **Deadband Filtering**: OPC-UA deadband filters (absolute or percentage-of-range) can be pushed to the PLC, filtering out baseline electrical sensor noise before serialization.

### Alternatives Considered
- **Cyclic Read Polling (like Modbus)**: Rejected as primary mechanism. While useful as a fallback for defective PLC firmware, cyclic polling wastes bandwidth and strains controller processing capacity.

---

## 3. Data Type Normalization & Quality Code Handling

### Decision
Normalize incoming OPC-UA `Variant` values into canonical IEEE 754 `float64` metrics while preserving the exact `SourceTimestamp` (falling back to `ServerTimestamp` or local ingestion time if absent). Map OPC-UA `StatusCode` values into telemetry metadata flags.

### Rationale
- **Numeric Precision**: Industrial process parameters (pressure, vibration, temperature, speed, voltage) map naturally to `float64`. Integer variants (`Int16`, `Int32`, `UInt32`, `Int64`, `UInt64`) and `Boolean` values (`1.0` / `0.0`) are converted without precision loss.
- **Quality Status Visibility**: OPC-UA StatusCodes (32-bit unsigned integers) convey critical operational state:
  - `0x00000000` (`Good`): Normal operation.
  - `0x40000000` (`Uncertain`): Sensor calibration drift or stale cached value.
  - `0x80000000` (`Bad`): Sensor wire break, out-of-range hardware fault, or device disconnected.
  Values with `Bad` status are marked in telemetry metadata and increment diagnostic error counters without crashing the stream.

---

## 4. Dynamic Address Space Browsing

### Decision
Implement hierarchical address space browsing using the OPC-UA `Browse` service starting from configurable root nodes (default: `ns=0;i=85` Objects folder). Support recursive traversal with visited-set cycle detection and glob/prefix matching.

### Rationale
- **Commissioning Efficiency**: Industrial PLCs often manage thousands of variables organized under hierarchical folders (`Objects/Line1/Turbine02/BearingTemp`). Dynamic browsing allows engineers to specify patterns (e.g., `Objects/Line1/*/*_temp`) instead of manually transcribing hundreds of numeric NodeIDs (`ns=2;i=10842`).
- **Resilience to PLC Re-indexing**: If a PLC program download shifts numeric NodeIDs, browse path re-resolution automatically discovers the updated identifiers upon reconnection.

### Alternatives Considered
- **Static Configuration Only**: Rejected. Forcing manual entry of hundreds of NodeIDs is brittle and human-error prone.
- **Full Address Space Crawl on Startup**: Rejected without path filters. Large DCS systems can have over 500,000 nodes; an unbounded crawl would exhaust memory and delay bridge startup. Configurable root paths and depth limits ensure fast, bounded startup.

---

## 5. Offline Buffering & Connection State Machine

### Decision
Integrate the OPC-UA adapter directly into the `Coordinator` and `OfflineSpooler` pipeline established in Milestone 010. Maintain a 3-state connection machine (`Disconnected`, `Connecting`, `Connected`) with keep-alive heartbeat monitoring.

```
       [ Disconnected ] ◄──────────────────────────────┐
              │                                        │ Keep-alive failure /
              │ Net.Dial + HEL/ACK + OPN               │ Server disconnect
              ▼                                        │
        [ Connecting ] ────────────────────────────────┤
              │                                        │
              │ CreateSession + ActivateSession + Sub  │
              ▼                                        │
        [ Connected / Streaming ] ─────────────────────┘
```

### Rationale
- **Zero Loss During Edge Partitions**: The adapter emits `TelemetryReading` structs into the bridge's unified `inChan`. If the upstream gateway is unreachable, the coordinator automatically spools records to the local `PUBR` disk FIFO log.
- **Graceful PLC Failover**: When the PLC reboots, keep-alive publish timeouts detect the severance within seconds, tear down dead TCP sockets, and trigger exponential backoff reconnection.

