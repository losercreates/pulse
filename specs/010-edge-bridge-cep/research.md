# Research: Milestone 010 — Industrial Edge Bridge & Complex Event Processing (CEP) Engine

**Feature**: `010-edge-bridge-cep`  
**Date**: 2026-10-30  
**Status**: Completed  

---

## 1. Industrial Edge Protocol Ingestion (MQTT v3.1.1/v5.0 & Sparkplug B)

### Context & Challenge
Industrial field sensors and SCADA gateways in power plants, oil & gas installations, and manufacturing lines communicate primarily via publish/subscribe message brokers over MQTT (v3.1.1 and v5.0). Furthermore, the industrial IoT standard **Sparkplug B** organizes topic structures into `<namespace>/<group_id>/<message_type>/<edge_node_id>/[<device_id>]` and encodes multiple measurements into compact Google Protocol Buffer payloads (`sparkplug_b.proto`).

### Decisions
1. **Pure Go MQTT Client**:
   - Utilize a robust, pure Go client abstraction without external native C libraries (e.g., standard Go MQTT client / Franz-style connection management) to maintain static compilation (`CGO_ENABLED=0`).
   - Subscribe to configurable topics with QoS 0, 1, or 2 and topic wildcards (`+`, `#`).
2. **Sparkplug B Payload Parser**:
   - Parse standard Sparkplug B payload payloads (`NBIRTH`, `NDATA`, `DBIRTH`, `DDATA`, `NDEATH`, `DDEATH`).
   - Iterate metric collections within the decoded payload, extracting alias, name, timestamp, and typed values (Double, Float, Int32, Int64, Boolean).
   - Normalize each metric into a canonical Pulse Protobuf `TelemetryReading` with the exact edge sensor timestamp (`timestamp_ns`).
3. **Generic JSON/Key-Value MQTT Parser**:
   - For non-Sparkplug MQTT topics, provide a configurable JSON path extractor mapping payload fields (e.g. `{"temp": 55.4, "vibe": 1.2}`) and topic tokens (e.g. `factory/area1/{device_id}/{metric}`) into canonical `TelemetryReading` structs.

### Alternatives Considered
- **Direct Redpanda MQTT Proxy**: While Redpanda or Kafka can deploy HTTP/MQTT proxies, this requires direct WAN connectivity from factory edge devices into the cloud/datacenter cluster. In an industrial architecture, an edge bridge (`pulse-bridge`) running on the local factory LAN decouples the field network from the central cluster and provides store-and-forward resilience during WAN disconnects.

---

## 2. Legacy Fieldbus Polling (Modbus TCP & OPC-UA)

### Context & Challenge
Legacy Programmable Logic Controllers (PLCs) (e.g., Siemens S7, Schneider Modicon, Allen-Bradley) expose diagnostic data over Modbus TCP on port 502, while modern industrial automation controllers expose OPC-UA nodes. Neither protocol natively pushes streams over internet-style gRPC.

### Decisions
1. **Modbus TCP Master Engine**:
   - Implement an asynchronous cyclic polling scheduler in `pulse-bridge`.
   - Support Modbus Function Codes:
     - `FC03` (Read Holding Registers)
     - `FC04` (Read Input Registers)
   - Decode contiguous 16-bit registers into multi-byte data types:
     - 16-bit Signed / Unsigned Integer
     - 32-bit Float (IEEE 754) with configurable word endianness: Big-Endian (`ABCD`), Little-Endian (`DCBA`), Mid-Big/Swap (`BADC`), Mid-Little (`CDAB`).
   - Apply linear scaling equations: $y = \text{raw} \times \text{multiplier} + \text{offset}$.
2. **OPC-UA Polling & Monitored Items**:
   - Query targeted NodeIDs (e.g., `ns=2;s=Turbine1.BearingTemp`) over binary TCP.
3. **Fault Tolerance & Connection Recovery**:
   - Connection pooling per PLC endpoint with configurable timeouts (default 2s) and automatic reconnection backoff to prevent slow or unresponsive PLCs from blocking other polling loops.

---

## 3. Store-and-Forward Offline Edge Buffering

### Context & Challenge
Factory internet uplinks (cellular, microwave, satellite, leased lines) frequently experience transient drops or scheduled maintenance. Ingestion systems that drop readings when the upstream gateway is unreachable cause blind spots in historical records and disrupt event-time watermark progression.

### Decisions
1. **Dual-Tier Buffer Architecture (Memory + Local Disk FIFO)**:
   - **Tier 1 (In-Memory Ring Buffer)**: Low-latency circular queue holding recent readings (e.g., up to 10,000 records) for instantaneous absorption of micro-stalls.
   - **Tier 2 (Disk-Backed FIFO Log)**: When upstream gateway is unreachable, serialized Protobuf chunks are committed to append-only disk segment files (`spool_000001.dat`) in `/var/lib/pulse/bridge/`.
   - **Crash-Resilient Indexing**: A lightweight manifest (`manifest.json`) tracks read/write offsets, ensuring unacknowledged records survive edge bridge process restarts (`kill -9`).
2. **Strict Chronological Backfill**:
   - Upon reconnection to `pulse-gateway`, `pulse-bridge` streams spooled segments in strict FIFO order over gRPC client streaming (`IngestStream`) before switching back to live streaming, guaranteeing deterministic watermark progression.
3. **Safety Caps & Backpressure**:
   - Configurable maximum disk allocation (e.g. 5GB). When exceeded, bridge prioritizes critical equipment or throttles non-essential polling loops while emitting a `bridge_buffer_overflow` Prometheus alert.

---

## 4. Complex Event Processing (CEP) Engine Architecture

### Context & Challenge
The Stream Processor (`pulse-processor`) currently calculates univariate $k$-sigma deviations per `(device_id, metric_name)` in partition-local memory. However, critical mechanical failures involve sequential, multi-metric causal progressions:
- *Stage 1*: Shaft vibration exceeds nominal baseline.
- *Stage 2*: Within 30 seconds, bearing temperature spikes.
- *Stage 3*: Within 60 seconds, lubrication oil pressure drops.

### Decisions
1. **Constitution Principle I Conformance (Zero-Lock Concurrency)**:
   - The CEP engine evaluates stateful rules **strictly inside the dedicated partition worker goroutines**. Because telemetry is partitioned by `device_id` via Murmur2 hashing, all correlated metrics for a given device arrive at the exact same worker!
   - Pattern state is held in partition-local candidate tables with **zero cross-thread locks**.
2. **Non-Deterministic Finite Automaton (NFA) Pattern Matching**:
   - Model sequential rules (`A FOLLOWED_BY B WITHIN ΔT`) using a lightweight, pure Go NFA.
   - When event $A$ matches the start predicate at event-time $T_A$, an in-flight `SequenceCandidate` is instantiated with expiration deadline $T_{\text{expire}} = T_A + \Delta T$.
   - When event $B$ arrives for that device at event-time $T_B$:
     - If $T_A \le T_B \le T_{\text{expire}}$ and predicate $B$ matches, the sequence completes successfully!
     - If $T_B > T_{\text{expire}}$, candidate is dropped.
3. **Event-Time Watermark Expiration (Constitution Principle II)**:
   - Candidate states are purged strictly when the partition event-time watermark advances past $T_{\text{expire}}$. This prevents candidate memory leaks while ensuring deterministic behavior independent of wall-clock arrival order.
4. **Cross-Metric Mathematical Expressions**:
   - Implement an embedded, pure Go expression evaluator supporting standard arithmetic operators (`+`, `-`, `*`, `/`) and comparisons (`<`, `<=`, `>`, `>=`, `==`, `!=`).
   - Synchronizes multi-metric values within a sliding correlation window (e.g. comparing the most recent `power_mw` and `fuel_flow` readings).
5. **Composite Incident Synthesis & Decoupled Alerting**:
   - When a complex sequence or cross-metric condition triggers, the engine synthesizes a `CompositeIncidentEvent` listing root-cause trigger metric, secondary symptoms, and deviation scores.
   - Emits the event to Redpanda topic `telemetry.alerts` for asynchronous dispatch by `alertd` with HMAC-SHA256 signatures, ensuring downstream webhook delays never block stream processing (Constitution Principle IV).

---

## 5. Live Rule Configuration & Dynamic Reloading

### Context & Challenge
In continuous 24/7 manufacturing operations, stopping or restarting stream processors to adjust a vibration threshold or add a new composite rule is unacceptable.

### Decisions
1. **Atomic Rule Registry Swapping**:
   - Rule configurations are stored as declarative YAML/JSON files.
   - Stream processor monitors the rule directory or exposes a local administration trigger (`POST /api/v1/rules/reload` or `SIGHUP`).
   - The rule compiler parses and validates the new rule definitions in a staging area. If validation passes, active rule pointers in partition workers are updated via atomic swaps (`atomic.Pointer[RuleSet]`).
   - If syntax or logical errors exist in the update, the existing rule set remains active and an error is returned immediately with zero service disruption.

