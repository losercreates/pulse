# Feature Specification: Milestone 010 — Industrial Edge Bridge & Complex Event Processing (CEP) Engine

**Feature Branch**: `010-edge-bridge-cep`  
**Created**: 2026-10-30  
**Status**: Draft  
**Input**: User description: "Create milestone 010: Industrial Edge Bridge and Complex Event Processing (CEP) Engine with a standalone edge bridge daemon (pulse-bridge) supporting MQTT v3.1.1/v5.0, Sparkplug B, and Modbus TCP/OPC-UA protocol translation to Protobuf, local disk store-and-forward offline buffering with mTLS gRPC streaming to gateway, and an in-memory partition-local CEP rule engine in stream processor supporting temporal sequence patterns (FOLLOWED_BY, WITHIN), cross-metric mathematical expressions, multi-sensor composite incident synthesis, and live reloadable rule configurations."

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Industrial MQTT & Sparkplug B Ingestion with Store-and-Forward Offline Buffering (Priority: P1) 🎯 MVP

As an Industrial Automation Engineer, I need an edge bridge daemon (`pulse-bridge`) that subscribes to factory MQTT brokers (v3.1.1/v5.0) and Sparkplug B namespaces, translates industrial JSON/binary metrics into canonical Protobuf telemetry, securely forwards them to `pulse-gateway` via mTLS gRPC streaming, and buffers data locally on disk during factory network disconnects so that no critical machine readings are lost during plant LAN outages.

**Why this priority**: Industrial facilities, power plants, and wind farms standardly deploy MQTT and Sparkplug B edge infrastructure. Providing a protocol bridge with local store-and-forward buffering enables immediate adoption in existing factory automation without requiring field firmware modifications, and guarantees zero data loss during intermittent edge WAN disconnects.

**Independent Test**: Connect `pulse-bridge` to a local MQTT broker publishing simulated machine topics; verify telemetry is converted to Protobuf and streamed to `pulse-gateway` in real time; sever network connectivity between bridge and gateway for 60 seconds while publishing continues; verify bridge spools telemetry to local disk ring buffer; restore network connectivity and verify all buffered records are flushed in chronological timestamp order to the gateway with zero lost or duplicated records.

**Acceptance Scenarios**:
1. **Given** an MQTT broker publishing industrial telemetry topics (e.g. `factory/alpha/turbine-01/temp`), **When** `pulse-bridge` receives payload bytes, **Then** it decodes the payload, maps topic/metric tokens into a canonical `TelemetryReading` with the exact edge timestamp, and streams it to `pulse-gateway` over gRPC.
2. **Given** a Sparkplug B payload containing multiple metric metrics within a single protobuf frame, **When** `pulse-bridge` processes the frame, **Then** it decomposes the batch into individual `TelemetryReading` events, preserving metric aliases and timestamps.
3. **Given** an edge network partition where `pulse-gateway` is unreachable, **When** new readings arrive from the local broker, **Then** `pulse-bridge` writes readings to a FIFO disk-backed ring buffer and attempts exponential backoff reconnection.
4. **Given** network restoration after a partition, **When** the gRPC connection to `pulse-gateway` re-establishes, **Then** `pulse-bridge` flushes buffered readings in strict event-time sequence before resuming live-stream bypass.

---

### User Story 2 - Modbus TCP & OPC-UA Industrial Equipment Polling & Normalization (Priority: P2)

As a Plant Operations Engineer, I need `pulse-bridge` to poll legacy Modbus TCP PLCs (Programmable Logic Controllers) and OPC-UA server nodes at configured periodic intervals, apply unit scaling factors, and convert raw register bytes into typed floating-point telemetry so that legacy factory machines without native MQTT capabilities can be ingested into Pulse.

**Why this priority**: A large fraction of operational manufacturing machinery and legacy turbines communicate exclusively over Modbus TCP or OPC-UA. Integrating direct polling capabilities into `pulse-bridge` eliminates the need for expensive third-party protocol converters.

**Independent Test**: Configure `pulse-bridge` to poll a simulated Modbus TCP PLC holding register every 100ms; verify register values are read, scaled (e.g., raw integer 1024 scaled by 0.1 to 102.4), and streamed to `pulse-gateway` with accurate device and metric metadata.

**Acceptance Scenarios**:
1. **Given** a Modbus TCP device with configured IP, port, slave ID, and register definitions, **When** the polling ticker fires, **Then** `pulse-bridge` reads holding/input registers, applies configured scaling factors and data type decoders (e.g., 16-bit int, 32-bit float, IEEE endianness), and emits canonical `TelemetryReading` records.
2. **Given** an OPC-UA endpoint with specified NodeIDs, **When** monitored items change or cyclic polling occurs, **Then** `pulse-bridge` extracts value attributes and converts them into standardized metric readings.
3. **Given** a transient Modbus read timeout or connection drop, **When** a poll fails, **Then** `pulse-bridge` records an error metric, marks the device as temporarily degraded, and retries on the next polling cycle without crashing.

---

### User Story 3 - Partition-Local Temporal Sequence Pattern Evaluation (`FOLLOWED_BY`, `WITHIN`) (Priority: P1)

As a Reliability Engineer, I need the Stream Processor to evaluate sequential multi-stage temporal patterns across sliding event-time windows on partition-local state (e.g. *Vibration spike > 4σ FOLLOWED_BY Bearing temperature > 3σ WITHIN 45s on the same device*), so that multi-phase mechanical failure cascades are caught deterministically without cross-goroutine lock contention.

**Why this priority**: Complex industrial physical failures are rarely isolated to a single instantaneous metric spike. Catching sequential causal patterns (e.g., initial mechanical unbalance followed by thermal runaway) provides early diagnostic warning before catastrophic equipment destruction.

**Independent Test**: Define a CEP rule requiring `vibration_spike` followed by `temperature_spike` within 30 seconds; stream a sequence where vibration spikes at $T=0\text{s}$ and temperature spikes at $T=15\text{s}$; verify the rule triggers a composite anomaly alert; stream a sequence where temperature spikes at $T=45\text{s}$ (past the 30-second window); verify the pattern times out and does not trigger.

**Acceptance Scenarios**:
1. **Given** a temporal pattern rule `EventA FOLLOWED_BY EventB WITHIN ΔT` assigned to an active asset, **When** `EventA` occurs at event-time $T_1$, **Then** a stateful partial match candidate is initiated in partition-local memory with expiration deadline $T_1 + \Delta T$.
2. **Given** an active partial match candidate awaiting `EventB`, **When** `EventB` arrives at event-time $T_2$ where $T_1 \le T_2 \le T_1 + \Delta T$, **Then** the sequence matches, the candidate transitions to complete, and a composite pattern event is produced.
3. **Given** an active partial match candidate awaiting `EventB`, **When** the event-time watermark advances past $T_1 + \Delta T$ without `EventB` arriving, **Then** the candidate expires deterministically and is evicted from memory.
4. **Given** high-volume streaming across 12 partitions, **When** evaluating temporal sequence patterns, **Then** pattern evaluation executes strictly in partition worker goroutines with zero cross-thread locking (Constitution Principle I).

---

### User Story 4 - Cross-Metric Mathematical Expressions, Composite Incident Synthesis & Live Rule Reloading (Priority: P2)

As an Industrial Asset Performance Analyst, I need the CEP engine to evaluate dynamic cross-metric arithmetic formulas (e.g. thermal efficiency `power_mw / (fuel_flow * heating_value) < 0.72`), synthesize multiple correlated metric alarms into a single unified root-cause incident for `alertd`, and allow rule definitions to be reloaded dynamically at runtime without restarting the stream processor.

**Why this priority**: Operators are overwhelmed by alert fatigue during emergency trips when 50 sensors sound alarms simultaneously. Synthesizing these into a single composite incident with the identified initiating event clarifies root causes and accelerates operator response. Live rule reloading allows engineering teams to deploy and tune thresholds without stream processing interruption.

**Independent Test**: Deploy a CEP rule for thermal efficiency degradation; stream correlated power and fuel flow telemetry violating the formula; verify a composite incident event is generated with details of both contributing metrics; update the rule threshold in the configuration file; trigger a live configuration reload signal; verify the new threshold takes effect immediately without dropped records or stream processor restarts.

**Acceptance Scenarios**:
1. **Given** a cross-metric formula rule combining two metrics for a device (e.g. `power_mw / fuel_flow < 0.8`), **When** readings for both metrics arrive within a synchronized sliding window, **Then** the CEP engine computes the expression value and evaluates the threshold.
2. **Given** multiple metric deviations occurring within a common incident window on a device, **When** the CEP engine synthesizes alerts, **Then** it produces a single `CompositeIncidentEvent` listing primary trigger metric, secondary symptoms, and deviation magnitudes.
3. **Given** an updated CEP rule YAML/JSON file, **When** an operator triggers a reload signal (via HTTP endpoint or SIGHUP), **Then** `pulse-processor` compiles and swaps the active rule registry atomically without dropping in-flight readings or resetting partition watermarks.

---

### Edge Cases

- **Factory Network Outage Exceeding Local Buffer**: When the edge bridge's local disk buffer reaches configured maximum capacity (e.g., 5GB), the bridge enters backpressure mode: drops oldest unacknowledged readings or halts non-critical polling while raising an edge alarm.
- **Out-of-Order Delivery Post-Partition Recovery**: When an offline buffer flushes after a prolonged network outage, flushed readings may arrive with timestamps older than current real-time readings. The Stream Processor's allowed lateness window determines whether flushed events are processed or routed to the late-data dead-letter queue (`telemetry.late`).
- **High-Frequency Sensor Flapping / Chatter**: If an erratic sensor oscillates across a sequence threshold 100 times per second, the CEP candidate matcher enforces a deduplication / debounce quiet period to avoid candidate state table explosion.
- **Partial Sequence Candidate Starvation / Memory Bounds**: Unmatched sequence candidates (e.g., Step 1 triggered, but Step 2 never arrives) are capped by a per-device maximum candidate limit and purged strictly when the partition event-time watermark advances past the window deadline.
- **Concurrent Rule Compilation Errors During Hot Reload**: If an invalid syntax or malformed expression is submitted in a rule configuration update, the CEP compiler rejects the update, retains the existing active rule registry intact, logs the validation error, and avoids process panics.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a standalone, lightweight edge daemon (`pulse-bridge`) capable of running on edge gateways and industrial Linux PCs with $< 50\text{MB}$ memory footprint.
- **FR-002**: `pulse-bridge` MUST support subscribing to MQTT v3.1.1 and v5.0 brokers with QoS 0, 1, and 2, supporting topic wildcards (`+`, `#`).
- **FR-003**: `pulse-bridge` MUST support decoding Sparkplug B binary protobuf payloads into individual `TelemetryReading` records with metric aliases and edge timestamps.
- **FR-004**: `pulse-bridge` MUST support polling Modbus TCP slave devices over IPv4/IPv6, reading Holding Registers (FC03) and Input Registers (FC04).
- **FR-005**: `pulse-bridge` MUST support Modbus data type decoding including 16-bit signed/unsigned int, 32-bit float (Big-Endian, Little-Endian, Mid-Big, Mid-Little), and scaling multipliers.
- **FR-006**: `pulse-bridge` MUST normalize all decoded industrial protocol data into canonical Protobuf `TelemetryReading` structures.
- **FR-007**: `pulse-bridge` MUST forward normalized telemetry to `pulse-gateway` using client-side streaming gRPC (`:50051`) with mTLS certificate authentication.
- **FR-008**: `pulse-bridge` MUST implement a disk-backed FIFO ring buffer spooling telemetry during network disconnects and flushing in timestamp sequence upon reconnection.
- **FR-009**: Stream Processor MUST include an embedded, zero-lock Complex Event Processing (CEP) rule engine running inside partition worker goroutines conforming to Constitution Principle I.
- **FR-010**: CEP engine MUST support temporal sequence pattern rules evaluating `EventA FOLLOWED_BY EventB WITHIN ΔT` on a per-device event-time timeline.
- **FR-011**: CEP engine MUST expire uncompleted temporal pattern candidates deterministically based on partition event-time watermarks conforming to Constitution Principle II.
- **FR-012**: CEP engine MUST evaluate cross-metric mathematical expressions across concurrent metric readings within an event-time correlation window.
- **FR-013**: CEP engine MUST synthesize multi-sensor alarm cascades into unified `CompositeIncidentEvent` payloads indicating initiating trigger and correlated symptoms.
- **FR-014**: CEP engine MUST publish generated composite incidents to Redpanda topic `telemetry.alerts` for decoupled handling by `alertd` conforming to Constitution Principle IV.
- **FR-015**: Stream Processor MUST support atomic live-reloading of CEP rule definitions from disk or HTTP endpoint without dropping telemetry or stalling partition consumption.
- **FR-016**: System MUST expose Prometheus metrics for edge bridge connection states, buffered bytes, spool write rates, CEP pattern evaluation rates, active sequence candidates, and composite incident counts.

---

### Key Entities

- **`EdgeBridgeConfig`**: Configuration model defining upstream gateway addresses, TLS credentials, MQTT broker endpoints, topic subscriptions, Modbus polling schedules, and offline buffer disk limits.
- **`ProtocolAdapter`**: Abstraction interface implemented by MQTT, Sparkplug B, and Modbus TCP ingestion drivers converting source frames into `TelemetryReading` messages.
- **`OfflineBuffer`**: Disk-backed FIFO persistence manager holding serialized telemetry frames during WAN disconnects with crash-resilient metadata tracking.
- **`CEPRule`**: Declarative definition of an event pattern, containing rule ID, target device selection pattern, temporal sequence steps, cross-metric formula, evaluation window ($\Delta T$), and output severity.
- **`SequenceCandidate`**: In-flight state tracking a partially matched sequence (e.g. Step 1 matched at $T_1$, awaiting Step 2 prior to $T_1 + \Delta T$).
- **`CompositeIncidentEvent`**: High-level alert event describing a matched multi-stage or multivariate anomaly, referencing initiating metric, correlated symptom metrics, timestamps, and confidence scores.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: `pulse-bridge` sustains $\ge 25,000\text{ readings/sec}$ protocol normalization and gRPC streaming throughput with $< 2\text{ms}$ bridge processing overhead.
- **SC-002**: 100% of telemetry received during factory network partitioning ($\le 1\text{ hour}$ under nominal rate) is preserved in local disk buffer and delivered to the gateway in chronological order upon reconnection with zero data loss.
- **SC-003**: CEP engine evaluates sequence patterns across 50,000 events/sec with $< 0.1\text{ms}$ per-event processing overhead and zero cross-goroutine lock contention.
- **SC-004**: Multi-sensor cascading alarm events on correlated machinery are synthesized into a single composite incident, achieving $\ge 90\%$ reduction in duplicate downstream alert dispatches.
- **SC-005**: Dynamic CEP rule updates reload and take effect in $< 500\text{ms}$ with zero dropped events and zero stream processor restarts.
- **SC-006**: Complete multi-service pipeline (Bridge $\rightarrow$ Gateway $\rightarrow$ Redpanda $\rightarrow$ Processor with CEP $\rightarrow$ Alertd) executes with zero data race warnings under Go `-race`.

---

## Assumptions

- **Cloud-Free / On-Premise Execution**: The entire edge bridge and CEP engine operate self-contained on bare-metal, edge industrial PCs, and local Kubernetes clusters without cloud dependencies (AWS/GCP/Azure).
- **Local Writeable Storage for Edge Bridge**: Edge gateways hosting `pulse-bridge` provide a writeable local directory (e.g. `/var/lib/pulse/bridge`) with sufficient disk capacity for offline buffering during expected outage durations.
- **Network Topology**: Local factory brokers (MQTT) and PLCs (Modbus TCP) are reachable over local factory LAN; `pulse-bridge` has an outbound network route to `pulse-gateway` (HTTP `:8080` or gRPC `:50051`).
- **Existing Security Reused**: `pulse-bridge` authenticates to `pulse-gateway` using the mTLS client certificates and anti-spoofing identity mechanisms implemented in Milestone 009.

