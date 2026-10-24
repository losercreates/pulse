# Feature Specification: Milestone 005 — High-Throughput Device Fleet Load Simulator & Chaos Stress Harness

**Feature Branch**: `005-fleet-load-simulator`  
**Created**: 2026-10-24  
**Status**: Draft  
**Input**: User description: "Create milestone 005: High-throughput device fleet load simulator and chaos stress harness generating steady-state, bursty, offline-recovery, and anomalous telemetry profiles across gRPC and REST to validate 50k events/sec SLA"

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Configurable Device Fleet Topology & High-Throughput Steady-State Generation (Priority: P1) 🎯 MVP

As a platform performance engineer, I need to instantiate a virtual fleet of industrial devices (e.g. 100 to 10,000+ devices across wind turbines, motors, pumps, and generators) emitting realistic, multi-metric sensor readings at high frequency over gRPC and HTTP/REST, so that I can validate that the Ingestion Gateway and stream processing pipeline sustain $\ge 50,000$ events/sec with bounded memory and consumer lag.

**Why this priority**: Delivering baseline high-throughput load generation across thousands of discrete device IDs is the foundational prerequisite to validating the platform's primary SLA and Constitution performance mandates.

**Independent Test**: Configure the simulator with 1,000 devices emitting at 50 Hz over gRPC, run continuously for 60 seconds, and verify that the Ingestion Gateway receives $\ge 50,000$ events/sec with 0% socket exhaustion or client-side event drops.

**Acceptance Scenarios**:

1. **Given** a target device count $N=1,000$ and target throughput $R=50,000\text{ events/sec}$, **When** the simulator starts with transport set to gRPC streaming, **Then** it concurrently drives telemetry across all $N$ distinct device identifiers sustaining $\ge 50,000\text{ events/sec}$ with $< 1\%$ rate deviation.
2. **Given** a target endpoint configured for HTTP/REST batch ingestion, **When** the simulator executes with batch size $B=100$, **Then** it aggregates readings into schema-compliant batch payloads and receives HTTP 202 Accepted receipts.
3. **Given** multi-metric device profiles, **When** telemetry is generated, **Then** readings cycle deterministically through realistic metric names (`vibration`, `bearing_temp`, `rotor_rpm`, `power_mw`, `oil_pressure`) with plausible physical engineering units (`celsius`, `mm/s`, `rpm`, `mw`, `psi`) and continuous values.

---

### User Story 2 - Bursty Spikes & Backpressure Traffic Profile (Priority: P2)

As a site reliability engineer, I need to simulate sudden, high-amplitude traffic bursts (e.g., $3\times$ to $5\times$ baseline volume for 10–30 seconds) representing storm events or operational shifts, so that I can observe broker queue depths, consumer lag bounding, and backpressure propagation through the system without unhandled crashes.

**Why this priority**: Streaming platforms frequently experience peak traffic surges; validating that the gateway buffers appropriately and the stream processor handles bursts without memory runaway is vital for production survivability.

**Independent Test**: Run a steady baseline of 20,000 events/sec, inject a 10-second burst of 80,000 events/sec, and verify that the system absorbs the burst with zero dropped packets and consumer lag recovers to $< 1$s within 15 seconds after the burst ends.

**Acceptance Scenarios**:

1. **Given** an ongoing steady-state load simulation, **When** the operator triggers or schedules a burst profile (duration $D=15\text{s}$, multiplier $M=3.0$), **Then** the simulator immediately escalates delivery rate to $M \times \text{baseline}$ and logs burst ingress metrics.
2. **Given** the burst rate exceeds maximum single-connection TCP capacity, **When** workers experience gateway backpressure, **Then** client-side bounded ring buffers absorb spikes or rate-limit gracefully without client process termination or out-of-memory panics.

---

### User Story 3 - Offline Network Disconnect & Out-of-Order Recovery Backfills (Priority: P2)

As an edge systems engineer, I need to simulate intermittent fleet network disconnects where a subset of edge devices lose connectivity, buffer historical telemetry locally for 30–60 seconds, and then dump large out-of-order historical backfills upon reconnecting, so that I can verify that event-time watermarking progresses monotonically, closed windows remain strictly immutable, and stale data routes safely to the dead-letter sink (`telemetry.late`).

**Why this priority**: Fulfills Constitution Principle II (Event-Time Watermarking) and Testing Standard § Fleet Simulation: aggregation outputs must remain mathematically invariant to arrival order within allowed lateness, and out-of-order late events must never corrupt closed historical aggregates.

**Independent Test**: Disconnect 50 devices for 45 seconds while the rest of the fleet continues emitting, reconnect the 50 devices with rapid backfill replay, and verify that (a) on-time backfilled readings within allowed lateness are correctly included in open windows, and (b) readings older than the current watermark are cleanly routed to `telemetry.late` with zero corruption of closed aggregates in ChronoDB.

**Acceptance Scenarios**:

1. **Given** an offline chaos simulation configured for $10\%$ of devices with a disconnect window of 40 seconds, **When** those devices disconnect, **Then** the simulator pauses network transmissions for those specific device IDs while continuing local timestamp progression.
2. **Given** the offline window concludes, **When** the devices reconnect, **Then** the simulator dumps the accumulated backlog at maximum link capacity, transmitting timestamps strictly ordered per-device.
3. **Given** backfilled readings with timestamps older than $(watermark - allowed\_lateness)$, **When** the stream processor evaluates them, **Then** closed ChronoDB window records remain unchanged and late event metrics increment proportionally.

---

### User Story 4 - Deliberate Sensor Anomaly & Failure Chaos Injection (Priority: P3)

As an automation engineer, I need to inject deliberate, statistically significant anomalies (abrupt step changes, rolling sensor drifts, and zero-variance flatline failures) into targeted devices, so that I can verify that the real-time anomaly detection engine flags $z \ge 3.0$ standard score violations, triggers severity escalation, and dispatches alerts without false-positive storms.

**Why this priority**: Fulfills Constitution Principle IV: real-time statistical baselines and decoupled alert dispatching must be tested under controlled anomaly conditions to verify alerting accuracy and circuit breaker behavior.

**Independent Test**: Target device `turbine-042` with an abrupt vibration spike of $50.0\text{ mm/s}$ (baseline $\mu=4.5, \sigma=0.3$) and flatline `motor-012` at constant $75.0^\circ\text{C}$ for 100 samples; assert that an alert event is published for the spike and the variance floor guardrail prevents alert runaway for the flatline.

**Acceptance Scenarios**:

1. **Given** a designated device with warmed-up nominal baseline ($\ge 30$ samples), **When** an anomaly profile of type `step_spike` is injected with deviation $k=5.0$, **Then** the emitted reading value forces $z \ge 5.0$ and triggers an alert event with severity `SEVERITY_CRITICAL`.
2. **Given** an anomaly profile of type `variance_flatline`, **When** a device emits 100 consecutive identical values, **Then** the variance floor guardrail ($\sigma^2 \ge \epsilon$) maintains numerical stability and generates zero false-positive anomaly storms.
3. **Given** an ongoing anomalous reading sequence, **When** the anomaly persists beyond the 5-minute cooldown window, **Then** an alert escalation reminder is dispatched.

---

### User Story 5 - Real-Time Ingestion SLA & Latency Telemetry Dashboard (Priority: P3)

As a performance test lead, I need a terminal-based real-time telemetry dashboard and summary report displaying live generation throughput, dropped/errored packets, active device counts, and end-to-end event-to-query latency (time from sensor creation timestamp to visibility in ChronoDB), so that I can immediately evaluate compliance against the 50k events/sec SLA and latency gates.

**Why this priority**: Provides actionable feedback during load test runs and produces machine-readable summary reports (JSON/stdout) for CI pipeline regression verification.

**Independent Test**: Run a 120-second load test; verify that the terminal outputs updating live throughput, partition distribution metrics, and a final summary verifying that average throughput exceeded $50,000\text{ events/sec}$ and p95 end-to-end latency was $< 2.0\text{s}$.

**Acceptance Scenarios**:

1. **Given** a running simulator session, **When** viewing the console, **Then** it refreshes every 1 second displaying instantaneous throughput (events/sec), total events sent, error rate, and active worker pool utilization.
2. **Given** latency probing enabled, **When** the simulator samples synthetic canary readings, **Then** it periodically polls the ChronoDB query endpoint (`:8084`) to measure and record end-to-end window-close-to-query latency.
3. **Given** the simulation run finishes, **When** exiting, **Then** the simulator outputs a structured SLA Compliance Report with exit code 0 if all performance criteria are satisfied, or non-zero if throughput or latency SLAs are violated.

---

## Edge Cases

- **Target Service Unavailability**: If the Ingestion Gateway (:8080 / :50051) is temporarily unreachable or slow to start, the simulator MUST retry with exponential backoff rather than crashing on launch.
- **Client Resource Starvation**: Generating $\ge 50,000$ events/sec on a single host may exceed local OS ephemeral socket limits or garbage collection thresholds; the simulator MUST reuse persistent gRPC stream connections and pre-allocated object pools.
- **Clock Skew Defenses**: Generated event timestamps MUST remain strictly within $[-24\text{h}, +5\text{m}]$ of the host clock to satisfy Gateway validation rules, except when explicitly exercising late-data routing.
- **Graceful Termination**: On receiving `SIGINT` or `SIGTERM`, the simulator MUST cleanly flush active stream buffers, log aggregate generation statistics, and terminate within 3 seconds.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a standalone command-line simulator tool supporting configurable device fleet topology ($N \ge 10,000$ distinct device IDs).
- **FR-002**: System MUST sustain configurable aggregate ingestion throughput from 1,000 up to $\ge 50,000$ events/sec across dual transports (gRPC client-side streaming on `:50051` and HTTP/JSON batching on `:8080`).
- **FR-003**: System MUST support independent device state machines generating realistic industrial telemetry spanning multiple metrics (`vibration`, `bearing_temp`, `rotor_rpm`, `power_mw`, `oil_pressure`) with correlated physical units.
- **FR-004**: System MUST partition simulated device IDs uniformly across Redpanda topic partitions using deterministic device key hashing.
- **FR-005**: System MUST support a **Steady-State Profile** maintaining smooth Gaussian-distributed values within nominal operating ranges.
- **FR-006**: System MUST support a **Bursty Spike Profile** scaling event volume by a configurable multiplier ($1.5\times$ to $5.0\times$) for predetermined durations.
- **FR-007**: System MUST support an **Offline Recovery Profile** simulating network dropouts where selected devices pause transmission for $T_{offline}$ seconds and subsequently dump buffered events in burst upon reconnection.
- **FR-008**: System MUST support an **Anomaly Injection Profile** producing deliberate step spikes, sensor drift, and zero-variance flatlines targeting selected devices to test downstream detection and alerting.
- **FR-009**: System MUST ensure per-device event timestamps advance monotonically during normal operation to preserve per-device temporal ordering.
- **FR-010**: System MUST measure client-side submission throughput (events/sec) and error rates (dropped or rejected readings).
- **FR-011**: System MUST support canary latency probes that periodically sample end-to-end latency from reading timestamp to query availability in ChronoDB (`:8084`).
- **FR-012**: System MUST produce structured machine-readable test summaries (JSON output) indicating SLA compliance (throughput achieved, error count, p95 latency).
- **FR-013**: System MUST provide an interactive terminal dashboard displaying live throughput, active fleet size, and broker error counts during manual test runs.
- **FR-014**: System MUST handle OS interruption signals (`SIGINT`, `SIGTERM`) with graceful socket closure and summary output within 3 seconds.
- **FR-015**: System MUST allow configuration via command-line flags and environment variables (`SIM_TARGET_RATE`, `SIM_NUM_DEVICES`, `SIM_DURATION`, `SIM_TRANSPORT`, `SIM_PROFILE`).

---

### Key Entities

- **DeviceTopology**: The collection of virtual devices, device identifiers, assigned industrial profiles, and metric generation state machines.
- **TrafficProfile**: Operational parameters governing emission frequency, burst multipliers, anomaly frequency, and offline backfill behaviors.
- **SimulationRun**: The execution context capturing target throughput, actual achieved rate, elapsed duration, transport protocol, error tallies, and latency samples.
- **CanaryProbe**: A dedicated synthetic telemetry record tracked end-to-end to measure window closure and storage persistence latency.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Simulator sustains $\ge 50,000\text{ events/sec}$ throughput continuously for at least 5 minutes over gRPC streaming without client-side memory leakage.
- **SC-002**: Simulator successfully manages a fleet of $10,000$ concurrent simulated devices with $< 0.1\%$ dropped readings under nominal operating conditions.
- **SC-003**: End-to-end event-to-query p95 latency is measured and verified $< 2.0\text{ seconds}$ for 10-second tumbling windows under sustained 50,000 events/sec load.
- **SC-004**: During offline recovery backfill tests, 100% of out-of-order readings exceeding allowed lateness are rejected to `telemetry.late` with zero corruption of closed historical aggregates.
- **SC-005**: 100% of injected anomaly readings with deviation $z \ge 3.0$ trigger corresponding alert records in the downstream alert subsystem.
- **SC-006**: CLI tool terminates cleanly and outputs complete JSON execution summaries within 3 seconds upon receiving termination signals.

---

## Assumptions

- Target Pulse services (Gateway, Stream Processor, Alertd, ChronoDB, and Redpanda) are running and accessible on standard localhost ports (`:8080`, `:50051`, `:8081`, `:8083`, `:8084`).
- Host machine running the load test provides at least 4 CPU cores and 8 GB RAM to sustain 50,000 events/sec client generation alongside server processes.
- gRPC streaming is the primary transport for maximum throughput benchmarks; HTTP batching is available for REST compatibility validation.
- Simulated device IDs follow standard naming conventions (`turbine-XXXX`, `motor-XXXX`, `pump-XXXX`) with length between 10 and 32 characters.
