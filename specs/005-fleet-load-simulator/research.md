# Research: Milestone 005 — Fleet Load Simulator & Chaos Harness

**Feature**: `005-fleet-load-simulator`  
**Date**: 2026-10-24  
**Status**: Completed  

---

## Technical Decisions & Rationale

### 1. High-Throughput Transport Architecture: gRPC Streaming vs HTTP Batching

- **Context**: To validate the Constitution performance target of sustaining $\ge 50,000\text{ events/sec}$, the simulator must generate and transmit telemetry at wire speed without bottlenecking on client-side operating system ephemeral port exhaustion or connection handshakes.
- **Decision**: Primary transport uses **client-side gRPC streaming** (`IngestionService.StreamReadings` on port `:50051`) multiplexed across a bounded pool of persistent HTTP/2 transport connections (e.g. 4 to 8 connections). An asynchronous **HTTP/JSON batching transport** (`POST /api/v1/telemetry/batch` on port `:8080`) is maintained as a configurable alternative to validate REST pipeline compatibility at up to 1,000 readings per batch.
- **Rationale**:
  - Unary gRPC or individual HTTP requests require per-message round-trips that introduce socket churn and latency overhead.
  - gRPC client-side streaming utilizes HTTP/2 bidirectional multiplexing with binary Protobuf encoding, achieving throughput $> 100,000\text{ events/sec}$ per client process with sub-millisecond network framing overhead.
  - HTTP batching with pipelining validates REST ingestion paths while respecting the 1,000 reading batch ceiling defined in Milestone 001.
- **Alternatives Considered**:
  - *Direct Kafka/Redpanda Producer*: Rejected because the load simulator must test the entire edge platform starting at the Ingestion Gateway ingress boundary, not bypass gateway validation and partition routing.
  - *Raw TCP / UDP Sockets*: Rejected because Pulse standardizes on Protobuf v3 over gRPC and JSON over HTTP.

---

### 2. Fleet Concurrency & Worker Pool Architecture

- **Context**: The simulator must simulate up to $10,000+$ distinct industrial devices concurrently without exhausting host memory or goroutine scheduling overhead.
- **Decision**: Pre-allocate an array of $N$ lightweight `DeviceState` structs partitioned across a fixed worker pool of $W = \text{runtime.NumCPU()} \times 2$ worker goroutines (default 16 workers on 8-core host). Each worker manages a deterministic partition of the device fleet and writes to a thread-safe batch channel or persistent gRPC stream.
- **Rationale**:
  - Spawning 10,000 concurrent goroutines with separate timers introduces non-trivial GC pressure and timer wheel overhead in Go.
  - A fixed worker pool with round-robin device emission allows sub-nanosecond micro-scheduling, deterministic memory footprints ($< 100\text{ MB}$ for 10,000 devices), and clean rate-limiting.
- **Alternatives Considered**:
  - *One Goroutine Per Device*: Simple to model but degrades above 10,000 devices due to runtime context switching and scheduler contention.

---

### 3. Realistic Industrial Sensor Signal Modeling

- **Context**: Sensor readings must reflect realistic physical dynamics (turbines, generators, motors) so that the downstream stream processor window aggregations and anomaly detection baselines operate on realistic distributions.
- **Decision**: Implement a composite signal model for each device:
  $$\text{Value}(t) = \text{Base} + A \cdot \sin(2\pi f t + \phi) + \mathcal{N}(0, \sigma^2) + \text{Drift}(t) + \text{Anomaly}(t)$$
  Each device is assigned an industrial archetype:
  1. **Wind Turbine (`turbine-XXXX`)**: Emits `vibration` ($4.5 \pm 0.4\text{ mm/s}$), `bearing_temp` ($65 \pm 3.0^\circ\text{C}$), `rotor_rpm` ($15 \pm 2\text{ rpm}$).
  2. **Induction Motor (`motor-XXXX`)**: Emits `vibration` ($2.8 \pm 0.2\text{ mm/s}$), `current_amp` ($45 \pm 4.0\text{ A}$), `power_mw` ($1.2 \pm 0.1\text{ MW}$).
  3. **Hydraulic Pump (`pump-XXXX`)**: Emits `oil_pressure` ($120 \pm 8.0\text{ psi}$), `temp` ($55 \pm 2.5^\circ\text{C}$).
- **Rationale**:
  - Pure pseudo-random noise lacks continuous correlation, while purely static values trigger artificial flatline variance alarms.
  - Adding sinusoidal components with gentle Gaussian noise mirrors actual mechanical vibration and thermal dissipation cycles.

---

### 4. Precision Monotonic Rate Pacing

- **Context**: Driving steady $\ge 50,000\text{ events/sec}$ requires emitting an event every $20\text{ microseconds}$ without burst clumping or drift against wall-clock time.
- **Decision**: Use a token bucket rate limiter with atomic credit allocation combined with micro-batching. Workers emit small micro-batches (e.g. 20 to 50 readings) rather than individual atomic mutex locks per event, amortizing channel and stream lock overhead.
- **Rationale**:
  - Nanosecond sleeping (`time.Sleep(20µs)`) on Linux/WSL2 has standard scheduler resolution of $\sim 50\text{–}100\text{µs}$, causing burstiness if attempted per-event.
  - Micro-batching 25 readings at $500\text{µs}$ intervals delivers steady target throughput with sub-millisecond pacing fidelity.

---

### 5. Chaos Simulation Mechanisms (Bursts, Backfills, Anomalies)

- **Context**: The simulator must validate Constitution Principles II (Event-Time Watermarking) and IV (Decoupled Anomaly Alerts).
- **Decision**:
  1. **Bursty Spike**: Multiplies target rate by $M \in [1.5, 5.0]$ for duration $D$.
  2. **Offline Recovery Backfill**: Intercepts outgoing packets for a selected subset of devices ($P_{offline} = 5\text{–}10\%$), accumulates them in memory ring buffers for $T_{offline} = 30\text{–}60\text{s}$, and releases them in burst when connectivity resumes. Events that fall behind $(watermark - allowed\_lateness)$ test late-data dead-letter routing to `telemetry.late`.
  3. **Sensor Anomaly Chaos**:
     - *Step Spike*: Value jumps $k \cdot \sigma$ (e.g. $k=5.0$) above baseline, forcing $z \ge 5.0$ and triggering `SEVERITY_CRITICAL`.
     - *Sensor Flatline*: Value freezes at constant value ($\sigma=0$) for $N=100$ readings, testing variance floor guardrails ($\sigma^2 \ge \epsilon$).
     - *Calibration Drift*: Value drifts gradually upward by $+0.5\sigma$ per reading, testing EWMA baseline adaptation.

---

### 6. Canary Latency Probing & End-to-End SLA Verification

- **Context**: Validating Constitution performance targets requires measuring the elapsed duration from sensor emission until window aggregate persistence in ChronoDB.
- **Decision**: Simulator generates a periodic **Canary Event** tagged with metadata (e.g. `device_id="canary-probe-XX"`, `metric_name="latency_canary"`). A dedicated background poller samples the ChronoDB query endpoint (`GET /api/v1/query?device_id=canary-probe-XX&metric_name=latency_canary`) to compute true end-to-end event-to-query latency.
- **Rationale**:
  - Measuring client-side send latency only reflects the Gateway socket time.
  - End-to-end canary probing accurately measures the complete pipeline latency: Gateway $\to$ Redpanda $\to$ Stream Processor $\to$ Aggregates Topic $\to$ ChronoDB $\to$ Query API.
