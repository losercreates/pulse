# Feature Specification: Real-Time Statistical Anomaly Detection with Decoupled Alert Dispatching

**Feature Branch**: `003-anomaly-detection-alerts`

**Created**: 2026-10-22

**Status**: Draft

**Input**: User description: "Create the specification for milestone 003: Real-Time Anomaly Detection Engine & Decoupled Alert Dispatcher. Feature Name: Real-Time Statistical Anomaly Detection with Decoupled Alert Dispatching. Scope & Requirements: Real-Time Statistical Anomaly Detector (Constitution Principle IV, partition-local EWMA, rolling variance/stddev, k*sigma anomaly bands, minimum variance epsilon guardrail), Alert Deduplication, Throttling & Hysteresis (suppression cooldown, auto-resolution after N consecutive nominal readings), Decoupled Asynchronous Alert Dispatcher (telemetry.alerts 12 partitions, dedicated async worker pool, HTTP webhooks, Slack/PagerDuty mock, exponential backoff with jitter and circuit breaker), Interface Contracts & Schemas (alerts.proto with AlertEvent), Observability & Verification Invariants (Prometheus metrics, chaos test with slow webhook endpoints ensuring zero stream processor lag)."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Real-Time Statistical Anomaly Detection via Dynamic Sigma Bands (Priority: P1) 🎯 MVP

Industrial plant operators and site reliability engineers manage fleets of thousands of sensors with widely varying nominal operating ranges. Manually configuring static warning thresholds for every individual metric across every machine is impossible and error-prone. The system automatically learns dynamic statistical baselines per machine and metric in real time using Exponentially Weighted Moving Average (EWMA) and rolling variance, flagging any reading that deviates beyond $k$-sigma bounds from its established mean.

**Why this priority**: Continuous, automated baseline learning and anomaly detection forms the core intelligence of the platform. Without automated detection, operators must react after catastrophic machinery failure occurs.

**Independent Test**: Ingest a steady baseline stream of 500 nominal sensor readings (e.g., mean 70.0°C with standard deviation 2.0°C) to train the baseline. Then inject a single reading of 85.0°C ($z \approx 7.5 > 3.0$). Verify that an anomaly is detected immediately and emitted as an alert record within 10 milliseconds of ingestion.

**Acceptance Scenarios**:

1. **Given** an established baseline for device `turbine-01` metric `temperature` with EWMA 70.0 and $\sigma = 2.0$, **When** a reading with value 78.5 arrives ($|78.5 - 70.0| = 8.5 > 3 \times 2.0$), **Then** the engine flags the reading as an anomaly, computes $z\text{-score} = 4.25$, and marks the alert severity as WARNING.
2. **Given** an established baseline with EWMA 70.0 and $\sigma = 2.0$, **When** an extreme reading with value 95.0 arrives ($z = 12.5 > 5.0$), **Then** the engine flags the reading as CRITICAL anomaly.
3. **Given** a flatlined sensor reporting identical values (e.g. exactly 50.0 for 200 consecutive readings, resulting in near-zero rolling variance), **When** a reading of 50.2 arrives, **Then** the engine enforces the minimum variance threshold ($\sigma^2 \ge \epsilon$) and does NOT trigger a false alert storm.

---

### User Story 2 - Decoupled Asynchronous Alert Dispatching (Priority: P1)

Downstream notification infrastructure (HTTP webhooks, enterprise incident management systems, Slack, PagerDuty) operates over external networks that frequently experience latency spikes, intermittent outages, or rate-limiting. Under Constitution Principle IV, downstream alert dispatching must never introduce backpressure, latency spikes, or consumer lag into the real-time stream ingestion and processing pipelines.

**Why this priority**: Enforces Constitution Principle IV. An external webhook provider suffering an outage or running with 30-second response latency must never degrade industrial telemetry ingestion or stream aggregation.

**Independent Test**: Configure a mock alert webhook endpoint that artificially delays responses by 30 seconds or drops connections. Feed high-throughput telemetry with injected anomalies through the pipeline, and verify that the stream processor maintains sub-second latency and zero consumer lag while the alert dispatcher buffers and delivers alerts asynchronously.

**Acceptance Scenarios**:

1. **Given** an anomaly detected by a partition worker, **When** the alert is emitted, **Then** it is published to broker topic `telemetry.alerts` without blocking subsequent telemetry readings on that partition.
2. **Given** the alert dispatcher worker pool consuming from `telemetry.alerts`, **When** the configured downstream webhook responds with HTTP 200 within 50ms, **Then** the alert delivery is recorded as successful and logged.
3. **Given** downstream webhook endpoints taking 30 seconds to respond, **When** 5,000 telemetry readings per second are processed, **Then** partition consumer lag remains at zero and window aggregate emission latency is completely unaffected.

---

### User Story 3 - Alert Deduplication, Throttling & Auto-Resolution Hysteresis (Priority: P2)

When equipment overheats or mechanical bearings degrade, the abnormal condition typically persists for minutes or hours until maintenance crews intervene. Telemetry arriving at 50 Hz during an incident would generate 3,000 alerts per minute if unthrottled, overwhelming on-call personnel and causing alert fatigue. The engine provides configurable alert suppression cooldowns and automatic incident resolution once nominal operation resumes.

**Why this priority**: Eliminates alert fatigue and flapped alerts in industrial production environments while maintaining auditability of incident state.

**Independent Test**: Continuously send 1,000 anomalous readings for device `motor-04` over a 10-minute period with a 5-minute cooldown configured. Verify that only 1 initial notification is dispatched at onset, followed by a reminder at the 5-minute mark, rather than 1,000 separate notifications. Then send 10 nominal readings and verify an auto-resolution alert (`RESOLVED`) is emitted.

**Acceptance Scenarios**:

1. **Given** an alert fired for `motor-04` at `10:00:00` with a 5-minute suppression window, **When** subsequent anomalous readings arrive at `10:00:05`, `10:01:00`, and `10:03:00`, **Then** these events update the active incident duration metric but do not trigger repetitive external notifications.
2. **Given** an ongoing incident with cooldown expired at `10:05:00`, **When** the anomaly persists at `10:05:01`, **Then** a follow-up escalation/reminder alert is dispatched.
3. **Given** an active firing alert, **When** 10 consecutive readings fall back within $1.5\sigma$ of the nominal mean, **Then** an auto-resolution alert event with status `RESOLVED` is dispatched, clearing the incident.

---

### User Story 4 - Multi-Sink Dispatching & Circuit Breaker Fault Tolerance (Priority: P2)

Industrial platforms must deliver alerts to different sinks based on severity (e.g. WARNINGs to Slack channels, CRITICALs to PagerDuty webhooks, and all alerts to an audit log). If an external endpoint repeatedly fails or times out, the dispatcher must trip a circuit breaker to protect worker resources and divert undeliverable notifications to a dead-letter queue.

**Why this priority**: Ensures platform resilience, avoids thread exhaustion, and guarantees reliable delivery with audit trails during external service degradations.

**Independent Test**: Point the dispatcher at a failing webhook endpoint returning HTTP 500. Verify that the dispatcher retries with exponential backoff and jitter, trips the circuit breaker after 5 consecutive failures, and routes failed alerts to a dead-letter sink without crashing or leaking memory.

**Acceptance Scenarios**:

1. **Given** an alert event with severity `CRITICAL`, **When** dispatched, **Then** it is formatted and delivered to both the primary webhook endpoint and the incident notification sink.
2. **Given** a webhook target returning HTTP 500 errors, **When** the dispatcher attempts delivery, **Then** it retries up to 3 times with exponential backoff (100ms, 200ms, 400ms) with random jitter.
3. **Given** 5 consecutive delivery failures to a specific sink, **When** subsequent alerts arrive, **Then** the circuit breaker opens for 30 seconds, immediately diverting alerts to dead-letter storage without blocking dispatcher threads.

---

### User Story 5 - Real-Time Anomaly & Alert Observability (Priority: P3)

Operations engineers need visibility into anomaly frequency, active firing incidents per machine, dispatcher queue depths, webhook success rates, and delivery latency distributions to monitor platform health.

**Why this priority**: Crucial for operational transparency, tuning statistical sensitivity parameters ($k$, $\alpha$, $\epsilon$), and verifying downstream delivery health.

**Independent Test**: Scrape Prometheus `/metrics` during simulated anomaly bursts and verify that active alert counts, anomaly rates, and dispatch latency histograms reflect actual pipeline events.

**Acceptance Scenarios**:

1. **Given** active stream processing with anomalies occurring, **When** Prometheus scrapes `/metrics`, **Then** metrics for `pulse_anomaly_detections_total`, `pulse_active_alerts_gauge`, and `pulse_alert_dispatch_latency_seconds` are updated accurately.
2. **Given** a downstream webhook failing, **Then** `pulse_alert_dispatch_failures_total` and `pulse_circuit_breaker_state` reflect the failure state.

---

### Edge Cases

- **Cold Start / Warmup Phase**: When a new device or metric begins streaming, sample count $N < 30$ is insufficient to establish a reliable variance. The engine uses a configurable warmup sample threshold (default: 30 samples) during which baselines are updated but alerts are suppressed or require higher confidence.
- **Flatline Sensor Signals**: A malfunctioning sensor sending an exact constant value (e.g. 50.0000) causes sample variance to collapse to 0.0. The engine enforces a minimum variance guardrail $\sigma^2 \ge \epsilon$ (default: $\epsilon = 10^{-4}$ or $0.01^2$) to prevent near-zero divisions and false-positive storms.
- **Step Change / Regime Shift**: A piece of machinery changes operational state (e.g., turbine speeds up from idle 1000 RPM to full power 3600 RPM). EWMA with smoothing parameter $\alpha$ (default: $\alpha = 0.05$) adapts to the new baseline over time without permanent lock-in.
- **Webhook Thundering Herd**: 100 machines experience power fluctuations simultaneously, producing 100 simultaneous alerts. The async dispatcher uses a bounded worker pool and rate limiter to prevent overwhelming external webhook endpoints.
- **Consumer Rebalance**: When partition consumers rebalance across nodes, partition-local EWMA states are re-initialized or restored without corrupting broker offset tracking.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST compute and maintain running statistical baselines per `(device_id, metric_name)` partition-locally without cross-goroutine locking on the hot ingestion path.
- **FR-002**: System MUST update statistical baselines using Exponentially Weighted Moving Average (EWMA) with configurable decay factor $\alpha$ ($0 < \alpha < 1$, default: $0.05$).
- **FR-003**: System MUST calculate rolling variance and standard deviation ($\sigma$) per baseline, incorporating a configurable minimum variance threshold $\sigma^2 \ge \epsilon$ (default: $\epsilon = 10^{-4}$) to prevent variance collapse.
- **FR-004**: System MUST evaluate every incoming reading against dynamic bands $|value - \text{EWMA}| > k \times \sigma$, where $k$ is configurable (default: $k=3.0$).
- **FR-005**: System MUST assign alert severity based on standard score ($z = \frac{|value - \text{EWMA}|}{\sigma}$): `INFO` ($2.5 \le z < 3.0$), `WARNING` ($3.0 \le z < 5.0$), and `CRITICAL` ($z \ge 5.0$).
- **FR-006**: System MUST suppress new notification dispatches for an ongoing anomaly on the same `(device_id, metric_name)` within a configurable cooldown window (default: 5 minutes).
- **FR-007**: System MUST detect when an anomalous metric returns within nominal tolerance ($|value - \text{EWMA}| \le 1.5\sigma$) for $N$ consecutive readings (default: $N=10$) and emit an auto-resolution alert with status `RESOLVED`.
- **FR-008**: System MUST publish all detected anomaly events to a dedicated message topic `telemetry.alerts` (12 partitions) keyed by `device_id`.
- **FR-009**: System MUST execute alert dispatching in an isolated, asynchronous worker pool decoupled from the stream processing engine, ensuring downstream webhook latency NEVER creates backpressure on stream ingestion.
- **FR-010**: System MUST support configurable alert delivery sinks including HTTP Webhooks (JSON payload), Slack/PagerDuty webhook formatting, and structured dead-letter logging.
- **FR-011**: System MUST implement exponential backoff with jitter (up to 3 retries) and a circuit breaker pattern (trip after 5 consecutive failures, cool-off for 30 seconds) for external webhook delivery.
- **FR-012**: System MUST expose Prometheus metrics for anomaly detection rate, active firing alerts, alert dispatch latency, dispatch retries/failures, and circuit breaker states.

---

### Key Entities

- **StatisticalBaseline**: In-memory partition-local running statistics:
  - `device_id` (string): Unique identifier of the device.
  - `metric_name` (string): Measured physical quantity.
  - `mean` (float64): Current EWMA estimate of the mean.
  - `variance` (float64): Current estimate of rolling variance.
  - `sample_count` (uint64): Number of processed observations.
  - `last_update_ns` (int64): Timestamp of last observed reading.
  - `active_incident` (bool): True if currently in a firing anomaly state.
  - `last_alert_time_ns` (int64): Timestamp of the last dispatched alert (for cooldown).
  - `consecutive_nominal_count` (int): Counter for auto-resolution hysteresis.

- **AlertEvent**: Protobuf wire contract for detected anomalies:
  - `alert_id` (string): UUIDv4 identifying the alert.
  - `device_id` (string): Machine or sensor identifier.
  - `metric_name` (string): Measured physical quantity.
  - `trigger_value` (float64): The raw measurement that triggered the alert.
  - `expected_mean` (float64): Learned baseline mean at trigger time.
  - `standard_deviation` (float64): Current baseline $\sigma$ at trigger time.
  - `z_score` (float64): Deviation standardized by standard deviation.
  - `severity` (enum): `SEVERITY_INFO`, `SEVERITY_WARNING`, `SEVERITY_CRITICAL`.
  - `status` (enum): `STATUS_FIRING`, `STATUS_RESOLVED`.
  - `timestamp_ns` (int64): Epoch nanoseconds when the event occurred.
  - `partition_id` (int32): Source partition that identified the anomaly.

- **CircuitBreakerState**: Tracks the operational health of external alert sinks:
  - `sink_name` (string): Name or URL of the downstream notification sink.
  - `state` (enum): `CLOSED` (Normal), `OPEN` (Tripped), `HALF_OPEN` (Probing).
  - `consecutive_failures` (int): Failure counter (trips to `OPEN` when $\ge 5$).
  - `cooldown_duration` (duration): Time to hold in `OPEN` state before probing (default: 30 seconds).
  - `last_state_change` (timestamp): Timestamp of the last state transition.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of injected anomalies exceeding $k$-sigma ($k \ge 3.0$) are detected and published to `telemetry.alerts` within 20 milliseconds of telemetry ingestion.
- **SC-002**: Under continuous downstream webhook failures or 30-second webhook latency, telemetry stream processor ingestion throughput sustains $\ge 50,000\text{ events/sec}$ with zero added consumer lag (0% degradation).
- **SC-003**: Alert suppression prevents redundant notifications by $> 99\%$ during continuous 10-minute machine failure simulations.
- **SC-004**: 100% of anomalous devices return an auto-resolution alert (`RESOLVED`) within 5 seconds of sensor values returning to nominal baseline bounds.
- **SC-005**: 100% of failed alert dispatches reaching maximum retries are routed to the dead-letter sink with zero lost alert payloads.

---

## Assumptions

- Telemetry readings on `telemetry.raw` continue to be keyed and partitioned by `device_id`, ensuring all readings for a given device arrive on the same partition worker.
- Memory consumption for running EWMA baselines ($< 128\text{ bytes}$ per metric) allows storing up to 100,000 active device baselines in $< 20\text{ MB}$ per partition.
- External webhook sinks accept JSON payloads via HTTP `POST` requests and indicate success via HTTP 2xx response codes.
- High-severity alerts (`CRITICAL`) can be dispatched to both standard webhooks and incident management integrations (e.g. PagerDuty mock).
