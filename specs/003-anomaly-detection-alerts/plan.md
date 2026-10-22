# Implementation Plan: Real-Time Anomaly Detection & Decoupled Alerting

**Branch**: `003-anomaly-detection-alerts` | **Date**: 2026-10-22 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/003-anomaly-detection-alerts/spec.md`

## Summary

Build the real-time statistical anomaly detection engine and decoupled asynchronous alert dispatcher for Pulse. The engine computes online dynamic statistical baselines per `(device_id, metric_name)` using Exponentially Weighted Moving Average (EWMA) and rolling variance within partition-local memory with zero hot-path locks, evaluating readings against dynamic $k$-sigma bounds with minimum variance guardrails ($\sigma^2 \ge \epsilon$). Detected anomalies are published to Redpanda topic `telemetry.alerts` (12 partitions). An independent asynchronous alert dispatcher consumes alerts, manages cooldown suppression and auto-resolution hysteresis, formats notifications for multiple sinks (HTTP Webhooks, Slack/PagerDuty), and enforces resilient delivery via exponential backoff with jitter and circuit breaking (Constitution Principle IV).

## Technical Context

**Language/Version**: Go 1.23+

**Primary Dependencies**:
- `github.com/twmb/franz-go` (v1.22+): Kafka consumer & producer client for `telemetry.alerts`.
- `google.golang.org/protobuf`: Protobuf serialization for `AlertEvent`.
- `github.com/prometheus/client_golang`: Streaming metrics exporter for anomaly rates, dispatch latencies, and circuit breaker states.
- Standard `net/http`: HTTP webhook client with configured timeouts, keep-alives, and connection pooling.

**Storage**: Redpanda (Topics: `telemetry.raw` [source], `telemetry.alerts` [12 partitions, alert stream], `telemetry.alerts.deadletter` [unreachable sink dead-letter]).

**Testing**: Standard Go `testing`, `testify/assert`, slow-webhook chaos integration tests, property-based baseline tests.

**Target Platform**: Linux / macOS server.

**Project Type**: Real-time statistical analytics & event-driven notification service.

**Performance Goals**:
- Anomaly baseline update & evaluation latency $< 100\text{ ns}$ per reading on the partition worker.
- Dispatcher worker pool sustains $\ge 1,000\text{ alerts/sec}$.
- Downstream webhook latency of 30 seconds causes **zero** backpressure or lag on stream processor ingestion (Constitution Principle IV).

**Constraints**:
- Partition-local baseline state with zero cross-thread locking on the hot ingestion path (Constitution Principle I).
- Decoupled asynchronous alert dispatching (Constitution Principle IV).
- Circuit breaker trips after 5 consecutive downstream delivery failures.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle / Gate | Compliance Analysis | Status |
| :--- | :--- | :---: |
| **I. Partition-Local State & Zero-Lock Ingestion Hot Path** | Anomaly baseline evaluation (EWMA and variance updates) executes strictly inside the partition worker goroutine without mutexes or cross-thread locking. | **PASS** |
| **II. Event-Time Watermarking** | Anomaly detection evaluates synchronously during partition ingestion and does not interfere with watermark tracking or window boundary closures. | **PASS** |
| **III. Idempotent Ingestion & At-Least-Once Delivery** | Alert events are published with unique `alert_id` (UUIDv4) and partition offsets are checkpointed durably. | **PASS** |
| **IV. Decoupled Asynchronous Side-Effects** | **Directly enforced**. Stream processor publishes anomalies to Redpanda topic `telemetry.alerts`. The alert dispatcher runs in an isolated worker pool. Webhook outages or slow endpoints **never** block or lag stream processing. | **PASS** |
| **V. Explicit Polyglot Storage Boundary** | Alerts are published to Redpanda; ChronoDB historical integration is deferred to Milestone 004. | **PASS** |

---

## Project Structure

### Documentation (this feature)

```text
specs/003-anomaly-detection-alerts/
├── spec.md                  # Feature Specification
├── plan.md                  # This file (Implementation Plan)
├── research.md              # Phase 0 Technical Decisions & Algorithms
├── data-model.md            # Phase 1 Domain Entities & State Machines
├── quickstart.md            # Phase 1 Validation & Test Run Guide
├── contracts/
│   └── alerts.proto         # Protobuf v3 AlertEvent, AlertSeverity, AlertStatus
└── checklists/
    └── requirements.md      # Specification Quality Checklist
```

### Source Code (repository root layout)

```text
pulse/
├── cmd/
│   ├── gateway/main.go
│   ├── processor/main.go           # Stream processor (evaluates anomalies -> publishes to telemetry.alerts)
│   └── alertd/main.go              # Decoupled alert dispatcher daemon
├── proto/
│   └── v1/
│       ├── telemetry.pb.go
│       ├── aggregates.pb.go
│       └── alerts.pb.go            # Generated from contracts/alerts.proto
├── internal/
│   ├── alert/
│   │   ├── config/
│   │   │   └── config.go           # Alpha, k-sigma, epsilon, cooldown, webhook URLs
│   │   ├── detector/
│   │   │   ├── baseline.go         # Online EWMA, rolling variance, variance floor guardrail
│   │   │   ├── detector.go         # Anomaly evaluation & z-score threshold classification
│   │   │   └── baseline_test.go    # Unit tests for baseline convergence and flatline guardrail
│   │   ├── dispatcher/
│   │   │   ├── dispatcher.go       # Async consumer loop & worker pool for telemetry.alerts
│   │   │   ├── circuit_breaker.go  # 3-state circuit breaker (Closed, Open, Half-Open)
│   │   │   ├── retry.go            # Exponential backoff with jitter
│   │   │   └── dispatcher_test.go  # Unit & mock tests for retry and circuit breaking
│   │   ├── incident/
│   │   │   ├── tracker.go          # Cooldown suppression & consecutive nominal auto-resolution
│   │   │   └── tracker_test.go     # Unit tests for hysteresis and auto-resolution
│   │   ├── sink/
│   │   │   ├── sink.go             # Sink interface definition
│   │   │   ├── webhook.go          # HTTP POST JSON webhook sink
│   │   │   └── deadletter.go       # Dead-letter sink for unroutable alerts
│   │   └── metrics/
│   │       └── metrics.go          # Prometheus metrics (detection rate, dispatch latency, circuit breaker)
│   ├── processor/ ...
│   └── gateway/ ...
├── deployments/
│   └── docker-compose.yml          # Updated with telemetry.alerts (12 partitions)
└── tests/
    └── integration/
        └── decoupled_alert_test.go # Chaos test: 30s slow webhook causes 0% stream processor lag
```

---

## Complexity Tracking

> **Constitution Check: 0 violations. All principles satisfied.**

| Violation | Why Needed | Simpler Alternative Rejected Because |
| :--- | :--- | :--- |
| *None* | Architecture strictly complies with Constitution Principles I–V | N/A |
