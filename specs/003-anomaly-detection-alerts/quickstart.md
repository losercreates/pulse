# Quickstart Validation Guide: Anomaly Detection & Decoupled Alerting

**Feature**: `003-anomaly-detection-alerts`
**Date**: 2026-10-22
**Status**: Ready for Implementation

---

## 1. Prerequisites

- **Docker Compose**: Redpanda cluster with topics `telemetry.raw`, `telemetry.aggregates`, `telemetry.late`, and `telemetry.alerts` (12 partitions each).
- **Go 1.23+**: Installed locally.
- **Compiled Protobufs**: `telemetry.proto`, `aggregates.proto`, and `alerts.proto` compiled in `proto/v1/`.

---

## 2. Infrastructure Setup: Start Broker & Verify Topics

```bash
make up
```

Verify that topic `telemetry.alerts` exists:
```bash
docker compose -f deployments/docker-compose.yml exec redpanda \
  rpk topic list | grep telemetry.alerts
```

---

## 3. End-to-End Validation Scenarios

### Scenario A: Real-Time Anomaly Detection & Alert Emission
1. Feed 50 nominal telemetry readings for `turbine-01` (`temperature = 70.0 ± 1.0`°C) to establish the baseline.
2. Ingest an anomalous spike: `temperature = 92.5`°C ($z > 10.0$).
3. Inspect `telemetry.alerts` to confirm the emitted `AlertEvent`:
```bash
docker compose -f deployments/docker-compose.yml exec redpanda \
  rpk topic consume telemetry.alerts -n 1 --format '%k | %v\n'
```
*Expected Output: `AlertEvent` with `severity = CRITICAL`, `status = FIRING`, `z_score > 10.0`, emitted within 20ms.*

---

### Scenario B: Decoupled Dispatching with Hanging Webhook (Constitution Principle IV)
1. Start a mock webhook server with a 30-second sleep response delay:
```bash
# Starts mock server listening on :8089 delaying all POST requests by 30s
go run tests/mock/slow_webhook.go -port 8089 -delay 30s &
```
2. Run stream processor and alert dispatcher configured with webhook sink `http://localhost:8089/webhook`.
3. Stream 10,000 telemetry readings with 10 injected anomalies.
4. Verify that the stream processor maintains $\ge 50,000\text{ events/sec}$ with zero consumer lag while the dispatcher handles the slow webhook asynchronously.

---

### Scenario C: Cooldown Suppression & Auto-Resolution Hysteresis
1. Continuous Anomaly: Ingest 100 consecutive anomalous readings ($95^\circ\text{C}$) over 2 minutes.
   - Verify only **1** `STATUS_FIRING` notification is dispatched (suppressing the other 99).
2. Recovery: Ingest 10 consecutive nominal readings ($70^\circ\text{C}$).
   - Verify an auto-resolution `AlertEvent` with `status = RESOLVED` is emitted.

---

### Scenario D: Circuit Breaker Tripping on Downstream Outage
1. Point alert dispatcher at an invalid or crashing webhook endpoint returning HTTP 500.
2. Emit 10 anomalies.
3. Observe circuit breaker status transitioning from `CLOSED` to `OPEN` after 5 failures:
```bash
curl -s http://localhost:8083/metrics | grep pulse_circuit_breaker
```
4. Verify subsequent alerts are diverted to dead-letter logging without thread blocking.

---

## 4. Automated Tests & Invariants

```bash
# Run unit tests for EWMA baseline and variance guardrails
go test -v ./internal/alert/detector/...

# Run unit tests for circuit breaker and dispatcher retry logic
go test -v ./internal/alert/dispatcher/...

# Run decoupled slow-webhook chaos integration test
go test -v ./tests/integration/decoupled_alert_test.go
```
