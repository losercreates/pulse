# Quickstart Validation Guide: Milestone 005 — Fleet Load Simulator & Chaos Harness

**Feature**: `005-fleet-load-simulator`  
**Date**: 2026-10-24  
**Status**: Implemented & Verified  

---

## 1. Prerequisites

Ensure the full Pulse streaming cluster is compiled and running locally:

```bash
# 1. Start Redpanda Cluster
make up

# 2. Compile Protobufs & Build All Binaries (gateway, processor, alertd, chronod, simulator)
make build

# 3. Start Core Platform Services in Separate Terminals (or background)
./bin/gateway &
./bin/processor &
./bin/alertd &
./bin/chronod &
```

Verify service health probes:
```bash
curl -s http://localhost:8080/healthz | jq   # Gateway (:8080)
curl -s http://localhost:8081/healthz | jq   # Processor (:8081)
curl -s http://localhost:8083/healthz | jq   # Alert Dispatcher (:8083)
curl -s http://localhost:8084/healthz | jq   # ChronoDB Storage (:8084)
```

---

## 2. Validation Scenarios

### Scenario A: Baseline 50,000 Events/Sec SLA Validation (gRPC Streaming)

Run a continuous 60-second steady-state load test across 1,000 devices driving $\ge 50,000\text{ events/sec}$ over gRPC client streaming:

```bash
./bin/simulator --rate 50000 --devices 1000 --duration 60s --transport grpc --json
```

*Expected Verification*:
- Command line prints active progress and terminates with exit code `0`.
- Output JSON reports `achieved_rate >= 47500` ($95\%$ of target rate).
- Total sent readings $\ge 3,000,000$.
- `total_failed == 0` (0% socket errors, 0% dropped packets).
- ChronoDB write metrics increment proportionally:
  ```bash
  curl -s http://localhost:8084/metrics | grep pulse_storage_writes_total
  ```

---

### Scenario B: Bursty Traffic Surge & Broker Buffer Absorption

Simulate a sudden $4\times$ surge in load (jumping from 20,000 to 80,000 events/sec) for 15 seconds:

```bash
./bin/simulator --rate 20000 --devices 1000 --profile burst --burst-multiplier 4.0 --burst-duration 15s --duration 45s
```

*Expected Verification*:
- Ingestion Gateway and Redpanda broker absorb peak 80,000 events/sec without TCP disconnects.
- Redpanda consumer lag momentarily increases during the burst and fully clears within 15 seconds after the burst completes:
  ```bash
  docker compose -f deployments/docker-compose.yml exec -T redpanda \
    rpk group describe pulse-stream-processor
  ```

---

### Scenario C: Offline Network Disconnect & Out-of-Order Backfill Chaos

Simulate $10\%$ of the device fleet losing network connectivity for 20 seconds, accumulating telemetry locally, and dumping out-of-order backfill upon reconnection:

```bash
./bin/simulator --rate 10000 --devices 500 --profile offline --offline-pct 0.10 --offline-duration 20s --duration 45s
```

*Expected Verification*:
- Reconnected devices dump backfilled packets in bursts.
- Readings timestamped prior to $(watermark - allowed\_lateness)$ are safely rejected to `telemetry.late` dead-letter sink:
  ```bash
  curl -s http://localhost:8081/metrics | grep pulse_processor_late_records_total
  ```
- Previously closed window aggregates in ChronoDB remain strictly invariant (zero retroactive mutation).

---

### Scenario D: Sensor Anomaly Chaos Injection & Decoupled Alerting

Inject $5\%$ deliberate statistical anomalies (step spikes $z \ge 5.0$, variance flatlines) across the fleet:

```bash
./bin/simulator --rate 5000 --devices 200 --profile anomaly --anomaly-pct 0.05 --duration 30s
```

*Expected Verification*:
- Real-time anomaly detector flags $k$-sigma deviations and emits `AlertEvent` protobufs to `telemetry.alerts`.
- Decoupled `alertd` daemon consumes and dispatches alerts without degrading stream processor throughput:
  ```bash
  curl -s http://localhost:8083/metrics | grep pulse_alert_events_total
  ```

---

### Scenario E: End-to-End Canary Latency Verification (SLA Gate: p95 < 2s)

Run continuous canary latency probes sampling end-to-end time from sensor creation to visibility in ChronoDB:

```bash
./bin/simulator --rate 25000 --devices 500 --canary-interval 5s --duration 60s --json | jq .results.canary_p95_latency_ms
```

*Expected Verification*:
- Canary p95 latency is reported $< 2,000\text{ ms}$ ($< 2.0\text{s}$), fulfilling Constitution Operational Constraint § Performance Targets.

---

## 3. Automated Benchmark Verification

```bash
# Run simulator unit and pacing tests
go test -v ./internal/simulator/...

# Run high-throughput driver benchmark
go test -bench=BenchmarkSimulator ./tests/bench/...
```
