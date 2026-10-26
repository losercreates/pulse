# Quickstart Validation Guide: Milestone 006 — Unified Observability & Grafana Dashboards

**Feature**: `006-unified-observability-dashboards`  
**Date**: 2026-10-26  
**Status**: Completed  

---

## 1. Prerequisites

Ensure Docker Compose and Go 1.23+ are installed:

```bash
# 1. Compile all platform binaries (gateway, processor, alertd, chronod, simulator)
make build
```

---

## 2. Start Observability & Messaging Stack

Start the unified Docker Compose stack containing Redpanda, Prometheus, Grafana, and Alertmanager:

```bash
# Start all infrastructure daemons
make obs-up
# or
docker compose -f deployments/docker-compose.yml up -d
```

Verify container health checks:
```bash
docker compose -f deployments/docker-compose.yml ps
```

*Expected Output*:
- `pulse-redpanda`: healthy (ports `9092`, `9644`)
- `pulse-prometheus`: healthy (port `9090`)
- `pulse-grafana`: healthy (port `3000`)
- `pulse-alertmanager`: healthy (port `9093`)

To view dashboard links at any time:
```bash
make dashboards
```

---

## 3. Automated Configuration & Rules Verification

Run the built-in observability verification harness to validate all scrape configurations, SLA alert rules, and Grafana dashboard JSON models:

```bash
./scripts/verify_observability.sh
```

---

## 4. Start Core Platform Daemons

In separate terminal tabs or in the background, start the 4 platform services:

```bash
./bin/gateway &
./bin/processor &
./bin/alertd &
./bin/chronod &
```

---

## 5. Validation Scenarios

### Scenario A: Verify Prometheus Scrape Targets

Query Prometheus API to confirm all platform endpoints are discovered and actively scraped:

```bash
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health, lastScrape: .lastScrape}'
```

*Expected Verification*:
- `pulse-gateway`: `health: "up"`
- `pulse-processor`: `health: "up"`
- `pulse-alertd`: `health: "up"`
- `pulse-chronod`: `health: "up"`
- `pulse-redpanda`: `health: "up"`

---

### Scenario B: Verify Declarative Grafana Dashboards

Open your browser to Grafana at [http://localhost:3000](http://localhost:3000).

*Expected Verification*:
- Anonymous admin access enabled (no login credentials required, or login with `admin`/`admin`).
- Navigate to **Dashboards** → Folder **Pulse Platform**:
  1. `Pulse Executive Pipeline Overview` (`pulse-executive-overview`)
  2. `Ingestion Gateway & Redpanda Partition Lag` (`pulse-ingest-broker-lag`)
  3. `Stream Processor & Watermarking Health` (`pulse-stream-processor`)
  4. `Anomaly Detection & Decoupled Alerting` (`pulse-anomaly-alerts`)
  5. `ChronoDB Storage & Analytical Query Engine` (`pulse-chronodb-storage`)
- All panels load without datasource errors.

---

### Scenario C: Live Load Stress & Dashboard Telemetry

Drive high-throughput load across the platform using the fleet simulator:

```bash
./bin/simulator --rate 25000 --devices 500 --duration 60s --profile mixed --transport grpc
```

*Expected Verification*:
- In **Pulse Executive Pipeline Overview**: Throughput gauge jumps to `~25,000 ev/s`, Canary p95 gauge reads `< 2.0s`.
- In **Ingestion Gateway & Redpanda Partition Lag**: Stacked area graph displays real-time lag across all 12 partitions.
- In **Stream Processor & Watermarking Health**: Event-time watermark advances synchronously with incoming timestamps.
- In **Anomaly Detection & Decoupled Alerting**: Anomaly count panels register injected step spikes and flatlines.
- In **ChronoDB Storage**: Storage write rate matches window closure frequency.

---

### Scenario D: Verify SLA Alert Rules & Alertmanager

Query Prometheus alert rules:

```bash
curl -s http://localhost:9090/api/v1/rules | jq '.data.groups[].rules[] | {alert: .name, state: .state}'
```

The 6 SLA rules enforced in `deployments/prometheus/alert_rules.yml`:
1. `CriticalIngestionErrorRate`: Gateway error rate $> 0.10\%$ over 1m
2. `HighConsumerLag`: Consumer lag $> 250,000$ events over 30s
3. `WatermarkStalled`: Watermark lag $> 15\text{s}$ with active traffic over 30s
4. `AlertCircuitBreakerOpen`: Alert dispatcher circuit breaker trips to `OPEN` for 10s
5. `CanaryLatencySLABreached`: Synthetic canary p95 latency $> 2000\text{ms}$ over 1m
6. `StorageQueryLatencyHigh`: ChronoDB p95 query latency $> 5.0\text{ms}$ over 1m

Check Alertmanager at [http://localhost:9093](http://localhost:9093):
- Alert transitions from `pending` to `firing` after configured `for:` duration.
- Alert is routed to configured receiver.
