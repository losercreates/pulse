# Quickstart Validation Guide: Milestone 006 — Unified Observability & Grafana Dashboards

**Feature**: `006-unified-observability-dashboards`  
**Date**: 2026-10-26  
**Status**: Ready for Implementation  

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
make up
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

---

## 3. Start Core Platform Daemons

In separate terminal tabs or in the background, start the 4 platform services:

```bash
./bin/gateway &
./bin/processor &
./bin/alertd &
./bin/chronod &
```

---

## 4. Validation Scenarios

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
- No login required (or login with `admin`/`admin`).
- Navigate to **Dashboards** → Folder **Pulse Platform**:
  1. `Pulse Executive Pipeline Overview`
  2. `Ingestion Gateway & Redpanda Partition Lag`
  3. `Stream Processor & Watermarking Health`
  4. `Anomaly Detection & Decoupled Alerting`
  5. `ChronoDB Storage & Analytical Query Engine`
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

Test alert evaluation by intentionally tripping a condition (e.g. stopping `chronod` to induce consumer lag or tripping the circuit breaker).

Check Alertmanager at [http://localhost:9093](http://localhost:9093):
- Alert transitions from `pending` to `firing` after configured `for:` duration.
- Alert is routed to configured receiver.
