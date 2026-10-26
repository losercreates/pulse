# Research & Architectural Decisions: Milestone 006 — Unified Multi-Service Observability & Executive Grafana Dashboards

**Feature**: `006-unified-observability-dashboards`  
**Date**: 2026-10-26  
**Status**: Completed  

---

## 1. Overview & Research Scope

Milestone 006 introduces a unified, production-grade observability infrastructure for the Pulse platform. This research analyzes:
1. High-frequency Prometheus scraping patterns for streaming microservices and Redpanda brokers.
2. Declarative, automated Grafana provisioning (dashboards & data sources) without manual UI interaction.
3. PromQL query designs for per-partition consumer lag across 12 partitions and event-time watermark progression.
4. Prometheus alerting rules and Alertmanager routing for platform SLA enforcement.

---

## 2. Architectural Decisions & Rationales

### Decision 1: Centralized Prometheus Scrape Configuration & Relabeling

- **Decision**: Configure a unified Prometheus configuration (`deployments/prometheus/prometheus.yml`) scraping all 5 platform components at a 5-second interval:
  - `pulse-gateway`: `gateway:8080` (or `host.docker.internal:8080`)
  - `pulse-processor`: `processor:8081` (or `host.docker.internal:8081`)
  - `pulse-alertd`: `alertd:8083` (or `host.docker.internal:8083`)
  - `pulse-chronod`: `chronod:8084` (or `host.docker.internal:8084`)
  - `pulse-redpanda`: `redpanda:9644` (broker public Prometheus metrics)
- **Rationale**: 
  - A 5-second scrape interval provides sufficient granularity to detect brief consumer lag spikes and burst transitions while imposing $< 0.1\%$ CPU overhead on the services.
  - Standard Prometheus Go client scrapers format metrics in OpenMetrics/Prometheus text exposition format with zero memory allocations on scrape.
- **Alternatives Considered**:
  - *Pushgateway*: Rejected because platform components are long-running daemons, not ephemeral batch jobs; pull-based scraping provides automatic target liveness detection (`up == 0`).
  - *OpenTelemetry Collector*: Evaluated as an extra layer; rejected for local development complexity since Prometheus directly scrapes native `/metrics` endpoints.

---

### Decision 2: Declarative Grafana Provisioning (Dashboards as Code)

- **Decision**: Use Grafana's file-based provisioning system (`deployments/grafana/provisioning/`) with two subdirectories:
  1. `datasources/prometheus.yaml`: Automatically configures `http://prometheus:9090` as default data source with `jsonData.timeInterval: "5s"`.
  2. `dashboards/dashboards.yaml`: Automatically loads dashboard JSON definitions from `/etc/grafana/dashboards/` with `updateIntervalSeconds: 10`.
- **Rationale**:
  - Eliminates manual setup, password entry, or clicking "Import Dashboard" in the Grafana UI.
  - Dashboards are checked into version control as JSON files, making them reviewable, versioned, and reproducible across developer machines and CI.
- **Alternatives Considered**:
  - *Grafana API curl scripts*: Rejected because it requires waiting for Grafana to boot and handling authentication tokens. Native provisioning handles ordering automatically.

---

### Decision 3: PromQL Metrics & Consumer Lag Calculation

- **Decision**:
  - For per-partition consumer lag: Combine Redpanda high watermark offset with consumer group committed offsets:
    $$\text{Lag}(p) = \text{redpanda\_kafka\_max\_offset}\{topic="telemetry.raw", partition="p"\} - \text{redpanda\_kafka\_consumer\_group\_committed\_offset}\{group="pulse-stream-processor", partition="p"\}$$
  - For event-time watermark lag: Compare current wall-clock epoch time with the partition worker's current watermark:
    $$\text{WatermarkLagMs}(p) = (\text{time}() \times 1000) - (\text{pulse\_processor\_watermark\_timestamp\_ns}\{partition="p"\} / 10^6)$$
  - For aggregate throughput: Use per-second rate over a 30s window:
    $$\text{Throughput} = \text{sum}(\text{rate}(\text{pulse\_ingest\_readings\_total}[30s]))$$
- **Rationale**:
  - Exposes both macro system health (aggregate rate) and micro partition health (individual worker lag on partitions 0–11).
  - Pinpoints single-partition bottlenecks or slow consumer goroutines instantly.

---

### Decision 4: Alertmanager Routing & SLA Alert Rule Design

- **Decision**:
  - Configure Alertmanager with a default logging/webhook receiver and group alerts by `alertname` and `severity`.
  - Implement 6 core SLA alert rules in `deployments/prometheus/alert_rules.yml`:
    1. `IngestionErrorRateHigh`: `rate(pulse_ingest_errors_total[1m]) / rate(pulse_ingest_readings_total[1m]) > 0.001` (error rate $> 0.10\%$, `for: 1m`).
    2. `ConsumerLagBreached`: `redpanda_consumer_lag > 250000` or lag time $> 5\text{s}$ (`for: 30s`).
    3. `WatermarkStalled`: `rate(pulse_processor_events_total[1m]) > 100` and `delta(pulse_processor_watermark_timestamp_ns[30s]) == 0` (`for: 30s`).
    4. `AlertCircuitBreakerOpen`: `pulse_alert_circuit_breaker_state == 2` (`for: 10s`).
    5. `CanaryLatencySLAViolated`: `pulse_canary_p95_latency_ms > 2000` (`for: 1m`).
    6. `StorageQueryLatencyHigh`: `histogram_quantile(0.95, rate(pulse_storage_query_duration_seconds_bucket[1m])) > 0.005` (`for: 1m`).
- **Rationale**:
  - Matches the exact SLA and Constitution constraints ratified in Constitution Operational Constraints and Milestone 005 contracts.
  - Adding `for:` durations prevents transient burst spikes from triggering false-positive alerts.

---

### Decision 5: Docker Compose Network & Container Architecture

- **Decision**:
  - Enhance `deployments/docker-compose.yml` with services:
    - `redpanda`: broker (`:9092`), schema registry (`:8081` internal, `:18081` external), admin/metrics (`:9644`).
    - `prometheus`: `prom/prometheus:v2.54.1`, port `:9090`.
    - `grafana`: `grafana/grafana:11.1.0`, port `:3000`.
    - `alertmanager`: `prom/alertmanager:v0.27.0`, port `:9093`.
  - Mount configurations via local directory volumes:
    - `./prometheus/:/etc/prometheus/`
    - `./grafana/provisioning/:/etc/grafana/provisioning/`
    - `./grafana/dashboards/:/etc/grafana/dashboards/`
    - `./alertmanager/:/etc/alertmanager/`
  - Use `extra_hosts: ["host.docker.internal:host-gateway"]` so Prometheus inside Docker can seamlessly scrape locally running microservice binaries (`gateway`, `processor`, `alertd`, `chronod`) running on the host!
- **Rationale**:
  - Enables developers to run services either inside Docker or natively as Go binaries on the host while monitoring them with Docker Compose Prometheus and Grafana.
