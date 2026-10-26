# Feature Specification: Milestone 006 — Unified Multi-Service Observability & Executive Grafana Dashboards

**Feature Branch**: `006-unified-observability-dashboards`  
**Created**: 2026-10-26  
**Status**: Draft  
**Input**: User description: "Create milestone 006: Unified multi-service observability and executive Grafana dashboards integrating Prometheus, Grafana, and Alertmanager across Docker Compose to monitor Gateway throughput, Redpanda 12-partition consumer lag, event-time watermark progression, statistical anomaly/circuit-breaker states, and ChronoDB sub-millisecond storage/query latencies with automated SLA alert rules"

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Centralized Multi-Service Metrics Ingestion & Platform Prometheus Scraping (Priority: P1) 🎯 MVP

As a Platform Reliability Engineer, I need a centralized Prometheus scraping configuration that automatically pulls operational telemetry from all microservices and the messaging broker so that the entire streaming pipeline can be monitored from a single pane of glass without blind spots.

**Why this priority**: Without reliable multi-service metric collection and metric unification across all layers, visual dashboards and automated alerting cannot function. This provides the MVP data ingestion foundation for all observability.

**Independent Test**: Spin up the streaming platform services alongside Prometheus; execute `curl -s http://localhost:9090/api/v1/targets | jq` and verify that all service scrape targets report state `health: "up"` with zero scrape errors.

**Acceptance Scenarios**:

1. **Given** the Ingestion Gateway (`:8080`), Stream Processor (`:8081`), Alert Dispatcher (`:8083`), ChronoDB Storage (`:8084`), and Redpanda Broker (`:9644`) are running, **When** Prometheus scrapes their endpoints at the configured 5-second interval, **Then** all scrape targets are reported in `UP` state in Prometheus with active sample ingestion.
2. **Given** Prometheus is actively scraping, **When** querying for core metrics (`pulse_ingest_readings_total`, `pulse_processor_events_total`, `pulse_alert_events_total`, `pulse_storage_writes_total`), **Then** values increment monotonically matching actual workload traffic.

---

### User Story 2 - Executive & Subsystem Grafana Dashboards (Priority: P1)

As an Operations Engineer and Technical Stakeholder, I need pre-provisioned Grafana dashboards automatically loaded upon container startup so that I can immediately visualize real-time pipeline throughput, per-partition broker lag, event-time watermarking health, anomaly alerts, and sub-millisecond ChronoDB query performance without manual dashboard creation.

**Why this priority**: Operational insight requires visual representations of complex distributed invariants (watermark progression vs wall-clock time, 12-partition consumer group lag, circuit breaker transitions, and latency percentiles).

**Independent Test**: Launch Grafana (`:3000`) and verify all 5 pre-provisioned dashboards load cleanly, connect to the Prometheus data source without credentials prompts, and render real-time graphs during active load generation.

**Acceptance Scenarios**:

1. **Given** the **Pulse Executive Pipeline Overview** dashboard, **When** load is injected via the simulator, **Then** the dashboard displays instantaneous aggregate throughput (events/sec), total processed readings, canary p95 latency against the 2.0s SLA gauge, and active incident counts.
2. **Given** the **Ingestion & Broker Lag** dashboard, **When** events flow through Redpanda, **Then** per-partition consumer lag is displayed as a stacked area graph across all 12 partitions for both stream processor and storage consumer groups.
3. **Given** the **Stream Processor & Watermarking** dashboard, **When** out-of-order events arrive, **Then** the dashboard charts the event-time watermark progression against wall-clock time, watermark lag (ms), open tumbling window count, and late-data dead-letter drop rate.
4. **Given** the **Anomaly Detection & Decoupled Alerting** dashboard, **When** anomalies occur, **Then** the dashboard plots dynamic $k$-sigma bounds, anomaly counts by severity (`INFO`, `WARNING`, `CRITICAL`), active incident lifecycles, and circuit breaker state timelines.
5. **Given** the **ChronoDB Storage & Historical Query** dashboard, **When** window aggregates are flushed and historical queries are executed, **Then** the dashboard charts batch write throughput, p50/p95/p99 query latencies, dynamic rollup downsampling execution times, and 16-shard load balancing.

---

### User Story 3 - Automated SLA Alerting & Notification Rules via Alertmanager (Priority: P2)

As an On-Call Engineer, I need automated Prometheus alert rules evaluated continuously by Alertmanager so that team members are proactively alerted when platform SLAs (error rates, consumer lag, watermark stalls, canary latency, or circuit breaker trips) are breached.

**Why this priority**: Passive dashboards require human eyes; automated alerting guarantees that critical SLA breaches and platform degradation trigger immediate notification and incident management.

**Independent Test**: Inject a synthetic SLA failure (e.g. trip the alert dispatcher circuit breaker or inject high error rates); verify Alertmanager fires the alert within 15 seconds and routes the alert payload to configured notification webhooks.

**Acceptance Scenarios**:

1. **Given** Ingestion Gateway socket error rate exceeds $0.10\%$ for $> 1\text{ minute}$, **When** alert rules evaluate, **Then** Alertmanager fires a `CriticalIngestionErrorRate` alert.
2. **Given** Redpanda consumer lag on any partition exceeds 5 seconds of unconsumed messages, **When** alert rules evaluate, **Then** Alertmanager fires a `HighConsumerLag` alert citing the lagging partition and consumer group.
3. **Given** Stream Processor watermark does not advance for $> 15\text{ seconds}$ while raw events are arriving, **When** alert rules evaluate, **Then** Alertmanager fires a `WatermarkStalled` alert.
4. **Given** `alertd` circuit breaker transitions to `OPEN` state due to webhook delivery failures, **When** alert rules evaluate, **Then** Alertmanager fires a `CircuitBreakerOpen` alert.
5. **Given** Canary latency p95 exceeds 2.0 seconds, **When** alert rules evaluate, **Then** Alertmanager fires a `CanarySLALatencyBreached` alert.

---

### User Story 4 - Single-Command Compose Deployment & Local Observability Stack (Priority: P2)

As a Developer or DevOps Engineer, I need Docker Compose automation bundling Redpanda, Prometheus, Grafana, and Alertmanager so that the entire operational telemetry environment boots with a single command (`make up`) with zero manual configuration.

**Why this priority**: Observability infrastructure must be repeatable, ephemeral, and integrated into local development and CI testing environments.

**Independent Test**: Run `make up` (or `docker compose -f deployments/docker-compose.yml up -d`); assert all 4 monitoring containers start healthy, configuration files mount without permission errors, and Grafana data sources are automatically provisioned.

**Acceptance Scenarios**:

1. **Given** a fresh clone, **When** executing `docker compose up -d`, **Then** Prometheus (`:9090`), Grafana (`:3000`), Alertmanager (`:9093`), and Redpanda (`:9092`, `:9644`) start with passing container health checks.
2. **Given** containers are running, **When** containers restart, **Then** provisioned dashboards and data source configs persist without manual UI setup.

---

### Edge Cases

- **Service Restart or Temporary Disconnect**: If a service restarts (e.g. `gateway` or `processor`), Prometheus scrape targets temporarily mark `DOWN` and automatically recover to `UP` without flapping or corrupting historical metric counters (`rate()` and `increase()` handle counter resets gracefully).
- **High-Throughput Metric Cardinality Spikes**: If simulated fleets emit high device cardinalities ($10,000+$ devices), Prometheus scrapers record bounded aggregated metrics (`pulse_processor_events_total`, `pulse_storage_writes_total`) rather than unbounded per-device Prometheus timeseries, keeping metric scrape payloads $< 5\text{MB}$ and preventing Prometheus OOM.
- **Redpanda Broker Rebalance**: During partition reassignment, temporary consumer lag spikes are absorbed and visualized without causing false-positive flapping alerts by enforcing duration thresholds (`for: 1m` or `for: 30s`) in alert rules.
- **Alertmanager Webhook Unavailability**: If downstream incident receivers (e.g. Slack/PagerDuty webhooks) are unreachable, Alertmanager retries with exponential backoff and maintains a local memory buffer without blocking Prometheus alert evaluations.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a Prometheus configuration (`deployments/prometheus/prometheus.yml`) scraping Ingestion Gateway (`:8080`), Stream Processor (`:8081`), Alert Dispatcher (`:8083`), ChronoDB Storage (`:8084`), and Redpanda Broker (`:9644`) every 5 seconds.
- **FR-002**: System MUST configure Prometheus with 15-day local data retention and a maximum local storage size of 10GB.
- **FR-003**: System MUST provision Grafana automatically via configuration files (`deployments/grafana/provisioning/datasources/prometheus.yaml` and `deployments/grafana/provisioning/dashboards/dashboards.yaml`) without requiring manual UI configuration.
- **FR-004**: System MUST provide the **Pulse Executive Pipeline Overview** Grafana dashboard displaying aggregate ingestion rate, end-to-end processing volume, canary p95 latency gauge, and pipeline operational status.
- **FR-005**: System MUST provide the **Ingestion Gateway & Redpanda Partition Lag** Grafana dashboard charting gRPC streaming vs HTTP batching rates, rejected payloads, and per-partition consumer lag across all 12 partitions.
- **FR-006**: System MUST provide the **Stream Processor & Watermarking** Grafana dashboard charting event-time watermark progression, watermark lag (ms), active window min-heap priority queue depth, and late-data dead-letter drop rate.
- **FR-007**: System MUST provide the **Anomaly Detection & Decoupled Alerting** Grafana dashboard charting real-time $k$-sigma deviation rates, severity distribution (`INFO`, `WARNING`, `CRITICAL`), active incident count, cooldown deduplication rate, circuit breaker state timeline, and webhook latency percentiles ($p50, p95, p99$).
- **FR-008**: System MUST provide the **ChronoDB Storage & Analytical Query Engine** Grafana dashboard charting storage batch write throughput, batch write latency, historical time-range query latency distribution, dynamic multi-resolution downsampling rollup durations, and 16-shard load distribution.
- **FR-009**: System MUST define Prometheus SLA alert rules (`deployments/prometheus/alert_rules.yml`) enforcing platform constraints:
  - Ingestion error rate $> 0.10\%$ for $> 1\text{m}$.
  - Partition consumer lag $> 5\text{s}$ for $> 30\text{s}$.
  - Watermark stagnation for $> 15\text{s}$ while raw events are arriving.
  - Alert dispatcher circuit breaker state is `OPEN`.
  - Canary p95 end-to-end latency $> 2000\text{ms}$.
  - ChronoDB p95 range query latency $> 5\text{ms}$.
- **FR-010**: System MUST configure Alertmanager (`deployments/alertmanager/alertmanager.yml`) with grouping, deduplication intervals, and configurable notification receivers (webhook, log).
- **FR-011**: System MUST update `deployments/docker-compose.yml` to define services for `prometheus`, `grafana`, and `alertmanager` interconnected on the `pulse-network` with persistent data volumes.
- **FR-012**: System MUST add Makefile targets (`make obs`, `make dashboards`) and documentation detailing observability stack ports, dashboard access, and alert simulation.

---

### Key Entities *(include if feature involves data)*

- **ScrapeTarget**: A network address and HTTP endpoint exposed by a platform service providing Prometheus-formatted text metrics.
- **MetricSeries**: A timestamped numerical timeseries defined by metric name, labels (e.g. `partition`, `status`, `severity`), and value.
- **GrafanaDashboard**: A declarative JSON model defining panels, graph queries (PromQL), thresholds, and layout for operational monitoring.
- **AlertRule**: A declarative condition evaluated over Prometheus timeseries with duration thresholds (`for`) and severity annotations (`critical`, `warning`).
- **AlertNotification**: A structured event payload dispatched by Alertmanager to external notification receivers containing active incident details.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of platform services and messaging brokers are discovered and scraped by Prometheus with zero persistent scrape failures under normal operation.
- **SC-002**: Operators can assess end-to-end platform health, throughput, and SLA compliance within 5 seconds of opening the Executive Grafana Dashboard.
- **SC-003**: Per-partition consumer lag across all 12 Redpanda partitions is visualized in real-time with sub-5-second metric latency.
- **SC-004**: Critical SLA breaches (e.g. consumer lag $> 5\text{s}$, canary latency $> 2.0\text{s}$, or circuit breaker `OPEN`) trigger an Alertmanager notification within 15 seconds of the condition occurring.
- **SC-005**: All Grafana dashboards and Prometheus datasources are 100% provisioned via declarative code, requiring zero manual clicks or credentials prompts on fresh cluster bootstrap.
- **SC-006**: Complete observability infrastructure starts up via a single command (`docker compose up -d` or `make up`) in under 10 seconds.

---

## Assumptions

- Prometheus version `v2.54.0+`, Grafana version `11.1.0+`, and Alertmanager version `v0.27.0+` container images are pulled from Docker Hub.
- Default Grafana local credentials are set to `admin:admin` with anonymous viewing enabled for local development.
- Scraped platform components continue exposing `/metrics` on designated ports: Gateway (`:8080`), Processor (`:8081`), Alert Dispatcher (`:8083`), ChronoDB (`:8084`), and Redpanda (`:9644`).
- Alertmanager notification receiver defaults to a local mock webhook/logger for offline development, easily overridden via environment variables for Slack or PagerDuty.
- Prometheus scrape interval of 5 seconds provides high-fidelity resolution for real-time streaming operations without placing excessive CPU overhead on scraped services.
