# Implementation Plan: Milestone 006 — Unified Multi-Service Observability & Grafana Dashboards

**Branch**: `006-unified-observability-dashboards` | **Date**: 2026-10-26 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/006-unified-observability-dashboards/spec.md`

## Summary

Deploy an enterprise-grade unified observability and monitoring suite for the Pulse streaming platform. This milestone orchestrates Prometheus, Grafana, and Alertmanager alongside the Redpanda messaging cluster and platform microservices using Docker Compose. It provisions centralized scraping covering all 5 subsystems (`gateway :8080`, `processor :8081`, `alertd :8083`, `chronod :8084`, and `redpanda :9644`), auto-provisions 5 specialized Grafana dashboards with executive SLA gauges and deep per-partition consumer lag metrics, and defines Prometheus SLA alert rules routed through Alertmanager to enforce zero-blindspot platform reliability.

## Technical Context

**Language/Version**: Go 1.23+ (Platform Services), YAML/JSON (Prometheus, Grafana, Alertmanager configurations).

**Primary Dependencies**:
- Prometheus `v2.54.1`: Centralized metric timeseries scraper and PromQL evaluation engine.
- Grafana `11.1.0`: Visual dashboard rendering with automated file-based provisioning.
- Alertmanager `v0.27.0`: Alert deduplication, grouping, and notification dispatching.
- Docker Compose v2+: Multi-container service orchestration and bridge networking.

**Storage**: Prometheus local TSDB with 15-day retention and 10GB volume cap; Grafana SQLite local storage with persistent volume mount.

**Testing**:
- Configuration syntax and rule validation via `promtool check rules` and `amtool check-config`.
- Scrape target health validation via Prometheus API (`/api/v1/targets`).
- Grafana dashboard JSON schema linting and PromQL query syntax validation.
- Integration tests in `tests/integration/observability_test.go` verifying scrape discovery and metrics alignment.

**Target Platform**: Linux / macOS containerized development and CI environment.

**Project Type**: Distributed systems telemetry infrastructure, declarative provisioning, and operational dashboards.

**Performance Goals**:
- Scrape interval: 5 seconds.
- Dashboard initial render latency: $< 2.0\text{s}$.
- Alert evaluation cycle: 15 seconds.
- Time-to-alert for critical SLA breaches: $< 15\text{s}$.

**Constraints**:
- Metric scrape payload: $< 5\text{MB}$ per scrape (bounded aggregate series, avoiding high-cardinality device labels in Prometheus).
- Zero CPU impact on the hot message streaming path (Prometheus scrapes run asynchronously over HTTP).
- 100% declarative provisioning (zero manual UI clicks or credentials prompts on cluster bootstrap).

**Scale/Scope**:
- 5 platform scrape targets.
- 12 Redpanda topic partitions tracked individually.
- 5 comprehensive Grafana dashboards.
- 6 critical SLA alert rules.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle / Gate | Compliance Analysis | Status |
| :--- | :--- | :---: |
| **I. Partition-Local State & Zero-Lock Ingestion** | Prometheus scrapers pull metrics asynchronously over HTTP from existing Go atomic counters and EWMA snapshots. Zero locks are added to the message ingestion path. | **PASS** |
| **II. Event-Time Watermarking** | Observability stack passively charts event-time watermark progression against wall-clock time and triggers alerts if watermarks stall; it does not alter watermark priority queue logic. | **PASS** |
| **III. Idempotent Ingestion & Delivery** | Dashboards chart ChronoDB batch write volume and idempotent deduplication counts, confirming zero double-counted records during consumer replay. | **PASS** |
| **IV. Decoupled Asynchronous Side-Effects** | Alertmanager is completely decoupled from stream processor ingestion. Alert dispatcher queue depths and circuit breaker states are tracked independently. | **PASS** |
| **V. Polyglot Storage Boundary** | ChronoDB metrics are scraped cleanly across its designated HTTP observability port `:8084` without crossing internal storage boundaries. | **PASS** |

---

## Project Structure

### Documentation (this feature)

```text
specs/006-unified-observability-dashboards/
├── spec.md                     # Feature specification
├── plan.md                     # Implementation plan
├── research.md                 # Technical research & architectural decisions
├── data-model.md               # Telemetry entities & PromQL metrics models
├── quickstart.md               # Runnable validation guide
├── checklists/
│   └── requirements.md         # Requirements quality verification
└── contracts/
    ├── prometheus-metrics.md   # Platform metrics dictionary
    ├── alert-rules.md          # Prometheus alert rules & Alertmanager routing
    └── dashboards.md           # Grafana dashboard panels & PromQL queries
```

### Source Code (repository root)

```text
deployments/
├── docker-compose.yml          # Multi-container orchestration (Redpanda, Prometheus, Grafana, Alertmanager)
├── prometheus/
│   ├── prometheus.yml          # Prometheus scrape config & target endpoints
│   └── alert_rules.yml         # Prometheus SLA alert definitions
├── alertmanager/
│   └── alertmanager.yml        # Alert routing, grouping, and receivers
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── prometheus.yaml # Default Prometheus datasource definition
    │   └── dashboards/
    │       └── dashboards.yaml # Dashboard provider configuration
    └── dashboards/
        ├── executive_overview.json   # Dashboard A: Pulse Executive Pipeline Overview
        ├── ingest_broker_lag.json    # Dashboard B: Ingestion Gateway & Redpanda Partition Lag
        ├── stream_processor.json     # Dashboard C: Stream Processor & Watermarking Health
        ├── anomaly_alerts.json       # Dashboard D: Anomaly Detection & Decoupled Alerting
        └── chronodb_storage.json     # Dashboard E: ChronoDB Storage & Analytical Query Engine

tests/integration/
└── observability_test.go       # Automated verification of scrape targets & dashboard PromQL queries
```

**Structure Decision**: Infrastructure-as-code and declarative configuration placed in `deployments/`, keeping platform binaries decoupled from monitoring infrastructure while allowing seamless Docker Compose orchestration.

---

## Complexity Tracking

> No violations of the Constitution identified.
