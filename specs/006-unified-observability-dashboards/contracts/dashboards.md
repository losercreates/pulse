# Contract: Grafana Dashboards Specification

**Feature**: `006-unified-observability-dashboards`  
**Date**: 2026-10-26  
**Status**: Completed  

---

## 1. Dashboard Inventory

| Dashboard Title | UID | JSON Filename | Target Audience |
| :--- | :--- | :--- | :--- |
| **Pulse Executive Pipeline Overview** | `pulse-executive-overview` | `executive_overview.json` | Executive, SRE, Platform Leads |
| **Ingestion Gateway & Redpanda Partition Lag** | `pulse-ingest-broker-lag` | `ingest_broker_lag.json` | Operations & Data Engineers |
| **Stream Processor & Watermarking Health** | `pulse-stream-processor` | `stream_processor.json` | Stream Processing & Core Engineers |
| **Anomaly Detection & Decoupled Alerting** | `pulse-anomaly-alerts` | `anomaly_alerts.json` | Data Science & Incident Responders |
| **ChronoDB Storage & Analytical Query Engine** | `pulse-chronodb-storage` | `chronodb_storage.json` | Database & Storage Engineers |

---

## 2. Panel Specifications

### 2.1 Dashboard A: Pulse Executive Pipeline Overview (`pulse-executive-overview`)

1. **Total Ingestion Rate (Gauge / Stat)**:
   - Query: `sum(rate(pulse_ingest_readings_total[30s]))`
   - Thresholds: Green $> 45,000$, Yellow $> 20,000$, Red $< 20,000$.
   - Unit: `events/sec`
2. **Canary p95 Latency SLA (Gauge)**:
   - Query: `pulse_canary_p95_latency_ms`
   - Thresholds: Green $< 1,800$, Yellow $1,800 - 2,000$, Red $> 2,000$.
   - SLA Gate: Marker at $2,000\text{ ms}$.
3. **Ingestion Error Rate (Stat)**:
   - Query: `(sum(rate(pulse_ingest_errors_total[1m])) / (sum(rate(pulse_ingest_readings_total[1m])) + 0.001)) * 100`
   - Thresholds: Green $< 0.05\%$, Yellow $0.05\% - 0.10\%$, Red $> 0.10\%$.
4. **Active Incidents (Stat)**:
   - Query: `pulse_alert_active_incidents`
   - Thresholds: Green $0$, Yellow $1 - 5$, Red $> 5$.
5. **End-to-End Pipeline Volume (Time Series)**:
   - Queries:
     - `sum(rate(pulse_ingest_readings_total[1m]))` (Ingested)
     - `sum(rate(pulse_processor_events_total[1m]))` (Processed)
     - `sum(rate(pulse_storage_writes_total[1m]))` (Stored)

---

### 2.2 Dashboard B: Ingestion Gateway & Redpanda Partition Lag (`pulse-ingest-broker-lag`)

1. **Ingress Protocol Breakdown (Time Series)**:
   - Queries:
     - `sum by (transport) (rate(pulse_ingest_readings_total[30s]))` (gRPC streaming vs HTTP batching)
2. **Gateway Acknowledgement Latency Heatmap**:
   - Query: `sum(rate(pulse_ingest_duration_seconds_bucket[1m])) by (le)`
3. **Per-Partition Consumer Lag (12 Partitions Stacked Area Graph)**:
   - Query:
     ```promql
     sum by (partition) (
       redpanda_kafka_max_offset{topic="telemetry.raw"}
       -
       redpanda_kafka_consumer_group_committed_offset{group="pulse-stream-processor", topic="telemetry.raw"}
     )
     ```
   - Shows stacked lag for partitions 0 through 11.
4. **Ingestion Rejections by Reason (Time Series)**:
   - Query: `sum by (reason) (rate(pulse_ingest_errors_total[1m]))`

---

### 2.3 Dashboard C: Stream Processor & Watermarking Health (`pulse-stream-processor`)

1. **Event-Time Watermark Progression (Time Series)**:
   - Queries:
     - `pulse_processor_watermark_timestamp_ns / 1e9` (Watermark event-time)
     - `time()` (Current wall-clock time)
2. **Watermark Lag per Partition (Time Series)**:
   - Query: `(time() * 1000) - (pulse_processor_watermark_timestamp_ns / 1e6)`
   - Unit: milliseconds
3. **Open Window Min-Heap Priority Queue Depth (Time Series)**:
   - Query: `sum by (partition) (pulse_processor_open_windows)`
4. **Late Records Dropped (Dead-Letter) Rate**:
   - Query: `sum(rate(pulse_processor_late_records_total[1m]))`

---

### 2.4 Dashboard D: Anomaly Detection & Decoupled Alerting (`pulse-anomaly-alerts`)

1. **Real-Time Anomaly Detection Rate by Severity (Time Series)**:
   - Query: `sum by (severity) (rate(pulse_alert_events_total[30s]))`
2. **Circuit Breaker State Timeline (State Timeline)**:
   - Query: `pulse_alert_circuit_breaker_state`
   - Mapping: `0 = CLOSED (Green)`, `1 = HALF_OPEN (Yellow)`, `2 = OPEN (Red)`
3. **Alert Deduplication & Cooldown Suppression Rate (Time Series)**:
   - Query: `rate(pulse_alert_suppressed_total[1m])`
4. **Downstream Webhook Dispatch Latency Percentiles (Time Series)**:
   - Queries:
     - `histogram_quantile(0.50, sum(rate(pulse_alert_dispatch_latency_seconds_bucket[1m])) by (le))` (p50)
     - `histogram_quantile(0.95, sum(rate(pulse_alert_dispatch_latency_seconds_bucket[1m])) by (le))` (p95)
     - `histogram_quantile(0.99, sum(rate(pulse_alert_dispatch_latency_seconds_bucket[1m])) by (le))` (p99)

---

### 2.5 Dashboard E: ChronoDB Storage & Analytical Query Engine (`pulse-chronodb-storage`)

1. **Aggregate Storage Write Rate (Time Series)**:
   - Query: `sum(rate(pulse_storage_writes_total[30s]))`
2. **Batch Upsert Latency Percentiles (Time Series)**:
   - Query: `histogram_quantile(0.95, sum(rate(pulse_storage_batch_duration_seconds_bucket[1m])) by (le))`
3. **Historical Range Query Latency Heatmap**:
   - Query: `sum(rate(pulse_storage_query_duration_seconds_bucket{query_type="range"}[1m])) by (le)`
4. **Dynamic Downsampling Rollup Execution Time (Time Series)**:
   - Query: `histogram_quantile(0.95, sum(rate(pulse_storage_query_duration_seconds_bucket{query_type="rollup"}[1m])) by (le))`
5. **Shard Balance Heatmap (16 Shards)**:
   - Query: `pulse_storage_active_shards`
