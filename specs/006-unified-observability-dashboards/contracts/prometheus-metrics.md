# Contract: Platform Prometheus Metrics Dictionary

**Feature**: `006-unified-observability-dashboards`  
**Date**: 2026-10-26  
**Status**: Completed  

---

## 1. Ingestion Gateway Metrics (`:8080/metrics`)

| Metric Name | Type | Labels | Description |
| :--- | :---: | :--- | :--- |
| `pulse_ingest_readings_total` | Counter | `transport="grpc\|http"`, `status="accepted\|rejected"` | Total telemetry readings received by transport and acceptance outcome |
| `pulse_ingest_batches_total` | Counter | `transport="grpc\|http"` | Total batch submissions |
| `pulse_ingest_errors_total` | Counter | `transport="grpc\|http"`, `reason="..."` | Ingestion errors (clock skew, IEEE 754 NaN, length boundary) |
| `pulse_ingest_duration_seconds`| Histogram| `transport="grpc\|http"` | End-to-end gateway ingestion acknowledgement latency |

---

## 2. Stream Processor Metrics (`:8081/metrics`)

| Metric Name | Type | Labels | Description |
| :--- | :---: | :--- | :--- |
| `pulse_processor_events_total` | Counter | `partition="0..11"` | Total raw events consumed from Redpanda per partition |
| `pulse_processor_watermark_timestamp_ns` | Gauge | `partition="0..11"` | Current monotonic event-time watermark in nanoseconds per partition |
| `pulse_processor_open_windows` | Gauge | `partition="0..11"` | Count of active open windows currently tracked in min-heap priority queue |
| `pulse_processor_late_records_total` | Counter | `partition="0..11"` | Records arriving after window closure rejected to `telemetry.late` dead-letter sink |
| `pulse_processor_windows_closed_total` | Counter | `partition="0..11"` | Cumulative closed windows emitted to `telemetry.aggregates` |

---

## 3. Decoupled Alert Dispatcher Metrics (`:8083/metrics`)

| Metric Name | Type | Labels | Description |
| :--- | :---: | :--- | :--- |
| `pulse_alert_events_total` | Counter | `severity="info\|warning\|critical"`, `status="emitted\|dispatched\|dropped"` | Total alert events processed by dispatcher |
| `pulse_alert_active_incidents` | Gauge | — | Total currently active incidents undergoing tracking or cooldown |
| `pulse_alert_suppressed_total` | Counter | `reason="cooldown"` | Duplicate alert notifications suppressed within cooldown window |
| `pulse_alert_circuit_breaker_state` | Gauge | — | State of the alert webhook circuit breaker (`0=CLOSED`, `1=HALF_OPEN`, `2=OPEN`) |
| `pulse_alert_dispatch_latency_seconds` | Histogram | `endpoint="..."` | Duration of downstream webhook delivery attempts |
| `pulse_alert_deadletter_total` | Counter | — | Alerts routed to `telemetry.alerts.deadletter` after retry exhaustion |

---

## 4. ChronoDB Storage Sink & Query Service Metrics (`:8084/metrics`)

| Metric Name | Type | Labels | Description |
| :--- | :---: | :--- | :--- |
| `pulse_storage_writes_total` | Counter | `status="success\|error"` | Total window aggregate records persisted into sharded memory |
| `pulse_storage_batch_duration_seconds` | Histogram | — | Storage batch upsert execution latency |
| `pulse_storage_query_duration_seconds` | Histogram | `query_type="range\|rollup"` | Historical range query and dynamic rollup execution latency |
| `pulse_storage_active_shards` | Gauge | — | Total operational storage shards (fixed 16-shard partition) |
| `pulse_storage_replays_deduplicated_total` | Counter | — | Idempotent upsert re-evaluations confirming zero duplicate rows |

---

## 5. Redpanda Broker Cluster Metrics (`:9644/public_metrics`)

| Metric Name | Type | Labels | Description |
| :--- | :---: | :--- | :--- |
| `redpanda_kafka_max_offset` | Gauge | `topic="telemetry.raw\|telemetry.aggregates\|telemetry.alerts"`, `partition="0..11"` | High watermark log end offset for topic partitions |
| `redpanda_kafka_consumer_group_committed_offset` | Gauge | `group="pulse-stream-processor\|pulse-chronodb-consumer"`, `topic="..."`, `partition="0..11"` | Current committed offset for consumer groups |
| `redpanda_kafka_request_bytes_total` | Counter | `type="produce\|fetch"` | Volume of ingress and egress network throughput |
