# Contract: Prometheus Alerting Rules & Alertmanager Routing

**Feature**: `006-unified-observability-dashboards`  
**Date**: 2026-10-26  
**Status**: Completed  

---

## 1. Alert Rule Definitions (`alert_rules.yml`)

### Rule 1: High Ingestion Error Rate (SLA Violation)
- **Alert Name**: `CriticalIngestionErrorRate`
- **Severity**: `critical`
- **Condition**:
  ```promql
  (
    sum(rate(pulse_ingest_errors_total[1m]))
    /
    (sum(rate(pulse_ingest_readings_total[1m])) + 0.001)
  ) * 100 > 0.10
  ```
- **Duration (`for`)**: `1m`
- **Summary**: "Gateway ingestion error rate exceeds 0.10% SLA threshold."
- **Description**: "Ingestion error rate is {{ $value | printf \"%.3f\" }}% over the last minute. Check for clock skew rejections, malformed JSON, or IEEE 754 non-finite numeric values."

---

### Rule 2: Consumer Group Lag SLA Breach
- **Alert Name**: `HighConsumerLag`
- **Severity**: `critical`
- **Condition**:
  ```promql
  sum by (group, partition) (
    redpanda_kafka_max_offset{topic="telemetry.raw"}
    -
    redpanda_kafka_consumer_group_committed_offset{topic="telemetry.raw"}
  ) > 250000
  ```
- **Duration (`for`)**: `30s`
- **Summary**: "Consumer lag on partition exceeds 5s equivalent throughput (250,000 events)."
- **Description**: "Consumer group '{{ $labels.group }}' on partition {{ $labels.partition }} has accumulated {{ $value }} unconsumed events. Potential worker thread stall or CPU saturation."

---

### Rule 3: Event-Time Watermark Stagnation
- **Alert Name**: `WatermarkStalled`
- **Severity**: `critical`
- **Condition**:
  ```promql
  sum by (partition) (rate(pulse_processor_events_total[1m])) > 50
  and
  sum by (partition) (delta(pulse_processor_watermark_timestamp_ns[30s])) == 0
  ```
- **Duration (`for`)**: `30s`
- **Summary**: "Event-time watermark has stopped progressing on partition."
- **Description**: "Partition {{ $labels.partition }} is consuming active events but watermark has remained completely flat for > 30 seconds. Potential clock inversion or idle partition starvation."

---

### Rule 4: Alert Dispatcher Circuit Breaker Open
- **Alert Name**: `AlertCircuitBreakerOpen`
- **Severity**: `warning`
- **Condition**:
  ```promql
  pulse_alert_circuit_breaker_state == 2
  ```
- **Duration (`for`)**: `10s`
- **Summary**: "Decoupled alert dispatcher circuit breaker has tripped to OPEN."
- **Description**: "Repeated downstream webhook delivery failures have tripped the circuit breaker to OPEN. Outgoing alerts are buffering and routing to dead-letter storage."

---

### Rule 5: Canary Latency SLA Violation
- **Alert Name**: `CanaryLatencySLABreached`
- **Severity**: `critical`
- **Condition**:
  ```promql
  pulse_canary_p95_latency_ms > 2000
  ```
- **Duration (`for`)**: `1m`
- **Summary**: "Synthetic canary end-to-end latency exceeds 2.0s SLA."
- **Description**: "End-to-end canary ingestion-to-query latency p95 is {{ $value }}ms (SLA is < 2000ms). Investigate stream processor window closure lag or ChronoDB write queuing."

---

### Rule 6: ChronoDB Storage Query Latency Degradation
- **Alert Name**: `StorageQueryLatencyHigh`
- **Severity**: `warning`
- **Condition**:
  ```promql
  histogram_quantile(0.95, sum(rate(pulse_storage_query_duration_seconds_bucket[1m])) by (le)) > 0.005
  ```
- **Duration (`for`)**: `1m`
- **Summary**: "ChronoDB p95 query latency exceeds 5.0ms."
- **Description**: "Sub-millisecond query performance has degraded to {{ $value | printf \"%.3f\" }}s. Inspect storage lock contention or memory eviction."

---

## 2. Alertmanager Routing Contract (`alertmanager.yml`)

```yaml
global:
  resolve_timeout: 1m

route:
  group_by: ['alertname', 'severity']
  group_wait: 5s
  group_interval: 15s
  repeat_interval: 1h
  receiver: 'default-webhook'
  routes:
    - match:
        severity: critical
      receiver: 'critical-webhook'
      repeat_interval: 15m

receivers:
  - name: 'default-webhook'
    webhook_configs:
      - url: 'http://host.docker.internal:8083/api/v1/alerts/webhook'
        send_resolved: true

  - name: 'critical-webhook'
    webhook_configs:
      - url: 'http://host.docker.internal:8083/api/v1/alerts/webhook'
        send_resolved: true
```
