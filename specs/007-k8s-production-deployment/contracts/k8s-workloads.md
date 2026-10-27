# Contract: Kubernetes Workload Specifications

**Feature**: `007-k8s-production-deployment`  
**Date**: 2026-10-27  
**Status**: Completed  

---

## 1. Gateway Workload Contract

- **Kind**: `Deployment`
- **Replicas**: Managed by HPA (default `2`, bounds `2..10`)
- **Container Name**: `gateway`
- **Ports**:
  - `http`: `8080/TCP` (REST ingestion & health)
  - `grpc`: `50051/TCP` (Streaming gRPC)
- **Environment Variables**:
  - `PORT`: `"8080"`
  - `GRPC_PORT`: `"50051"`
  - `KAFKA_BROKERS`: From ConfigMap `PULSE_KAFKA_BROKERS`
  - `TOPIC`: From ConfigMap `PULSE_TOPIC_RAW`
- **Liveness Probe**: `httpGet: { path: /healthz, port: http }`, `periodSeconds: 5`, `timeoutSeconds: 2`
- **Readiness Probe**: `httpGet: { path: /healthz, port: http }`, `periodSeconds: 5`, `timeoutSeconds: 2`
- **Termination Grace Period**: `30s`

---

## 2. Stream Processor Workload Contract

- **Kind**: `Deployment`
- **Replicas**: Managed by HPA (default `2`, bounds `1..12`)
- **Constitution Gate**: Max replicas capped at `12` (matches 12 Redpanda topic partitions)
- **Container Name**: `processor`
- **Ports**:
  - `http`: `8081/TCP` (Health & Prometheus `/metrics`)
- **Environment Variables**:
  - `METRICS_PORT`: `"8081"`
  - `KAFKA_BROKERS`: From ConfigMap `PULSE_KAFKA_BROKERS`
  - `CONSUMER_GROUP`: `"pulse-stream-processor"`
  - `INPUT_TOPIC`: From ConfigMap `PULSE_TOPIC_RAW`
  - `OUTPUT_TOPIC`: From ConfigMap `PULSE_TOPIC_AGGREGATES`
  - `LATE_TOPIC`: From ConfigMap `PULSE_TOPIC_LATE`
  - `ALERT_TOPIC`: From ConfigMap `PULSE_TOPIC_ALERTS`
- **Liveness & Readiness**: `httpGet: { path: /healthz, port: http }`
- **Termination Grace Period**: `30s` (flushes in-flight window checkpoints before pod exit)

---

## 3. Decoupled Alert Dispatcher Workload Contract

- **Kind**: `Deployment`
- **Replicas**: Default `1` (or `1..3`)
- **Container Name**: `alertd`
- **Ports**:
  - `http`: `8083/TCP` (Health & Prometheus `/metrics`)
- **Environment Variables**:
  - `METRICS_PORT`: `"8083"`
  - `KAFKA_BROKERS`: From ConfigMap `PULSE_KAFKA_BROKERS`
  - `CONSUMER_GROUP`: `"pulse-alert-dispatcher"`
  - `INPUT_TOPIC`: From ConfigMap `PULSE_TOPIC_ALERTS`
  - `DEADLETTER_TOPIC`: From ConfigMap `PULSE_TOPIC_ALERTS_DEADLETTER`
  - `WEBHOOK_URL`: From Secret `PULSE_ALERT_WEBHOOK_URL`
- **Liveness & Readiness**: `httpGet: { path: /healthz, port: http }`
- **Termination Grace Period**: `30s`

---

## 4. ChronoDB Storage Sink & Query Service Contract

- **Kind**: `Deployment`
- **Replicas**: Default `1`
- **Container Name**: `chronod`
- **Ports**:
  - `http`: `8084/TCP` (REST Query API & `/healthz`)
  - `grpc`: `50054/TCP` (gRPC storage boundary)
- **Environment Variables**:
  - `PORT`: `"8084"`
  - `GRPC_PORT`: `"50054"`
  - `KAFKA_BROKERS`: From ConfigMap `PULSE_KAFKA_BROKERS`
  - `CONSUMER_GROUP`: `"pulse-chronodb-consumer"`
  - `TOPIC`: From ConfigMap `PULSE_TOPIC_AGGREGATES`
  - `SHARD_COUNT`: `"16"`
- **Liveness & Readiness**: `httpGet: { path: /healthz, port: http }`
- **Termination Grace Period**: `30s`

---

## 5. Simulator Batch Verification Job Contract

- **Kind**: `Job`
- **Restart Policy**: `Never`
- **Backoff Limit**: `0`
- **Command / Args**:
  - `--rate`, `25000`
  - `--devices`, `500`
  - `--duration`, `30s`
  - `--profile`, `mixed`
  - `--transport`, `grpc`
  - `--gateway-grpc`, `pulse-gateway:50051`
  - `--chrono-query`, `pulse-chronod:8084`
  - `--json`
