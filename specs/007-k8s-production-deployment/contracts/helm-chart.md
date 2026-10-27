# Contract: Helm Chart Hierarchy & Parameterization

**Feature**: `007-k8s-production-deployment`  
**Date**: 2026-10-27  
**Status**: Completed  

---

## 1. Chart Metadata (`Chart.yaml`)

```yaml
apiVersion: v2
name: pulse
description: Production Helm chart for Pulse real-time telemetry streaming and processing platform
type: application
version: 0.1.0
appVersion: "1.0.0"
keywords:
  - telemetry
  - streaming
  - redpanda
  - watermarking
  - chronodb
maintainers:
  - name: Pulse Platform Engineering
```

---

## 2. Standard Helper Definitions (`_helpers.tpl`)

- `pulse.name`: Truncated chart name.
- `pulse.fullname`: Release-prefixed fully qualified resource name.
- `pulse.labels`: Standard Kubernetes recommended labels:
  - `helm.sh/chart: {{ include "pulse.chart" . }}`
  - `app.kubernetes.io/name: {{ include "pulse.name" . }}`
  - `app.kubernetes.io/instance: {{ .Release.Name }}`
  - `app.kubernetes.io/managed-by: {{ .Release.Service }}`
- `pulse.selectorLabels`: Component-specific selector labels:
  - `app.kubernetes.io/name: {{ include "pulse.name" . }}`
  - `app.kubernetes.io/instance: {{ .Release.Name }}`
  - `app.kubernetes.io/component: {{ .Component }}`

---

## 3. Template Manifest Inventory

| Template File | Kind | Description |
| :--- | :--- | :--- |
| `templates/configmap.yaml` | `ConfigMap` | Broker endpoints, topics, ports, and default parameters |
| `templates/secret.yaml` | `Secret` | Sensitive auth tokens and alert webhook URLs |
| `templates/gateway-deployment.yaml` | `Deployment` | Dual-transport ingestion gateway (REST `:8080`, gRPC `:50051`) |
| `templates/gateway-service.yaml` | `Service` | LoadBalancer / ClusterIP routing to gateway pods |
| `templates/gateway-hpa.yaml` | `HorizontalPodAutoscaler` | Scales gateway 2–10 replicas on CPU utilization |
| `templates/processor-deployment.yaml`| `Deployment` | Stream processing core with watermarks & windows (`:8081`) |
| `templates/processor-service.yaml` | `Service` | Metrics and health check endpoint routing |
| `templates/processor-hpa.yaml` | `HorizontalPodAutoscaler` | Scales processor 1–12 replicas on consumer lag / CPU |
| `templates/alertd-deployment.yaml` | `Deployment` | Decoupled alert dispatcher with circuit breaker (`:8083`) |
| `templates/alertd-service.yaml` | `Service` | Metrics and alert webhook routing |
| `templates/chronod-deployment.yaml` | `Deployment` | Sharded ChronoDB storage & analytical query engine (`:8084`, `:50054`) |
| `templates/chronod-service.yaml` | `Service` | Query API and storage boundary service |
| `templates/pdb.yaml` | `PodDisruptionBudget` | High availability constraints during voluntary disruptions |
| `templates/simulator-job.yaml` | `Job` | On-demand load generation and SLA compliance test job |
