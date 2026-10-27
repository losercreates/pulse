# Technical Research: Milestone 007 — Production Container Packaging & Kubernetes Deployment

**Feature**: `007-k8s-production-deployment`  
**Date**: 2026-10-27  
**Status**: Completed  

---

## 1. Container Image Architecture & Distroless Base Layers

### Context
All 5 platform services (`cmd/gateway`, `cmd/processor`, `cmd/alertd`, `cmd/chronod`, `cmd/simulator`) are compiled Go 1.23+ binaries. Production deployment mandates minimal attack surface, non-root execution, reproducible builds, and small image footprints ($< 50\text{MB}$).

### Decision
Use a standardized multi-stage build pattern:
- **Build Stage**: `golang:1.23-alpine` with `CGO_ENABLED=0`, `GOOS=linux`, `GOARCH=amd64`, compiling static binaries with ldflags `-s -w` to strip symbol tables and debug information.
- **Runtime Stage**: `gcr.io/distroless/static-debian12:nonroot` (or `alpine:3.20` for services requiring shell probes). For Kubernetes, distroless `static-debian12:nonroot` is preferred as it contains CA certificates, timezone data, and a non-root user (`nonroot:nonroot` UID/GID `65532:65532`) with zero package managers, shells, or coreutils.
- **Root Filesystem**: Compatible with `readOnlyRootFilesystem: true`. Temporary files or socket mounts use an `emptyDir` mount at `/tmp`.

### Rationale
- Stripped static Go binaries compiled with `CGO_ENABLED=0` are self-contained and require only Linux kernel syscalls and root CA certs.
- Distroless images drastically reduce vulnerability surface area (zero CVEs from shell or package manager packages).
- Image footprint is reduced from $> 300\text{MB}$ (full alpine/debian) to $\sim 15 - 30\text{MB}$ per microservice.

### Alternatives Considered
- `scratch`: While smallest, `scratch` lacks `/etc/ssl/certs/ca-certificates.crt` and `/etc/passwd` by default. `distroless/static-debian12:nonroot` provides these securely without adding shell binaries.
- `alpine:latest` runtime: Adds `apk` and `sh`, which triggers vulnerability scanners and fails strict enterprise production container policies.

---

## 2. Helm Chart Architecture & Parameterization

### Context
A modular deployment requires managing 4 persistent daemons (`gateway`, `processor`, `alertd`, `chronod`) and 1 batch runner (`simulator`), interacting with Redpanda and Prometheus.

### Decision
Structure the Helm chart as a single unified chart in `deployments/helm/pulse`:
```text
deployments/helm/pulse/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── configmap.yaml
│   ├── secret.yaml
│   ├── gateway-deployment.yaml
│   ├── gateway-service.yaml
│   ├── gateway-hpa.yaml
│   ├── processor-deployment.yaml
│   ├── processor-service.yaml
│   ├── processor-hpa.yaml
│   ├── alertd-deployment.yaml
│   ├── alertd-service.yaml
│   ├── chronod-deployment.yaml
│   ├── chronod-service.yaml
│   ├── pdb.yaml
│   └── simulator-job.yaml
```

### Rationale
- Parameterized `values.yaml` allows per-environment overrides (`global.imageRegistry`, `global.redpandaBrokers`, `global.environment`).
- Clean separation of concerns per microservice template while sharing global helper templates (`pulse.fullname`, `pulse.labels`, `pulse.selectorLabels`).

---

## 3. Horizontal Pod Autoscaling (HPA) & Consumer Lag Boundaries

### Context
- Ingestion Gateway is stateless and CPU-bound on parsing and TLS termination.
- Stream Processor is stateful and partition-bound: Redpanda has 12 partitions on `telemetry.raw`.
- Constitution Principle I states: *"The concurrency model enforces exactly one goroutine per Kafka/Redpanda partition consumer. Cross-goroutine locking on the message ingestion hot path is strictly prohibited."*

### Decision
1. **Gateway HPA**:
   - `minReplicas: 2`, `maxReplicas: 10`
   - Scale target: Average CPU utilization $> 75\%$ or HTTP request throughput via Custom Metrics API.
2. **Stream Processor HPA**:
   - `minReplicas: 1`, `maxReplicas: 12` (**strictly capped at 12**)
   - Scale target: External/Custom metric `pulse_stream_consumer_lag` (or average CPU $> 75\%$).
   - Because franz-go consumer groups assign whole partitions, scaling beyond 12 pods results in idle pods consuming 0 partitions, wasting resources and generating unnecessary broker rebalance churn.
3. **Stabilization Windows**:
   - Scale-up: 15-second stabilization window for fast reaction to bursts.
   - Scale-down: 300-second stabilization window to prevent flapping during momentary watermark catch-ups.

---

## 4. Graceful Shutdown & Zero-Loss Rolling Termination

### Context
Abrupt pod termination causes dropped client connections, incomplete stream processing windows, and lost offset commits.

### Decision
1. **Termination Grace Period**: Set `terminationGracePeriodSeconds: 30` across all Deployment specs.
2. **Signal Handling**: All services already handle `SIGTERM` and `os.Interrupt` via root `context.WithCancel(context.Background())`.
3. **Order of Shutdown**:
   - `gateway`: Closes HTTP/gRPC listeners, flushes buffered Franz-go producer batches to Redpanda, then exits.
   - `processor`: Stops consuming new records, allows active open window accumulators to commit offsets, then exits.
   - `chronod`: Completes ongoing batch memory upserts, fsyncs if configured, closes query listeners, then exits.
   - `alertd`: Flushes active dispatch queue to webhook or dead-letter sink before closing.

---

## 5. Health Probes & Resource Allocations

### Decision
1. **Probes**:
   - Gateway: Liveness `GET :8080/healthz`, Readiness `GET :8080/healthz` (`initialDelaySeconds: 5`, `periodSeconds: 5`)
   - Processor: Liveness `GET :8081/healthz`, Readiness `GET :8081/healthz` (`initialDelaySeconds: 5`, `periodSeconds: 5`)
   - Alertd: Liveness `GET :8083/healthz`, Readiness `GET :8083/healthz` (`initialDelaySeconds: 5`, `periodSeconds: 5`)
   - Chronod: Liveness `GET :8084/healthz`, Readiness `GET :8084/healthz` (`initialDelaySeconds: 5`, `periodSeconds: 5`)
2. **Resource Requests & Limits**:
   - Standard baseline:
     - `gateway`: Requests `100m CPU / 128Mi RAM`, Limits `1000m CPU / 512Mi RAM`
     - `processor`: Requests `200m CPU / 256Mi RAM`, Limits `2000m CPU / 1Gi RAM`
     - `alertd`: Requests `50m CPU / 64Mi RAM`, Limits `500m CPU / 256Mi RAM`
     - `chronod`: Requests `200m CPU / 256Mi RAM`, Limits `2000m CPU / 2Gi RAM`
