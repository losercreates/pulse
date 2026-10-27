# Quickstart Validation Guide: Milestone 007 — Production Container Packaging & Kubernetes Deployment

**Feature**: `007-k8s-production-deployment`  
**Date**: 2026-10-27  
**Status**: Ready for Implementation  

---

## 1. Prerequisites

Ensure Docker (or Podman), Helm v3, and Go 1.23+ are installed:

```bash
docker --version
helm version
go version
```

---

## 2. Validation Scenarios

### Scenario A: Build Multi-Stage Distroless Container Images

Build all 5 microservice container images:

```bash
make docker-build
```

Verify uncompressed image sizes are under 50MB and run as non-root user (`UID 65532`):
```bash
docker images | grep pulse-
```

*Expected Verification*:
- `pulse-gateway`: $< 35\text{MB}$
- `pulse-processor`: $< 35\text{MB}$
- `pulse-alertd`: $< 35\text{MB}$
- `pulse-chronod`: $< 35\text{MB}$
- `pulse-simulator`: $< 35\text{MB}$

---

### Scenario B: Helm Chart Linting & Template Rendering

Lint the Helm chart and render template manifests:

```bash
helm lint deployments/helm/pulse
helm template pulse deployments/helm/pulse > /tmp/pulse-rendered.yaml
```

*Expected Verification*:
- `0 chart(s) linted, 0 failure(s)`
- Syntactically valid YAML manifests rendered for:
  - ConfigMap (`pulse-config`) & Secret (`pulse-secrets`)
  - Deployments & Services: `pulse-gateway`, `pulse-processor`, `pulse-alertd`, `pulse-chronod`
  - HPAs: `pulse-gateway-hpa` (2..10 replicas), `pulse-processor-hpa` (1..12 replicas)
  - PDB: `pulse-pdb` (`minAvailable: 1`)

---

### Scenario C: Deploy to Local Kubernetes Cluster (e.g. `kind` / `minikube`)

Install the Helm chart onto a running Kubernetes cluster:

```bash
# 1. Install or upgrade Pulse release
helm upgrade --install pulse ./deployments/helm/pulse \
  --set global.redpanda.brokers="pulse-redpanda:9092"

# 2. Check rollout status across all deployments
kubectl rollout status deployment/pulse-gateway
kubectl rollout status deployment/pulse-processor
kubectl rollout status deployment/pulse-alertd
kubectl rollout status deployment/pulse-chronod
```

*Expected Verification*:
- All pods achieve `1/1 Running` and `Ready` status via `/healthz` HTTP probes.
- Zero root permissions required.

---

### Scenario D: Verify Horizontal Pod Autoscaling Boundaries

Inspect the deployed Horizontal Pod Autoscalers:

```bash
kubectl get hpa
```

*Expected Verification*:
- `pulse-gateway-hpa`: `MINPODS=2`, `MAXPODS=10`, `TARGETS=cpu: 75%`
- `pulse-processor-hpa`: `MINPODS=1`, `MAXPODS=12` (strictly capped at 12 to match Redpanda partition count)

---

### Scenario E: Execute Simulator Verification Job

Run an on-demand in-cluster fleet load test:

```bash
# Enable and run simulator Job
helm template pulse ./deployments/helm/pulse \
  --set simulator.enabled=true \
  --set simulator.duration=15s | kubectl apply -f -

# Follow simulator log execution
kubectl logs -f job/pulse-simulator
```

*Expected Verification*:
- Simulator pod launches, streams synthetic telemetry to `pulse-gateway:50051`, and completes with exit code 0.
