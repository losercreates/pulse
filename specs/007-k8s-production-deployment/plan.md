# Implementation Plan: Milestone 007 — Production Container Packaging & Kubernetes Deployment

**Branch**: `007-k8s-production-deployment` | **Date**: 2026-10-27 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/007-k8s-production-deployment/spec.md`

---

## Summary

Deliver production-grade container packaging and turnkey Kubernetes orchestration for the entire Pulse platform:
1. Multi-stage minimal distroless Docker images for all 5 binaries (`gateway`, `processor`, `alertd`, `chronod`, `simulator`) with non-root security contexts (`UID 65532`) and footprints $< 50\text{MB}$.
2. Declarative Helm chart (`deployments/helm/pulse`) managing Deployments, Services, ConfigMaps, Secrets, liveness/readiness probes, and resource bounds.
3. Resilient autoscaling policies (HPA) scaling Gateway on CPU ($75\%$) and bounding Stream Processor between 1 and 12 replicas to strictly enforce Constitution Principle I (1 goroutine/pod per partition).
4. Graceful termination drains (`SIGTERM` 30s) and on-demand in-cluster fleet load verification via Kubernetes `Job`.

---

## Technical Context

**Language/Version**: Go 1.23+ (static builds with `CGO_ENABLED=0`), OCI Container Specification, Helm v3, Kubernetes v1.28+  
**Primary Dependencies**: Docker/Podman, Helm v3, Kubernetes Core APIs (`apps/v1`, `core/v1`, `autoscaling/v2`, `policy/v1`, `batch/v1`)  
**Storage**: Redpanda broker (`pulse-redpanda:9092`), ChronoDB sharded in-memory engine  
**Testing**: Go integration tests (`helm-lint`, template dry-run assertions, Dockerfile syntax checks), Kubernetes manifest schema validation  
**Target Platform**: Linux containers (amd64 / arm64), Kubernetes v1.28+ (EKS, GKE, AKS, kind, minikube)  
**Project Type**: Containerization, Infrastructure as Code, Kubernetes Helm Chart  
**Performance Goals**: Image build footprints $< 50\text{MB}$, pod startup & readiness $< 15\text{s}$, rolling restart zero-loss drain within 30s  
**Constraints**: Non-root execution (`runAsNonRoot: true`, `UID 65532`), read-only root filesystems, stream processor max replicas capped at 12  
**Scale/Scope**: 5 microservices, 1 unified Helm chart, 14 Kubernetes resource templates  

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Constitution Requirement | Plan Compliance Analysis | Status |
| :--- | :--- | :--- | :---: |
| **I. Partition Isolation** | Exactly one worker goroutine per Kafka partition. No cross-thread locking on hot path. | Stream processor HPA is **strictly capped at `maxReplicas: 12`**, ensuring no idle pod overhead and preserving partition-local consumer assignment across pods. | **PASS** |
| **II. Watermark Progression** | Monotonic event-time watermarking; zero wall-clock closures. | Stream processor replica scaling does not alter watermark logic; minimum 1 replica maintains continuous watermark progression. | **PASS** |
| **III. Idempotent Storage** | Writes must be idempotent upserts. Zero double-counting under rebalance or replay. | Rolling pod restarts flush in-flight batches before shutdown; ChronoDB composite keys guarantee 100% idempotent replays. | **PASS** |
| **IV. Decoupled Alerts** | Alert dispatch must execute in isolated worker pool; outages must never block ingestion. | `alertd` runs as an isolated Kubernetes Deployment with independent resource quotas, HPA, and memory limits. | **PASS** |
| **V. Polyglot Storage Boundary** | ChronoDB accessed across explicit boundary; backpressure propagates via consumer lag. | ChronoDB deployed with dedicated gRPC (`:50054`) and REST (`:8084`) Services. Ingestion backpressure naturally reflects in Redpanda consumer lag. | **PASS** |

*All Constitution checks PASS.*

---

## Project Structure

### Documentation (this feature)

```text
specs/007-k8s-production-deployment/
├── spec.md              # Feature specification
├── plan.md              # This implementation plan
├── research.md          # Technical research & decision records
├── data-model.md        # Entities, Helm values schema, and workload specs
├── quickstart.md        # Validation scenarios & testing guide
├── contracts/
│   ├── dockerfiles.md   # Container build contracts & user specifications
│   ├── helm-chart.md    # Helm chart hierarchy & helper contracts
│   └── k8s-workloads.md # Kubernetes Deployments, Services, HPA, PDB specs
└── checklists/
    └── requirements.md  # Specification quality checklist (16/16 passed)
```

### Source Code (repository root)

```text
deployments/
├── docker/
│   ├── Dockerfile.gateway      # Multi-stage distroless build for gateway
│   ├── Dockerfile.processor    # Multi-stage distroless build for processor
│   ├── Dockerfile.alertd       # Multi-stage distroless build for alertd
│   ├── Dockerfile.chronod      # Multi-stage distroless build for chronod
│   └── Dockerfile.simulator    # Multi-stage distroless build for simulator
└── helm/
    └── pulse/
        ├── Chart.yaml          # Helm chart metadata
        ├── values.yaml         # Default release configuration values
        └── templates/
            ├── _helpers.tpl    # Standardized template labels and names
            ├── configmap.yaml  # Shared cluster configurations & broker URLs
            ├── secret.yaml     # Webhook tokens & credentials
            ├── gateway-deployment.yaml
            ├── gateway-service.yaml
            ├── gateway-hpa.yaml
            ├── processor-deployment.yaml
            ├── processor-service.yaml
            ├── processor-hpa.yaml
            ├── alertd-deployment.yaml
            ├── alertd-service.yaml
            ├── chronod-deployment.yaml
            ├── chronod-service.yaml
            ├── pdb.yaml
            └── simulator-job.yaml

tests/
└── integration/
    └── k8s_packaging_test.go   # Integration tests validating Dockerfiles and Helm rendering
```

**Structure Decision**: Place all container specifications in `deployments/docker/` and the unified Helm chart in `deployments/helm/pulse/`. Add automated validation tests in `tests/integration/k8s_packaging_test.go` and Makefile targets in repository root.

---

## Complexity Tracking

> **Zero Constitution violations; all architectural invariants satisfied.**

| Violation | Why Needed | Simpler Alternative Rejected Because |
| :---: | :---: | :---: |
| *None* | *N/A* | *N/A* |
