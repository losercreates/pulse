# Feature Specification: Milestone 007 — Production Container Packaging & Kubernetes Deployment

**Feature Branch**: `007-k8s-production-deployment`  
**Created**: 2026-10-27  
**Status**: Draft  
**Input**: User description: "Create milestone 007: Production container packaging, Helm charts, and Kubernetes deployment orchestration with multi-stage distroless Dockerfiles for all microservices, declarative ConfigMaps/Secrets, liveness/readiness health probes, resource limits, and Horizontal Pod Autoscaling (HPA) driven by CPU and Redpanda partition consumer lag"

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Multi-Stage Minimal Distroless Container Packaging (Priority: P1) 🎯 MVP

As an Operations Engineer, I need secure, minimal, multi-stage container images for all platform services (`gateway`, `processor`, `alertd`, `chronod`, and `simulator`) so that containers can run in unprivileged, production-grade cloud environments with minimal attack surface and zero bloated build dependencies.

**Why this priority**: Packaging binaries into hardened, portable OCI images is the fundamental prerequisite for any containerized or Kubernetes deployment.

**Independent Test**: Build all container images using Docker or Podman; verify that each image starts cleanly as a non-root user, exposes its configured ports, and responds to HTTP `/healthz` checks with an uncompressed footprint under 50MB.

**Acceptance Scenarios**:
1. **Given** a platform service source repository, **When** building a container image, **Then** the resulting image uses a multi-stage build containing only statically compiled binaries and minimal runtime dependencies without shell utilities or package managers.
2. **Given** a container running any of the platform microservices, **When** inspecting runtime process attributes, **Then** the process executes as a non-root user (`UID 65532`) with read-only root filesystem compatibility.
3. **Given** the load simulator binary, **When** packaged into a container, **Then** it can execute CLI arguments for one-shot benchmarking or scheduled stress testing against cluster endpoints.

---

### User Story 2 - Declarative Helm Chart & Multi-Service Kubernetes Manifests (Priority: P1)

As a Platform Engineer, I need a declarative Helm chart and structured Kubernetes manifests so that I can reliably deploy, configure, and upgrade the entire Pulse ecosystem across development, staging, and production Kubernetes clusters with a single command.

**Why this priority**: Declarative deployment configuration eliminates manual environment drifts and standardizes service discovery, networking, and configuration management across all services.

**Independent Test**: Execute `helm template` dry-run and apply manifests to a local Kubernetes cluster (e.g. `kind` or `minikube`); verify all 4 core services deploy, achieve `Ready` status via liveness/readiness probes, and communicate over internal cluster DNS.

**Acceptance Scenarios**:
1. **Given** a Kubernetes cluster with Redpanda accessible, **When** deploying via `helm install pulse ./deployments/helm/pulse`, **Then** Deployments, Services, ConfigMaps, and Secrets are created for `gateway`, `processor`, `alertd`, and `chronod`.
2. **Given** running microservice pods, **When** Kubernetes probes `/healthz` endpoints, **Then** liveness and readiness statuses transition to healthy and route ingress traffic exclusively to ready pods.
3. **Given** external client traffic, **When** accessing the gateway Service, **Then** both gRPC unary/streaming (`:50051`) and HTTP REST (`:8080`) ports are exposed with cluster-internal load balancing.

---

### User Story 3 - Resilient Scaling & Lag-Driven Horizontal Pod Autoscaling (Priority: P2)

As a Site Reliability Engineer, I need horizontal autoscaling policies that scale service replicas dynamically based on CPU utilization and Redpanda consumer lag so that the platform automatically adapts to traffic spikes while respecting partition concurrency bounds.

**Why this priority**: Fixed replica counts either over-provision idle resources or suffer ingestion lag accumulation during sudden surge loads.

**Independent Test**: Simulate high ingestion load; verify that Gateway replicas scale out horizontally based on CPU demand, and Stream Processor replicas scale out up to 12 replicas based on consumer lag, contracting back to minimum replicas during quiet periods.

**Acceptance Scenarios**:
1. **Given** an Ingestion Gateway experiencing CPU utilization $> 75\%$ or request surges, **When** Horizontal Pod Autoscaling evaluates metrics, **Then** replica count scales out between 2 and 10 pods.
2. **Given** a Stream Processor consumer group accumulating unconsumed events in Redpanda, **When** consumer lag metrics exceed threshold, **Then** replicas scale out up to a strict ceiling of 12 pods (1 replica per topic partition, adhering to Constitution Principle I).
3. **Given** cluster maintenance or node draining, **When** pods are evicted, **Then** Pod Disruption Budgets (PDB) maintain at least 50% active replica availability to prevent streaming outages.

---

### User Story 4 - Graceful Termination, Secret Configuration & Batch Load Jobs (Priority: P2)

As an Incident Responder and Platform Operator, I need graceful shutdown handling and secure credential management so that rolling updates execute without dropped connections or offset corruption, and sensitive webhook credentials remain encrypted.

**Why this priority**: Abrupt container SIGKILL terminates in-flight HTTP/gRPC requests, leaves uncommitted consumer offsets, and risks incomplete window aggregate flushes.

**Independent Test**: Trigger a rolling restart of `processor` and `chronod` pods under simulated load; verify zero failed requests, zero duplicate stored records, and complete offset commit flushes within the 30-second termination grace period.

**Acceptance Scenarios**:
1. **Given** an active streaming pod receiving a Kubernetes `SIGTERM`, **When** graceful termination begins, **Then** the process ceases accepting new requests, flushes pending aggregate batches and offset commits, and exits cleanly within the termination grace period.
2. **Given** external alert webhook destinations, **When** configuring notification endpoints and auth tokens, **Then** values are mounted from Kubernetes Secrets rather than plain-text ConfigMaps.
3. **Given** an ad-hoc fleet load verification requirement, **When** running the simulator on Kubernetes, **Then** a Kubernetes `Job` or `CronJob` executes the specified workload profile and outputs structured test results.

---

### Edge Cases

- **Broker Connection Latency at Pod Startup**: When service pods start prior to Redpanda topic creation, containers must retry connection with exponential backoff rather than crash-looping indefinitely.
- **Partition Rebalancing during Horizontal Scaling**: When stream processor replicas scale between 1 and 12 pods, franz-go cooperative sticky rebalancing must safely reassign partition workers without losing in-flight window state.
- **Read-Only Root Filesystem Restrictions**: Production security policies requiring `readOnlyRootFilesystem: true` must not prevent services from running (temporary scratch storage mounted as `emptyDir` in `/tmp`).
- **Zero-Lag Scaling Floor**: When incoming traffic drops to zero, stream processor replica count must not scale below 1 pod, ensuring continuous event-time watermark progression monitoring.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide multi-stage Dockerfiles for all microservices (`gateway`, `processor`, `alertd`, `chronod`, and `simulator`) using a Go build stage and a minimal distroless runtime stage.
- **FR-002**: All container images MUST execute as an unprivileged, non-root user (`nonroot:nonroot` UID 65532) and specify explicit container port exposures.
- **FR-003**: All container images MUST produce static binary builds with CGO disabled and stripped debug symbols, maintaining an uncompressed image footprint under 50MB.
- **FR-004**: System MUST provide a structured, parameterized Helm chart in `deployments/helm/pulse` managing Deployments, Services, ConfigMaps, Secrets, and Autoscaling policies.
- **FR-005**: Helm values MUST allow declarative configuration of container images, replica counts, resource requests/limits, Redpanda broker addresses, port mappings, and metric endpoints.
- **FR-006**: Kubernetes Deployments MUST configure HTTP liveness and readiness health checks pointing to `/healthz` endpoints with configurable initial delay, timeout, and failure thresholds.
- **FR-007**: Gateway Kubernetes Deployment MUST expose dual ports: HTTP REST ingestion on port 8080 and gRPC streaming on port 50051.
- **FR-008**: ChronoDB Kubernetes Deployment MUST expose REST analytical queries on port 8084 and gRPC storage boundary on port 50054.
- **FR-009**: Stream Processor and Alert Dispatcher Deployments MUST expose HTTP observability and metrics endpoints on designated ports 8081 and 8083.
- **FR-010**: System MUST configure Horizontal Pod Autoscaling (HPA) for Gateway scaling between 2 and 10 replicas based on target CPU utilization (default 75%).
- **FR-011**: System MUST configure Horizontal Pod Autoscaling (HPA) for Stream Processor scaling between 1 and 12 replicas, bounded strictly to 12 replicas to enforce the 1-worker-per-partition invariant (Constitution Principle I).
- **FR-012**: Kubernetes Deployments MUST configure graceful shutdown hooks with `terminationGracePeriodSeconds: 30` to permit in-flight requests and offset flushes to complete before termination.
- **FR-013**: System MUST provide Pod Disruption Budgets (PDB) guaranteeing minimum availability during planned node drains or cluster upgrades.
- **FR-014**: System MUST provide a Kubernetes `Job` template for running the high-throughput load simulator within the cluster network for synthetic load and SLA verification.

---

### Key Entities

- **Container Image Specification**: Defines multi-stage build rules, base layers, non-root user context, port definitions, and static binary outputs for each microservice.
- **Helm Release Configuration**: Hierarchical configuration values (`values.yaml`) defining environment bindings, resource allocations, replica bounds, and cluster networking.
- **Kubernetes Microservice Deployment**: Declarative resource specification coordinating pod replicas, container specifications, volumes, probes, and restart policies.
- **Autoscaling Policy (HPA)**: Metric-driven horizontal pod autoscaling rule defining target metrics, scaling thresholds, stabilization windows, and min/max replica boundaries.
- **Pod Disruption Budget (PDB)**: High-availability operational constraint enforcing minimum available pod replicas during voluntary disruptions.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All microservice container images compile to an uncompressed size $< 50\text{MB}$ and run successfully under non-root unprivileged security contexts (`UID 65532`).
- **SC-002**: A complete platform deployment can be launched onto any CNCF-certified Kubernetes cluster (v1.28+) via a single `helm install` command within 60 seconds.
- **SC-003**: 100% of microservice pods pass Kubernetes liveness and readiness probes within 15 seconds of container startup.
- **SC-004**: Rolling cluster upgrades and pod terminations achieve zero dropped ingestion requests and zero duplicated stored records with complete offset checkpoint flushes within 30 seconds.
- **SC-005**: Horizontal Pod Autoscaler dynamically scales Ingestion Gateway replicas when CPU utilization crosses 75%, and bounds Stream Processor scaling between 1 and 12 pods without violating partition-local ordering.
- **SC-006**: Synthetic fleet load simulator packaged as a Kubernetes Job successfully connects to cluster-internal service endpoints and drives high-throughput validation traffic without networking errors.

---

## Assumptions

- Target deployment runtime is Kubernetes v1.28 or later.
- Redpanda broker cluster is provisioned within the same Kubernetes namespace or reachable via standard Kubernetes Service DNS endpoints (`pulse-redpanda:9092`).
- Prometheus metrics server or Custom Metrics Adapter is available in the cluster to expose metrics to the Horizontal Pod Autoscaler.
- Container image registry is accessible for pushing and pulling multi-architecture Linux x86_64 container images.
- All services maintain backward compatibility with existing configuration environment variables established in Milestones 001–006.
