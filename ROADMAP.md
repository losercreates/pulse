# Pulse Roadmap & Platform Evolution

This document tracks the architectural progression, completed milestones, and forward-looking roadmap for the **Pulse** real-time industrial telemetry ingestion and stream processing platform.

---

## Roadmap Overview

| Version | Milestone | Focus Area | Status |
|:---:|---|---|:---:|
| **v0.1.0** | **001: Ingestion Gateway & Broker Setup** | Dual-transport gRPC/HTTP perimeter & Redpanda 12-partition broker | **Completed** |
| **v0.2.0** | **002: Stateful Stream Processing** | Zero-lock partition workers, watermark priority queues & windowing | **Completed** |
| **v0.3.0** | **003: Anomaly Detection & Alerting** | Partition-local dynamic EWMA baselines & decoupled `alertd` dispatcher | **Completed** |
| **v0.4.0** | **004: ChronoDB Time-Series Storage** | 16-shard in-memory engine, idempotent upserts & dynamic rollups | **Completed** |
| **v0.5.0** | **005: Load Simulator & Chaos Harness** | 10k fleet traffic simulator, offline partition dumps & canary probes | **Completed** |
| **v0.6.0** | **006: Observability & Dashboards** | Prometheus scraping, Alertmanager rules & 5 Grafana dashboards | **Completed** |
| **v0.7.0** | **007: Container Packaging & Helm** | Distroless non-root Dockerfiles, Kubernetes Helm chart, HPAs & PDBs | **Completed** |
| **v0.8.0** | **008: ChronoDB Storage Durability** | Embedded LSM engine, WAL durability, SSTables & StatefulSets | **Completed** |
| **v0.9.0** | **009: Security & Access Control** | mTLS, device anti-spoofing, ChronoDB JWT RBAC & HMAC webhooks | **Completed** |
| **v1.0.0** | **010: Edge Bridge & CEP Engine** | MQTT/Sparkplug/Modbus bridge, disk spooler & partition-local CEP | **Completed** |
| **v1.1.0** | **011: Native OPC-UA Fieldbus Adapter** | Direct binary OPC-UA monitored items & dynamic node discovery | **Completed** |

---

## Completed Milestones (v0.1.0 – v1.1.0)

### Milestone 001: Ingestion Gateway & Broker Setup (v0.1.0)
- **Standardized Protobuf Schema**: Canonical `TelemetryReading` binary wire contract with nanosecond temporal precision (`timestamp_ns`).
- **Dual-Transport Ingestion Perimeter**:
  - Low-latency unary and client-side streaming gRPC server (`:50051`).
  - RESTful HTTP/JSON endpoint (`:8080`) supporting single and micro-batched ingestion.
- **Perimeter Guardrails**: Nanosecond clock skew validation (rejecting future timestamps $>5\text{m}$ and stale data $>24\text{h}$), IEEE 754 non-finite numeric filtering (`NaN`, `Inf`), and identifier boundaries.
- **Partition-Local Ordering Invariant**: Deterministic Murmur2 key hashing on `device_id` routing to 12 Redpanda partitions on `telemetry.raw`.

### Milestone 002: Stateful Stream Processing & Watermarking (v0.2.0)
- **Zero-Lock Concurrency**: Exactly one dedicated worker goroutine per Redpanda partition, isolating all state in partition-local memory without mutex contention.
- **Monotonic Event-Time Watermarking**: Monotonic progression (`max_event_time - allowed_lateness`) with idle partition timeout detection. Window closures triggered strictly by watermark progression.
- **High-Performance Window Engine**: Min-heap priority queue for $O(1)$ expiration checks, supporting tumbling and sliding windows with single-pass `WindowAccumulator` (min, max, sum, count, mean, 64-register HyperLogLog).
- **Late Data Routing**: Diverts records timestamped older than the watermark directly to `telemetry.late` without corrupting closed windows.

### Milestone 003: Dynamic Anomaly Detection & Decoupled Alerting (v0.3.0)
- **Partition-Local EWMA Baselines**: Computes online statistical mean and rolling variance with zero cross-thread locking on the hot ingestion path.
- **Dynamic $k$-Sigma Guardrails**: Evaluates dynamic deviation thresholds with a flatline sensor variance floor ($\sigma^2 \ge \epsilon$) preventing alert storms during steady-state sensor operation.
- **Alert Deduplication & Auto-Resolution**: 5-minute incident cooldown window, escalation notifications, and 10-sample hysteresis auto-resolution (`STATUS_RESOLVED`).
- **Decoupled Alert Dispatcher (`alertd`)**: Asynchronous consumer for `telemetry.alerts` with 3-state circuit breakers (`CLOSED`, `OPEN`, `HALF_OPEN`), exponential retry backoff, and dead-letter routing (`telemetry.alerts.deadletter`).

### Milestone 004: ChronoDB Time-Series Storage & Query Engine (v0.4.0)
- **Idempotent Ingestion Invariant**: Deterministic composite primary key `(device_id, metric_name, window_start_ns, window_end_ns)` ensuring zero duplicated data across broker rebalances or crash replays.
- **Polyglot Storage Boundary**: Dual gRPC (`:50054`) and REST (`:8084`) query interfaces backed by 16-way Murmur3 device sharding.
- **Sub-Millisecond Historical Queries**: Chronologically indexed range retrievals and dynamic multi-resolution rollups (`step=1m|5m|1h`) computed dynamically without raw event scans.

### Milestone 005: Device Fleet Load Simulator & Chaos Harness (v0.5.0)
- **Virtual Fleet Generation**: Simulated 10,000+ industrial assets across 4 archetypes (`turbines`, `induction motors`, `hydraulic pumps`, `generators`) with correlated sinusoidal signals, Gaussian noise, and baseline calibration drifts.
- **Micro-Batch Pacing Engine**: Token-bucket rate limiter sustaining $\ge 50,000\text{ events/sec}$ with $<5\%$ rate jitter.
- **Chaos Profiles**: Bursty traffic surges ($3\times$ multipliers), network partition disconnect/reconnect backfills, and deliberate statistical sensor anomalies ($z \ge 5.0$).
- **SLA Gate Validation**: Synthetic canary probes verifying real-time $p95\text{ latency} < 2.0\text{s}$ and error rates $< 0.10\%$.

### Milestone 006: Multi-Service Observability & Grafana Dashboards (v0.6.0)
- **Prometheus Scraping Matrix**: 5-second polling interval across Gateway, Stream Processor, Alert Dispatcher, ChronoDB, and Redpanda broker endpoints.
- **Automated SLA Alert Rules**: Production Alertmanager rules monitoring consumer lag, watermark stalling, gateway error spikes, and circuit breaker trip states.
- **Declarative Grafana Dashboards**: 5 pre-built production dashboards:
  - *Executive Pipeline Overview*
  - *Ingestion Gateway & Redpanda Partition Lag*
  - *Stream Processor & Watermarking Health*
  - *Anomaly Detection & Decoupled Alerting*
  - *ChronoDB Storage & Analytical Query Engine*

### Milestone 007: Container Packaging & Kubernetes Deployment (v0.7.0)
- **Distroless Multi-Stage Containers**: Static Go compilation (`CGO_ENABLED=0`, `-s -w`), minimal `gcr.io/distroless/static-debian12:nonroot` runtime, executing strictly as non-root (`UID 65532:65532`) with image footprints $< 35\text{MB}$.
- **Production Helm Chart**: Unified chart managing Deployments, Services, ConfigMaps, Secrets, HPAs, and PodDisruptionBudgets.
- **Concurrency Autoscaling Invariant**: Stream Processor HPA autoscales based on consumer lag with a strict 12-pod ceiling matching the 12 Redpanda partitions.

### Milestone 008: ChronoDB Storage Durability & StatefulSets (v0.8.0)
- **Embedded LSM Engine**: Replaced ephemeral storage with disk-backed Log-Structured Merge-tree (LSM) engine across 16 hash shards with directory file locking (`flock`).
- **Write-Ahead Logging (WAL)**: Synchronous binary WAL framing (`0x50554C53`, length, CRC32) with `fsync` flushing prior to Redpanda offset commit.
- **Crash Recovery Protocol**: Startup reconciliation discovering immutable `.sst` segments and replaying uncheckpointed WAL frames with torn-tail write tolerance.
- **Immutable SSTables & Bloom Filters**: Length-prefixed data blocks, sparse index blocks, and Murmur3 Bloom filters for zero-disk-read misses during queries.
- **StatefulSet Orchestration**: Kubernetes `StatefulSet` with dedicated `10Gi` `ReadWriteOnce` PersistentVolumeClaims and Headless Service peer resolution.

### Milestone 009: Defense-in-Depth Security & Access Control (v0.9.0)
- **Perimeter Mutual TLS (mTLS)**: Enforces TLS 1.3 client certificate validation against trusted root CA bundles on Gateway HTTP and gRPC ports.
- **Device Identity Anti-Spoofing**: Cryptographically binds client certificate Common Name (CN/SAN) to payload `device_id`, blocking identity spoofing with HTTP 403 / gRPC `PermissionDenied`.
- **ChronoDB Query API RBAC**: Zero-dependency pure Go JWT validator enforcing `Viewer`, `Operator`, and `Admin` roles with tenant device scope glob pattern isolation.
- **Outbound Webhook Integrity (HMAC-SHA256)**: Cryptographic signature generation (`X-Pulse-Signature`, `X-Pulse-Timestamp`) with a 300-second clock skew tolerance window and downstream Go verifier SDK.
- **NetworkPolicies & Secret Mounts**: Default-deny ingress policies, least-privilege pod communication rules, and read-only secret volume mounts (`mode: 0400`).

### Milestone 010: Industrial Edge Bridge & Complex Event Processing Engine (v1.0.0)
- **Standalone Edge Daemon (`pulse-bridge`)**:
  - MQTT v3.1.1/v5.0 subscriber with dynamic topic wildcard matching (`+`, `#`) and JSON token extraction (`{device_id}`, `{metric}`).
  - Sparkplug B binary protobuf payload decoder (`NBIRTH`, `NDATA`, `DBIRTH`, `DDATA`).
  - Modbus TCP cyclic polling master with FC03 (Holding) and FC04 (Input) registers, linear scaling, and word-endianness decoders.
- **Store-and-Forward Offline Buffering**: In-memory ring buffer combined with append-only disk FIFO log (`PUBR` 16-byte binary framing and IEEE CRC32 checksums). Flushes spooled records in strict event-time order over mTLS gRPC upon reconnect.
- **Partition-Local CEP Rule Engine**: Evaluates multi-stage temporal sequences (`EventA FOLLOWED_BY EventB WITHIN ΔT`) with event-time watermark candidate expiration and zero cross-thread locking.
- **Cross-Metric Expressions & Incident Synthesis**: AST evaluator for multivariate formulas (e.g. `power_mw / fuel_flow < 0.70`) and composite incident synthesis collapsing cascading alarms into single root-cause alerts ($\ge 90\%$ noise reduction).
- **Live Rule Reloading**: Administrative endpoint (`POST /api/v1/rules/reload`) and `SIGHUP` signal handler performing atomic rule registry swaps without stream processor downtime.

### Milestone 011: Native OPC-UA Binary Fieldbus Adapter (v1.1.0)
- **Direct OPC-UA Binary Driver**: Pure Go binary protocol implementation for OPC-UA (`opc.tcp://`) in `pulse-bridge` without external CGo or runtime C dependencies.
- **Monitored Items & Subscriptions**: Event-driven server subscriptions (`CreateSubscription`, `CreateMonitoredItems`, and pipelined `PublishRequest` loops) streaming `DataChangeNotification` frames with nanosecond timestamp precision.
- **Dynamic Address Space Discovery**: Recursive `Browse` crawler with visited cycle detection and path glob matching (`Objects/Turbine/*/*`) for automatic tag discovery and auto-subscription.
- **Resilient Connection State Machine & Keep-Alives**: Heartbeat monitoring detecting dropped links or server reboots with exponential backoff session reconstitution.
- **Store-and-Forward Spooling Integration**: Seamless handoff of OPC-UA telemetry into the local append-only disk FIFO spooler (`PUBR` CRC32 framing) during upstream gateway disconnects, flushing in chronological order upon reconnection.
- **Industrial Cybersecurity**: Support for `None` and `Basic256Sha256` security policies (`SignAndEncrypt`) with Anonymous, Username/Password, and X.509 client certificate authentication.

---

## Architectural Decisions & Retired Tracks

Following the platform production readiness assessment, the following previously contemplated enhancements were evaluated and intentionally retired to preserve Pulse's zero-overhead, air-gapped, and low-complexity operational footprint:

- **ChronoDB Raft Consensus**: Retired. Storage durability and crash recovery are already guaranteed by Redpanda's replicated broker log combined with ChronoDB's embedded LSM engine, synchronous WAL `fsync`, and Kubernetes `StatefulSet` PVC re-attachment. Adding application-level Raft consensus introduces unnecessary distributed state machine complexity, split-brain failure modes, and write latency.
- **WebAssembly (Wasm) UDFs**: Retired. Milestone 010 delivered a partition-local CEP engine with multivariate AST arithmetic expressions (`power_mw / fuel_flow < 0.70`), temporal sequencing, and live rule reloads via REST and `SIGHUP`. Embedding a Wasm runtime introduces memory overhead, JIT pauses, and operational complexity in air-gapped industrial facilities.
- **OpenTelemetry Distributed Tracing**: Retired. In high-throughput industrial streaming ($50\text{k} - 10\text{M}$ events/s), per-event distributed tracing induces unsustainable data amplification. End-to-end latency and health observability are already delivered via synthetic canary probes and hop-by-hop Prometheus metrics.

