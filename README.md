# Pulse — Real-Time Telemetry Ingestion & Stream Processing Platform

**Pulse** is a high-throughput, low-latency streaming platform designed to ingest high-cardinality industrial and IoT sensor telemetry, perform deterministic watermark-based windowed aggregation, detect anomalies using statistical guardrails in real time, and persist idempotent aggregates to ChronoDB instances.

---

## Architecture Overview

```mermaid
flowchart TB
    D1[Industrial Sensor 1] --> ING[Ingestion Gateway: gRPC / HTTP]
    D2[Edge Gateway N] --> ING
    ING -->|Murmur2 partition by device_id| RP[(Redpanda: telemetry.raw - 12 partitions)]
    RP --> SP[Stream Processor: Watermark Priority Queue]
    SP --> AGG[(Windowed Aggregates)]
    SP --> AD[Anomaly Detector: Per-Device EWMA]
    AD --> ALERT[Decoupled Alert Dispatcher]
    SP --> CHRONO[(ChronoDB Instances: Sharded by Device Range)]
    AGG --> API[Query API]
```

---

## Milestone 001: Ingestion Gateway & Broker Setup

Milestone 001 implements the entry tier of the Pulse platform:
1. **Redpanda Messaging Cluster**: Single-node Redpanda broker with 12 partitions configured on `telemetry.raw` for local development.
2. **Standardized Protobuf v3 Schema**: `TelemetryReading` binary wire contract with strict temporal precision (`timestamp_ns`).
3. **Dual Transport Gateway**:
   - **gRPC Server** (`:50051`): Low-latency unary and client-side streaming ingestion.
   - **HTTP/JSON Server** (`:8080`): REST endpoints (`/api/v1/telemetry`, `/api/v1/telemetry/batch`), Prometheus scraping (`/metrics`), and broker health probe (`/healthz`).
4. **Validation Guardrails**:
   - Clock skew defense (rejects timestamps $>5\text{ min}$ into future or $>24\text{ hours}$ in past).
   - IEEE 754 non-finite numeric rejection (`NaN`, `Inf`).
   - String length boundaries ($1 \le \text{device\_id} \le 128$).
5. **Partition-Local Ordering Invariant**:
   - Uses `twmb/franz-go` with `StickyKeyPartitioner(nil)` (Murmur2 hashing) on `device_id` to ensure per-device event ordering.

---

## Milestone 002: Stream Processing Engine & Watermarks

Milestone 002 implements the stateful stream-processing core conforming strictly to Constitution Principles I–III:
1. **Concurrency & Partition Isolation**: Exactly one dedicated worker goroutine per Redpanda partition with partition-local memory and zero cross-thread locking on the hot ingestion path.
2. **Deterministic Event-Time Watermarking**: Monotonic watermark progression (`max_event_time - allowed_lateness`) with idle partition timeout defense. Windows are closed strictly by watermark crossing `window_end` (zero wall-clock closures).
3. **High-Performance Window Engine**:
   - Tumbling and sliding window assignment.
   - Min-heap priority queue (`container/heap`) ordered by `window_end_ns` for $O(1)$ expiration checks.
   - Single-pass `WindowAccumulator` computing count, sum, min, max, mean, and compact 64-register HyperLogLog distinct sketch.
4. **Late Data Dead-Letter Routing**: Rejects records timestamped $< \text{watermark}$ to `telemetry.late` dead-letter sink without mutating closed windows.
5. **Durability & Checkpointing**: Franz-go cooperative sticky rebalancing with offset commits coordinated with window aggregate emission to `telemetry.aggregates`.
6. **Streaming Observability**: HTTP server on `:8081` exposing `/healthz` and `/metrics` (watermark lag, open windows, late event rates).

---

## Milestone 003: Real-Time Anomaly Detection Engine & Decoupled Alerting

Milestone 003 implements real-time statistical anomaly detection and decoupled alert dispatching conforming strictly to Constitution Principles I and IV:
1. **Partition-Local Dynamic Baselines**:
   - Computes online Exponentially Weighted Moving Average (EWMA) and rolling variance per `(device_id, metric_name)` strictly inside partition worker memory with zero cross-thread locking on the hot ingestion path.
   - Evaluates dynamic $k$-sigma anomaly bounds ($|x - \mu| > k \cdot \sigma$, default $k=3.0$) and classifies deviation severity (`INFO`, `WARNING`, `CRITICAL`).
   - Flatline sensor variance floor guardrail ($\sigma^2 \ge \epsilon$, default $\epsilon=10^{-4}$) prevents false-positive alert storms during steady-state operation.
2. **Alert Deduplication, Cooldown & Auto-Resolution**:
   - Suppresses duplicate alert notifications for ongoing anomalies within a 5-minute cooldown window.
   - Dispatches escalation reminders when anomalies persist beyond the cooldown window.
   - Auto-resolution hysteresis: emits `STATUS_RESOLVED` after $N=10$ consecutive nominal readings.
3. **Decoupled Asynchronous Alert Dispatcher (`alertd`)**:
   - Stream processor asynchronously publishes `AlertEvent` protobufs to `telemetry.alerts` (12 partitions).
   - Standalone daemon (`alertd`) consumes alerts with an isolated concurrent worker pool. Downstream webhook delays (e.g. 30s) cause **0% latency degradation or consumer lag** on stream processor ingestion (Constitution Principle IV).
   - 3-state Circuit Breaker (`CLOSED`, `OPEN`, `HALF_OPEN`): trips to `OPEN` after 5 consecutive failures, holds for 30s, probes in `HALF_OPEN`.
   - Jittered exponential backoff and automatic dead-letter routing to `telemetry.alerts.deadletter`.
4. **Subsystem Observability**:
   - HTTP server on designated port `:8083` exposing `/healthz` and `/metrics` (detection counts, active incidents, dispatch latencies, circuit breaker states).

---

## Milestone 004: ChronoDB Polyglot Storage Sink & Historical Query Engine

Milestone 004 implements durable time-series aggregate persistence and sub-millisecond historical query retrieval conforming strictly to Constitution Principles III and V:
1. **Idempotent Ingestion & Deduplication Invariant (Constitution Principle III)**:
   - Ingests closed window aggregates from Redpanda topic `telemetry.aggregates` (12 partitions).
   - Persists records using a deterministic composite primary key: `(device_id, metric_name, window_start_ns, window_end_ns)`.
   - Replays caused by consumer crashes, rebalances, or broker offset resets execute 100% idempotent upserts with **zero double-counting or duplicated rows**.
2. **Polyglot Storage Boundary & Sharded In-Memory Engine (Constitution Principle V)**:
   - Dual interface architecture: ChronoDB gRPC boundary (`:50054`) and REST query service (`:8084`).
   - Deterministic 16-way murmur3 sharding by device ID with per-shard read/write mutexes, eliminating cross-partition locking contention.
   - Backpressure propagation: Ingestion flushes block consumer batch reads, naturally reflecting storage backpressure into Redpanda consumer lag.
3. **Sub-Millisecond Historical Range Queries**:
   - `GET /api/v1/query?device_id=...&metric_name=...&start_time=...&end_time=...&limit=...` returns chronologically sorted window aggregates with point statistics (min, max, sum, count, mean).
4. **Dynamic Multi-Resolution Analytical Rollups**:
   - `GET /api/v1/query/rollup?device_id=...&metric_name=...&start_time=...&end_time=...&step=1m|5m|1h` dynamically computes exact combined counts, global min/max, combined sums, and weighted means ($\sum \text{sum}_i / \sum \text{count}_i$) over stored aggregates without raw event scanning.
5. **Active Device & Metric Discovery Catalog**:
   - `GET /api/v1/devices` returns discovered active device IDs, associated metric names, and temporal bounds.
6. **Storage Observability Server**:
---

## Milestone 005: High-Throughput Device Fleet Load Simulator & Chaos Stress Harness

Milestone 005 implements a carrier-grade fleet load simulator and chaos stress testing harness validating platform performance under extreme conditions:
1. **Virtual Fleet Topology & Industrial Archetypes**:
   - Models fleets of $10,000+$ distinct virtual devices partitioned across 4 industrial archetypes (`turbines`, `induction motors`, `hydraulic pumps`, `generators`).
   - Generates realistic correlated multi-metric signals (`vibration`, `bearing_temp`, `rotor_rpm`, `power_mw`, `oil_pressure`) combining continuous sinusoidal oscillations, Gaussian white noise, and baseline calibration drifts.
2. **Dual Transport Driver (gRPC Streaming & HTTP Batching)**:
   - Client-side gRPC multiplexed streaming (`:50051`) with binary Protobuf encoding.
   - HTTP/JSON micro-batching (`:8080`) with connection pooling for REST ingestion fallback.
3. **High-Resolution Micro-Batch Pacing Engine**:
   - Token bucket rate limiter with micro-batch coordination delivering sustained $\ge 50,000\text{ events/sec}$ generation with $< 5\%$ rate jitter and zero OS nanosecond sleep penalties.
4. **4 Operational Traffic & Chaos Profiles**:
   - **Steady-State**: Baseline nominal Gaussian oscillations.
   - **Bursty Spikes**: Scheduled $1.5\times$ to $5.0\times$ surge load injections testing broker buffer absorption and consumer lag recovery.
   - **Offline Recovery Chaos**: Network disconnect state machine buffering telemetry in device ring buffers, dumping out-of-order backfill upon reconnection to validate watermark progression and late-data dead-letter routing.
   - **Deliberate Sensor Anomaly Chaos**: Injects step spikes ($z \ge 5.0$), sensor variance flatlines, and calibration drifts into targeted devices to verify real-time $k$-sigma anomaly detection.
5. **Real-Time Ingestion SLA Dashboard & Canary Latency Probes**:
   - Synthetic canary probe emissions sampling ChronoDB query endpoint (`:8084`) measuring true end-to-end pipeline latency ($p50$, $p90$, $p95$).
   - Dynamic ANSI terminal dashboard and automated JSON SLA reporting (`--json`) with automated gate evaluation: $\ge 90\%$ target rate, $p95\text{ latency} < 2.0\text{s}$, and error rate $< 0.10\%$.

---

## Milestone 006: Unified Multi-Service Observability & Executive Grafana Dashboards

Milestone 006 delivers full-stack real-time visibility across all 5 platform subsystems using Prometheus, Alertmanager, and declarative Grafana dashboards:
1. **Centralized Platform Prometheus Scraping**:
   - Polling interval: 5 seconds across 5 core targets:
     - Ingestion Gateway (`host.docker.internal:8080`)
     - Stream Processor (`host.docker.internal:8081`)
     - Decoupled Alert Dispatcher (`host.docker.internal:8083`)
     - ChronoDB Storage Sink & Query Service (`host.docker.internal:8084`)
     - Redpanda Messaging Broker (`redpanda:9644/public_metrics`)
2. **Automated SLA Alerting Rules (`alert_rules.yml`)**:
   - `CriticalIngestionErrorRate`: Triggers if Gateway rejection rate exceeds $0.10\%$ over 1 minute.
   - `HighConsumerLag`: Triggers if consumer group lag exceeds 250,000 unconsumed events ($> 5\text{s}$ throughput equivalent) on any partition for 30 seconds.
   - `WatermarkStalled`: Triggers if active ingestion continues ($> 50\text{ ev/s}$) while watermark lag exceeds 15 seconds for 30 seconds.
   - `AlertCircuitBreakerOpen`: Triggers when alert webhook circuit breaker trips to `OPEN` for 10 seconds.
   - `CanaryLatencySLABreached`: Triggers if synthetic canary end-to-end latency p95 exceeds $2000\text{ms}$ over 1 minute.
   - `StorageQueryLatencyHigh`: Triggers if ChronoDB analytical query p95 latency degrades beyond $5.0\text{ms}$ over 1 minute.
3. **Alertmanager Notification Routing (`alertmanager.yml`)**:
   - Grouping intervals: `group_wait: 5s`, `group_interval: 15s`, `repeat_interval: 1h`.
   - Dedicated webhook routing for critical incidents with 15-minute escalation repeats.
4. **5 Declarative Grafana Dashboards (`deployments/grafana/dashboards/`)**:
   - **Executive Pipeline Overview** (`pulse-executive-overview`): High-level throughput stat, canary p95 gauge ($< 2.0\text{s}$ SLA gate), error rate, active incidents, and end-to-end volume.
   - **Ingestion Gateway & Redpanda Partition Lag** (`pulse-ingest-broker-lag`): Protocol distribution (gRPC vs HTTP), gateway latency heatmap, stacked 12-partition lag, and rejection reasons.
   - **Stream Processor & Watermarking Health** (`pulse-stream-processor`): Per-partition watermark lag, priority queue min-heap depth, partition consumption rates, and late-data drop rates.
   - **Anomaly Detection & Decoupled Alerting** (`pulse-anomaly-alerts`): Real-time $k$-sigma anomaly rates by severity, circuit breaker state timeline, webhook dispatch latency percentiles, and retry rates.
   - **ChronoDB Storage & Analytical Query Engine** (`pulse-chronodb-storage`): Aggregate write rates, batch upsert latency percentiles (p50, p95, p99), query latency heatmap, and 16-shard balance stats.
5. **Single-Command Developer Workflow**:
   - `make obs-up`: Starts Prometheus, Grafana, Alertmanager, and Redpanda with persistent volumes and health probes.
   - `make obs-down`: Gracefully stops the observability stack.
   - `make dashboards`: Prints direct URLs to all dashboards and service UIs.
   - `./scripts/verify_observability.sh`: Automated test harness validating configs, alert rules, and dashboard models.

---

## Milestone 007: Production Container Packaging & Kubernetes Deployment

Milestone 007 delivers cloud-native containerization and enterprise Kubernetes orchestration for the entire Pulse platform:
1. **Multi-Stage Minimal Distroless Dockerfiles (`deployments/docker/`)**:
   - Compiles static Go binaries with `CGO_ENABLED=0` and stripped debug symbols (`-s -w`).
   - Hardened `gcr.io/distroless/static-debian12:nonroot` runtime executing strictly as non-root (`USER 65532:65532`).
   - Uncompressed image footprints $< 35\text{MB}$ with zero shell or package manager vulnerabilities.
   - Images provided for: `gateway`, `processor`, `alertd`, `chronod`, and `simulator`.
2. **Unified Production Helm Chart (`deployments/helm/pulse/`)**:
   - Declarative Helm v3 release managing Deployments, Services, ConfigMaps, Secrets, HPAs, and PDB.
   - Standardized recommended labels (`app.kubernetes.io/name`, `app.kubernetes.io/component`, `helm.sh/chart`).
   - Dual-port service routing: Gateway (HTTP `:8080`, gRPC `:50051`) and ChronoDB (HTTP `:8084`, gRPC `:50054`).
   - Robust liveness and readiness HTTP probes on `/healthz` ensuring zero traffic routed to unready pods.
3. **Resilient Autoscaling & Partition Concurrency Invariant**:
   - Gateway Horizontal Pod Autoscaler scaling 2 to 10 replicas on CPU utilization ($> 75\%$).
   - Stream Processor Horizontal Pod Autoscaler scaling 1 to 12 replicas on Redpanda consumer lag and CPU, strictly capped at 12 replicas to guarantee Constitution Principle I (exactly one worker per partition without idle pod waste).
   - Pod Disruption Budget (`minAvailable: 1`) protecting cluster node drains and voluntary disruptions.
4. **Graceful Draining & Synthetic Load Verification**:
   - `terminationGracePeriodSeconds: 30` across all Deployments guaranteeing zero dropped requests and full offset commit flushes before pod exit.
   - On-demand in-cluster fleet load testing via the `pulse-simulator` Kubernetes Job.
5. **Developer & CI Workflow**:
   - `make docker-build`: Builds all 5 distroless container images.
   - `make helm-lint` & `make helm-template`: Lints and validates rendered Kubernetes manifests.
   - `make k8s-verify`: Executes the automated packaging verification harness.

---

## Verified Benchmark Performance

Running on development hardware (Intel Core i7-1165G7):

| Benchmark Scenario | Measured Throughput | Latency per Op | Target Spec |
| :--- | :---: | :---: | :---: |
| **Gateway Single Reading Ingestion** | **5,727,515 events/sec** | 174.6 ns/op | $\ge 50,000\text{ events/sec}$ |
| **Gateway Batch Ingestion (100/batch)** | **42,467,023 events/sec** | 2,355 ns/op | $\ge 50,000\text{ events/sec}$ |
| **Stream Processor (12 Partitions Concurrent)** | **1,449,421 events/sec** | 690.1 ns/op | $\ge 50,000\text{ events/sec}$ |
| **Statistical Baseline EWMA Update** | **175,495,860 updates/sec** | **5.7 ns/op** | $< 100\text{ ns/op}$ |
| **Anomaly Detector Full Evaluation** | **3,194,101 evaluations/sec** | 313.1 ns/op | — |
| **Decoupled Alert Dispatcher Pool** | **10,514,175 dispatches/sec** | **95.1 ns/op** | $\ge 1,000\text{ alerts/sec}$ |
| **Slow Webhook Ingestion Lag Under Chaos** | **0% lag (11.1 ms for 1,000 events)** | **< 12 µs/op** | 0% lag under 30s delay |
| **ChronoDB Storage Batch Upsert Ingestion** | **892,948 records/sec** | **111.9 µs/batch** | $\ge 100,000\text{ records/sec}$ |
| **ChronoDB Historical Time-Range Query (1k points)** | **70,250 queries/sec** | **14.2 µs/query** | $< 1\text{ ms/query}$ |
| **ChronoDB Dynamic Downsampling Rollup (1 hr / 1m)** | **52,464 rollups/sec** | **19.0 µs/rollup** | $< 5\text{ ms/rollup}$ |
| **Simulator Fleet Signal Generation** | **19,120,634 events/sec** | **52.3 ns/op** | $\ge 100,000\text{ events/sec}$ |
| **Simulator Micro-Batch Assembly & Dispatch** | **11,694,020 events/sec** | **4.2 µs/batch** | $\ge 100,000\text{ events/sec}$ |

---

## Milestone 008: ChronoDB Storage Durability & Persistent Volume Orchestration

Milestone 008 implements persistent storage durability, Write-Ahead Logging (WAL), crash recovery protocol, and Kubernetes StatefulSet volume orchestration conforming strictly to Constitution Principles I–V:
1. **Embedded LSM Disk-Backed Storage Engine (Constitution Principle V)**:
   - Replaces ephemeral memory storage with an embedded disk-backed Log-Structured Merge-tree (LSM) engine across 16 deterministic hash shards.
   - Non-blocking advisory directory flock (`LOCK`) prevents multiple competing daemons on the same volume.
2. **Synchronous Write-Ahead Logging (WAL) & Durability**:
   - Fixed-overhead binary WAL frame layout with magic sentinel (`0x50554C53`), big-endian length, IEEE CRC32 checksum, and operation tags.
   - Offsets are committed to Redpanda strictly **after** WAL batch append and physical disk `fsync`, guaranteeing zero data loss across ungraceful process terminations (`kill -9`).
3. **Crash Recovery Protocol & Replay Invariance (Constitution Principle III)**:
   - On daemon startup, shards automatically discover existing SSTables and replay uncheckpointed WAL frames, recovering 100% of acknowledged records in $<5\text{s}$ for 1,000,000 records.
   - Resilient against torn trailing writes at crash boundaries, discarding corrupt partial frames without failing startup.
   - Idempotent upsert deduplication ensures replay of re-delivered broker batches produces **0 duplicate records**.
4. **Immutable SSTables & Murmur3 Bloom Filters**:
   - Memtables flush to immutable `.sst` segments with length-prefixed protobuf data blocks, sparse index blocks, and 10 bits/key Murmur3 Bloom filters for zero-disk-read misses.
   - 32-byte fixed trailer (`IndexOffset`, `BloomOffset`, `RecordCount`, magic `0x53535442`).
5. **Background Multi-Way Merge Compaction & TTL Pruning**:
   - Periodic multi-way merge consolidates fragmented SSTables, eliminates superseded key revisions, and purges expired records past configured retention windows (`STORAGE_RETENTION_DAYS`).
6. **Kubernetes StatefulSet & Dedicated PersistentVolumeClaims**:
   - Packaged with Helm as a Kubernetes `StatefulSet` with `volumeClaimTemplates` requesting dedicated `10Gi` `ReadWriteOnce` PVCs mounted at `/var/lib/pulse/chronodb`.
   - Headless Service provides deterministic DNS peer resolution (`chronod-0.pulse-chronod-headless`).
   - Non-root security context (`UID 65532`, `fsGroup: 65532`) and readiness probes checking disk writeability and $>5\%$ free storage space.


---

## Milestone 009: Security & Access Control

Milestone 009 implements defense-in-depth perimeter security, client identity anti-spoofing, fine-grained Role-Based Access Control (RBAC), decoupled webhook cryptographic integrity, Kubernetes NetworkPolicies, and secret management conforming strictly to Constitution Principles I–V:
1. **Edge Perimeter Authentication & Identity Anti-Spoofing (Constitution Principle I)**:
   - **Mutual TLS (mTLS)**: Enforces TLS 1.3 client certificate verification against trusted CA bundles on Ingestion Gateway HTTP (`:8080`) and gRPC (`:50051`).
   - **Pre-Shared API Keys**: Constant-time verification (`crypto/subtle.ConstantTimeCompare`) with zero-lock atomic stores (`atomic.Pointer`) for resource-constrained edge devices.
   - **Cryptographic Anti-Spoofing Invariant**: Binds client certificate Common Name (CN)/SAN to telemetry payload `reading.device_id`. If `identity.DeviceID != payload.device_id`, the perimeter immediately returns HTTP 403 Forbidden / gRPC `PermissionDenied` before broker publish.
   - **Token-Bucket Rate Limiting**: Sharded, low-contention token-bucket limiter dropping rogue floods with HTTP 429 (`Retry-After: 1`).
2. **ChronoDB Query API Authentication & RBAC**:
   - Zero-dependency pure Go JWT validator (`HS256`, `EdDSA`) verifying Bearer tokens on REST (`:8084`) and gRPC (`:50054`).
   - **Fine-Grained Role Hierarchy**:
     - `Viewer`: Historical telemetry range queries (`/api/v1/query`) and device catalog (`/api/v1/devices`).
     - `Operator`: Analytical rollups (`/api/v1/query/rollup`) + health metrics.
     - `Admin`: Administrative operations, compaction triggers, and cross-tenant actions.
   - **Tenant & Device Scope Isolation**: Token claims support `allowed_devices: ["turbine-alpha-*"]` glob patterns. Query requests specifying target `device_id` outside token scopes are rejected with HTTP 403 Forbidden.
3. **Decoupled Webhook Cryptographic Integrity (HMAC-SHA256)**:
   - `alertd` attaches cryptographic signatures to outbound alert webhooks:
     - `X-Pulse-Signature: sha256=<hex_digest>`
     - `X-Pulse-Timestamp: <unix_epoch_seconds>`
   - Canonical signing string: `{timestamp}.{raw_json_body}`.
   - **Replay Protection**: Reject alerts outside clock drift window ($|T_{\text{current}} - T_{\text{header}}| \le 300\text{s}$).
   - **Receiver Verification SDK**: Public `pkg/webhookverifier` SDK and HTTP middleware for downstream systems.
4. **Kubernetes NetworkPolicies & Secret Management**:
   - **Default-Deny Ingress**: All pods in namespace block inbound network traffic by default.
   - **Inter-Service Least-Privilege Rules**:
     - `gateway`: Ingress 8080/50051; egress only to Redpanda (9092) and DNS (53).
     - `processor`: Ingress 8081 (/healthz, /metrics); egress only to Redpanda (9092) and DNS.
     - `alertd`: Ingress 8083; egress to Redpanda (9092), external webhooks (443/80), and DNS.
     - `chronod`: Ingress 8084/50054; egress only to Redpanda (9092) and DNS.
   - **Native Secret Management**: Mounts certificates, API keys, JWT secrets, and HMAC keys as read-only volumes (`mode: 0400`) into non-root distroless containers (`UID 65532`).
5. **Developer & Security Operations Commands**:
   - `make certs`: Generates development CA, server TLS certs, and client certificates in `certs/`.
   - `make test-security`: Executes unit, integration, and anti-spoofing security verification suite.

---

## Milestone 010: Industrial Edge Bridge & Complex Event Processing (CEP) Engine

Milestone 010 implements the industrial edge translation daemon (`pulse-bridge`) and the in-memory Complex Event Processing (CEP) rule engine conforming strictly to Constitution Principles I–V:
1. **Industrial Edge Bridge Daemon (`pulse-bridge`)**:
   - **MQTT v3.1.1 / v5.0 Support**: Subscribes to industrial broker topics with single-level (`+`) and multi-level (`#`) wildcards, dynamic topic variable extraction (`{device_id}`, `{metric}`), and JSON payload normalization.
   - **Sparkplug B Payload Parsing**: Decodes standard industrial IoT Sparkplug B binary protobuf frames (`NBIRTH`, `NDATA`, `DBIRTH`, `DDATA`), extracting typed metrics and timestamps into canonical Pulse `TelemetryReading` messages.
   - **Modbus TCP Master Engine**: Cyclically polls legacy PLCs via Function Codes 03 (Holding Registers) and 04 (Input Registers) with configurable timeouts, linear scaling equations ($y = \text{raw} \times m + c$), and multi-word endian conversions (`int16`, `uint16`, `int32`, `uint32`, `float32_be`, `float32_le`, `float32_swap`).
2. **Store-and-Forward Offline Buffering**:
   - Dual-tier buffering: In-memory absorption queue + append-only disk FIFO log (`/var/lib/pulse/bridge` or `/tmp/pulse-bridge-spool`).
   - Crash-resilient binary framing (`PUBR` 16-byte header with IEEE CRC32 checksum) and atomic manifest tracking (`manifest.json`) guaranteeing zero data loss during process termination or edge network partition.
   - Strict chronological backfill: Streams historical spooled records in event-time order over mTLS gRPC (`:50051`) upon reconnect, maintaining deterministic watermark progression.
3. **Partition-Local Complex Event Processing (CEP) Engine**:
   - Zero-lock stateful pattern matching running directly inside `pulse-processor` partition worker goroutines (Constitution Principle I).
   - Evaluates multi-stage temporal sequences (`EventA FOLLOWED_BY EventB WITHIN ΔT`) with event-time ordering.
   - Deterministic candidate expiration governed by partition event-time watermarks (Constitution Principle II).
   - Cross-metric mathematical expression evaluator supporting multivariate formulas (e.g. `power_mw / fuel_flow < 0.70`).
4. **Composite Incident Synthesis & Live Rule Reloading**:
   - Synthesizes cascading multi-metric alarms into structured `CompositeIncidentEvent` notifications, reducing downstream alarm noise by $\ge 90\%$ and routing to `telemetry.alerts` for `alertd` (Constitution Principle IV).
   - Live atomic rule registry swapping via administrative REST endpoint (`POST /api/v1/rules/reload`) and `SIGHUP` signal without stream processor downtime.
5. **Edge Bridge & CEP Commands**:
   - `make test-edge-cep`: Executes unit and integration test suites for protocol bridge and CEP pattern matcher.
   - `./bin/bridge --config deployments/config/bridge.yaml`: Launches standalone edge bridge daemon.

---

## Quickstart

### 1. Start Redpanda Cluster
```bash
make up
# or
docker compose -f deployments/docker-compose.yml up -d
```

### 2. Compile Protobuf Definitions
```bash
make proto
```

### 3. Build & Run Services
```bash
make build

# Start Ingestion Gateway (HTTP :8080, gRPC :50051)
./bin/gateway

# Start Stream Processor (HTTP :8081)
./bin/processor

# Start Decoupled Alert Dispatcher (HTTP :8083)
./bin/alertd

# Start ChronoDB Storage Sink & Query Service (HTTP :8084, gRPC :50054)
./bin/chronod

# Start Industrial Edge Bridge (HTTP :9095)
./bin/bridge --config deployments/config/bridge.yaml
```

### 4. Test Ingestion, Queries & Observability
```bash
# Health Checks
curl -s http://localhost:8080/healthz | jq   # Gateway
curl -s http://localhost:8081/healthz | jq   # Processor
curl -s http://localhost:8083/healthz | jq   # Alert Dispatcher
curl -s http://localhost:8084/healthz | jq   # ChronoDB Storage Service

# Ingest Telemetry Measurement
curl -X POST http://localhost:8080/api/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "turbine-alpha-01",
    "timestamp_ns": '$(date +%s%N)',
    "metric_name": "bearing_temp",
    "value": 72.8,
    "unit": "celsius"
  }' | jq

# Query Historical Time Series from ChronoDB
curl -s "http://localhost:8084/api/v1/query?device_id=turbine-alpha-01&metric_name=bearing_temp&start_time=0&end_time=2000000000000000000" | jq

# Query Dynamic Analytical Rollup (1-minute downsampled buckets)
curl -s "http://localhost:8084/api/v1/query/rollup?device_id=turbine-alpha-01&metric_name=bearing_temp&start_time=0&end_time=2000000000000000000&step=1m" | jq

# Discover Active Devices and Metrics Catalog
curl -s http://localhost:8084/api/v1/devices | jq

# Check Prometheus Metrics
curl -s http://localhost:8080/metrics | grep pulse_ingest
curl -s http://localhost:8081/metrics | grep pulse_processor
curl -s http://localhost:8083/metrics | grep pulse_alert
curl -s http://localhost:8084/metrics | grep pulse_storage
```

### 5. Run High-Throughput Load Simulator & Chaos Harness
```bash
# 1. Steady-state high-throughput SLA verification (50,000 ev/s over gRPC streaming)
./bin/simulator --rate 50000 --devices 1000 --duration 60s --transport grpc

# 2. Bursty surge profile (3x multiplier spike)
./bin/simulator --rate 20000 --devices 500 --profile burst --burst-multiplier 3.0 --duration 45s

# 3. Offline network disconnect & recovery backfill chaos (10% fleet disconnected)
./bin/simulator --rate 10000 --devices 500 --profile offline --offline-pct 0.10 --duration 45s

# 4. Statistical anomaly chaos injection (5% step spikes z >= 5.0 and flatlines)
./bin/simulator --rate 5000 --devices 200 --profile anomaly --anomaly-pct 0.05 --duration 30s

# 5. Machine-readable JSON SLA compliance report
./bin/simulator --rate 50000 --devices 1000 --duration 30s --json
```

### 6. View Observability Stack & Grafana Dashboards
```bash
# Start Prometheus, Grafana, and Alertmanager
make obs-up

# Print direct URLs for all dashboards
make dashboards

# Run automated observability verification harness
./scripts/verify_observability.sh

# Stop observability stack
make obs-down
```

### 7. Run Test Suites, Benchmarks & Security Verification
```bash
make test
make test-security   # Runs mTLS, anti-spoofing, RBAC, and Webhook HMAC tests
make test-edge-cep   # Runs Edge Bridge and CEP engine test suites
make bench
make e2e
make sim             # Runs 50k events/sec simulator load test
make certs           # Generates local test certificates in certs/
make k8s-verify      # Verifies Kubernetes packaging, NetworkPolicies and Secrets
```
