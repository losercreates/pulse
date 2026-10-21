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

## Verified Benchmark Performance

Running on modest development hardware (Intel Core i7-1165G7):

| Benchmark Scenario | Measured Throughput | Latency per Op | Target Spec |
| :--- | :---: | :---: | :---: |
| **Gateway Single Reading Ingestion** | **5,727,515 events/sec** | 174.6 ns/op | $\ge 50,000\text{ events/sec}$ |
| **Gateway Batch Ingestion (100/batch)** | **42,467,023 events/sec** | 2,355 ns/op | $\ge 50,000\text{ events/sec}$ |
| **Stream Processor (12 Partitions Concurrent)** | **1,449,421 events/sec** | 690.1 ns/op | $\ge 50,000\text{ events/sec}$ |
| **Single-Core Window Aggregator + HLL** | **972,302 events/sec** | 1,028 ns/op | — |

*Target Requirement: $\ge 50,000\text{ events/sec}$ (stream processor exceeds target by nearly $30\times$).*

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

### 3. Build & Run the Gateway
```bash
make build
./bin/gateway
```

### 4. Test Ingestion via HTTP
```bash
# Health Check
curl -s http://localhost:8080/healthz | jq

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

# Check Prometheus Metrics
curl -s http://localhost:8080/metrics | grep pulse_ingest
```

### 5. Run Test Suites & Benchmarks
```bash
make test
make bench
```
