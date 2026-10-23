# Phase 0 Research: ChronoDB Polyglot Storage Sink & Historical Query Engine

**Feature**: `004-chronodb-storage-query`  
**Date**: 2026-10-23  
**Status**: Complete  

---

## 1. Polyglot Architecture & Process Boundary

### Context & Principle V Alignment
Constitution Principle V mandates:
> *"Go is the standard language for Ingestion Gateway, Stream Processor, and Alert Dispatcher. ChronoDB (C++) is accessed strictly across a defined polyglot boundary (thin CGo or gRPC process boundary) sharded by device ID range. Storage write backpressure MUST propagate naturally via Redpanda consumer lag rather than unbounded in-memory queuing."*

### Architectural Evaluation: gRPC vs. CGo

| Criterion | In-Process CGo Binding | Decoupled gRPC Service Boundary | Decision / Impact |
| :--- | :--- | :--- | :--- |
| **Fault Isolation** | C++ memory corruption / segfault crashes the entire Go binary | Process-isolated: crash of storage daemon does not crash ingestion or streaming | **gRPC Wins**: Prevents cascade failures |
| **Cross-Platform & CI Build** | Complex C++ toolchain and header dependencies during `go build` | Standard Protobuf/gRPC code generation; clean mockability | **gRPC Wins**: Reproducible CI without CGo flags |
| **Sharding & Scalability** | Limited to single node in-process memory space | Naturally distributes across independent ChronoDB instances over network | **gRPC Wins**: Directly enables multi-shard routing |
| **Backpressure Propagation** | CGo function call blocks goroutine | gRPC backpressure + channel bounded buffers pause Franz-go fetch loop | **Both Pass**: Propagates to Redpanda consumer lag |
| **Latency Overhead** | $< 1\ \mu\text{s}$ per call | $\approx 25\text{--}50\ \mu\text{s}$ per batch over local domain socket / TCP loopback | **Acceptable**: Batching 100–1,000 aggregates amortizes overhead to $< 0.1\ \mu\text{s}$ per aggregate |

### Decision
Implement ChronoDB as an explicit, high-performance **gRPC service boundary** (`ChronoDBService`) with a dedicated Go consumer daemon (`chronod` / `internal/storage`). For deterministic unit testing and standalone local execution, provide both an in-memory/embedded columnar engine and client adapters that connect to live or mock ChronoDB gRPC endpoints.

---

## 2. Idempotent Upsert & Deduplication Invariant

### Context & Principle III Alignment
Constitution Principle III mandates:
> *"Writes to ChronoDB instances MUST be idempotent upserts keyed by `(device_id, metric, window_start, window_end)`. Reprocessing after a consumer crash or rebalance MUST recompute identical window results with zero double-counting or data loss."*

### Primary Composite Key Design
Every stored window aggregate is indexed by a deterministic composite primary key:
```text
Primary Key: device_id # metric_name # window_start_ns # window_end_ns
```
Example: `turbine-01#vibration#1700000000000000000#1700000010000000000`

### Idempotency Mechanics
1. **Insert vs. Overwrite**: When writing aggregate batch $B$:
   - If the key does not exist: Insert the record.
   - If the key already exists: Overwrite with the latest aggregate payload or assert equivalence. Because window boundaries are deterministic (governed by event-time watermarks per Principle II), reprocessed aggregates for identical windows have identical counts, sums, min, and max.
2. **Atomic Offset Commit**:
   - Franz-go consumer fetches records from `telemetry.aggregates`.
   - Batch is written and fsync-acknowledged by ChronoDB.
   - Consumer offsets are committed to Redpanda *only* after storage acknowledgment.
   - Under consumer crash before offset commit, re-reading the batch from Redpanda safely overwrites existing entries with zero duplicate increments or data divergence.

---

## 3. Storage Sharding & Device Locality

### Sharding Strategy
- Devices are hashed using Murmur3 / xxHash:
  $$\text{ShardID} = \text{Hash}(\text{device\_id}) \pmod{N_{\text{shards}}}$$
- Sharding by `device_id` guarantees that all metrics and all historical windows for a given machine reside on the **exact same shard**.
- **Query Affinity**: Single-device queries (`GET /api/v1/query?device_id=X`) are routed directly to the single shard holding device $X$, avoiding cross-shard scatter-gather overhead.
- Multi-device queries (e.g. catalog discovery) execute lightweight parallel shard scans with $O(1)$ memory streaming.

---

## 4. Analytical Downsampling & Dynamic Rollup Mathematics

### Challenge
A client querying 7 days of 10-second tumbling windows requests $7 \times 86,400 / 10 = 60,480$ data points per metric. Transferring and rendering this volume overwhelms frontend charts and network connections.

### Rollup Formulation
When downsampling from base 10-second windows into target step intervals $\Delta T$ (e.g. 5 minutes or 1 hour):
1. **Time Bucket Assignment**:
   $$\text{BucketStart} = \lfloor \text{window\_start\_ns} / \Delta T \rfloor \times \Delta T$$
2. **Aggregations over Sub-Windows** $i \in \text{Bucket}$:
   - **Total Count**: $\text{Count}_{\text{bucket}} = \sum_{i} \text{count}_i$
   - **Total Sum**: $\text{Sum}_{\text{bucket}} = \sum_{i} \text{sum}_i$
   - **Global Minimum**: $\text{Min}_{\text{bucket}} = \min_{i}(\text{min}_i)$
   - **Global Maximum**: $\text{Max}_{\text{bucket}} = \max_{i}(\text{max}_i)$
   - **Weighted Mean**:
     $$\text{Mean}_{\text{bucket}} = \frac{\sum_{i} \text{sum}_i}{\sum_{i} \text{count}_i} = \frac{\text{Sum}_{\text{bucket}}}{\text{Count}_{\text{bucket}}}$$

This mathematical property allows instantaneous, exact re-aggregation directly from stored window aggregates without scanning raw sensor readings.

---

## 5. Port Allocations & Observability

To prevent port conflicts with Redpanda (`:9092`), Pandaproxy (`:8082`), Gateway (`:8080`, `:50051`), Processor (`:8081`), and Alertd (`:8083`):
- **Storage Sink & Query HTTP Server**: Port `:8084` (`/healthz`, `/readyz`, `/metrics`, `/api/v1/...`)
- **ChronoDB gRPC Server**: Port `:50054`
