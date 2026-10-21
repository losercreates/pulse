# Phase 0 Research: Stream Processing Engine & Event-Time Watermarking

**Feature**: `002-stream-processor-core`
**Date**: 2026-10-21
**Status**: Complete

---

## 1. Concurrency Architecture: Partition-Local Processing

### Decision
Assign exactly one dedicated worker goroutine per Kafka/Redpanda partition. All window accumulators, watermark clocks, and active priority queues are isolated inside the worker's memory loop without cross-thread locking.

### Rationale
- **Zero-Lock Hot Path**: As mandated by Constitution Principle I, telemetry events are strictly keyed by `device_id`. Because all events for a given device are guaranteed to land on the same broker partition, their event-time ordering and window state are strictly local to that partition's goroutine.
- **Linear Multi-Core Scalability**: Partition consumers scale horizontally with the number of partitions (e.g. 12 partitions = 12 independent worker goroutines) without mutex bottlenecks or lock convoying.
- **Deterministic Rebalance Handling**: Utilizing `kgo.CooperativeStickyBalancers()` ensures that when consumers join or leave, only revoked partitions are paused, flushed, and cleanly handed off.

### Alternatives Considered
- **Shared Concurrent Map (`sync.Map` / `RWMutex`)**: Massive lock contention and cache-line bouncing at $\ge 50,000\text{ events/sec}$.
- **Actor Model / Distributed Actor Framework**: Unnecessary dependency overhead (e.g., ProtoActor); Go native channels and goroutine loops provide superior memory efficiency and sub-microsecond scheduling.

---

## 2. Event-Time Watermarking & Idle Partition Defense

### Decision
Implement a monotonic, event-driven watermark tracker per partition:
$$\text{Watermark} = \max(\text{SeenEventTime}) - \text{AllowedLateness}$$
where `AllowedLateness` defaults to $5\text{ seconds}$ (configurable).
To prevent partition starvation when a sensor cluster goes idle, an **idle partition heartbeat timer** (default: $1\text{ second}$) advances the watermark if no new events arrive, ensuring open windows are not blocked from closing indefinitely.

### Rationale
- **Zero Wall-Clock Window Closes**: Aligns with Constitution Principle II. Closing windows by event-time watermarks guarantees that replaying historical streams produces mathematically identical window closures.
- **Elastic Jitter Absorption**: Allowed lateness naturally absorbs network delay and minor edge clock drift without dropping data.
- **Starvation Immunity**: Idle partition timeouts prevent deadlocks where inactive partitions stall global or downstream rollups.

### Alternatives Considered
- **Wall-Clock Periodic Watermarks**: Closes windows based on system time rather than data time; completely fails on bursty or offline-replay datasets.
- **Global Minimum Watermark on Ingestion**: Requires inter-goroutine coordination and locks across all partition consumers, destroying hot-path throughput.

---

## 3. Window State & Expiration Discovery: Priority Queue (Min-Heap)

### Decision
For each partition, maintain:
1. `map[WindowKey]*Window` for $O(1)$ lookup and state accumulation on each incoming event.
2. A Min-Heap Priority Queue (`container/heap`) of active `*Window` pointers ordered by `window_end_ns`.

When the partition watermark advances to $W$:
- Check `heap.Peek().window_end_ns \le W`.
- If true, pop the expired window, emit the computed `WindowAggregate`, delete from the map, and repeat until `heap.Peek().window_end_ns > W`.

### Rationale
- **$O(1)$ Expiration Discovery**: The engine never performs linear scans over active windows. Checking whether any window is ready to close takes $O(1)$ time, and popping takes $O(\log N)$ only when a window closes.
- **Memory Bound Enforcement**: Expired windows are immediately removed from memory upon emission, ensuring bounded memory footprint even under continuous 24/7 ingestion.

### Alternatives Considered
- **Linear Array / Slice Scan**: Scanning thousands of active windows on every watermark advance consumes excessive CPU cycles ($O(N)$ per event).
- **Time-Wheel / Bucket Ring**: Excellent for fixed tumbling intervals, but inflexible for variable sliding windows and arbitrary lateness bounds.

---

## 4. Statistical Accumulators & Approximate Distinct Counting (HyperLogLog)

### Decision
Store window state in a lightweight `WindowAccumulator`:
- Exact metrics: `Count` (`uint64`), `Sum` (`float64`), `Min` (`float64`), `Max` (`float64`), `Mean` (`float64`).
- Distinct device/entity estimation: A compact 64-register HyperLogLog (HLL) sketch ($m=64$, standard error $\approx 13\%$, memory footprint $\le 64\text{ bytes}$ per window) using 64-bit FNV-1a hashing.

### Rationale
- **Zero-Allocation Hot Path**: Exact stats update in $O(1)$ operations with zero heap allocation per measurement.
- **Constant Memory Cardinality**: Using HyperLogLog enables estimating unique devices per window without storing raw string device IDs, keeping per-window memory overhead fixed under 256 bytes regardless of device fleet size.

### Alternatives Considered
- **Exact Set (`map[string]struct{}`)**: Memory explodes linearly with cardinality (e.g., 50,000 devices $\times$ 32 bytes $\approx 1.6\text{ MB}$ per window $\times$ hundreds of open windows $\rightarrow$ gigabytes of RAM and heavy GC pressure).

---

## 5. Late Data Routing & Dead-Letter Topic

### Decision
When an event arrives with `event_time_ns < watermark`:
1. Reject event from active window aggregation.
2. Publish record to the dead-letter topic `telemetry.late` using the existing Franz-go producer with headers `x-late-watermark-ns` and `x-late-duration-ns`.
3. Increment the metric `pulse_stream_late_events_total{partition="..."}`.

### Rationale
- **Closed Window Immutability**: Guarantees that emitted window aggregates are immutable, preventing cascading database updates and race conditions downstream.
- **Full Traceability**: Allows offline devices (e.g. ships, mining trucks) to be audited without corrupting real-time streaming SLAs.

---

## 6. Checkpointing & Idempotent Crash Invariants

### Decision
Commit consumer offsets periodically and immediately following window closure emissions. All aggregate emission records include the deterministic key:
`(device_id, metric, window_start_ns, window_end_ns)`
Downstream storage (ChronoDB in Milestone 004) performs idempotent upserts on this primary key.

### Rationale
- Conforms to Constitution Principle III: At-least-once delivery with idempotent writes guarantees that reprocessing after a crash recomputes identical aggregate values with zero double-counting.
