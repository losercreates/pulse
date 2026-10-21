# Phase 1 Data Model: Stream Processing Engine & Event-Time Watermarking

**Feature**: `002-stream-processor-core`
**Date**: 2026-10-21
**Status**: Complete

---

## 1. Domain Entities & Schemas

### Entity: `WindowKey`
Identifies an immutable window aggregate slice across partition-local state.

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `device_id` | `string` | Machine or sensor identifier (1 to 128 characters). |
| `metric_name` | `string` | Telemetry metric identifier (e.g. `bearing_temp`). |
| `window_start_ns` | `int64` | Epoch timestamp of window start boundary (inclusive). |
| `window_end_ns` | `int64` | Epoch timestamp of window end boundary (exclusive). |

---

### Entity: `WindowAccumulator`
Maintains running statistical aggregates for an open window in partition-local memory.

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `key` | `WindowKey` | Window coordinates. |
| `count` | `uint64` | Total count of readings accumulated in this window. |
| `sum` | `float64` | Running arithmetic sum of reading values. |
| `min` | `float64` | Minimum recorded value in this window. |
| `max` | `float64` | Maximum recorded value in this window. |
| `hll_registers` | `[64]uint8` | Compact 64-register HyperLogLog sketch for distinct device counting. |
| `heap_index` | `int` | Internal index in the partition min-heap priority queue. |

---

### Entity: `WindowAggregate` (Emitted Record)
The published aggregate summary emitted to downstream consumers (anomaly detection and storage).

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `device_id` | `string` | Machine or sensor identifier. |
| `metric_name` | `string` | Telemetry metric name. |
| `window_start_ns` | `int64` | Window start timestamp in nanoseconds. |
| `window_end_ns` | `int64` | Window end timestamp in nanoseconds. |
| `count` | `uint64` | Total records in window. |
| `sum` | `float64` | Total sum. |
| `min` | `float64` | Minimum value. |
| `max` | `float64` | Maximum value. |
| `mean` | `float64` | Computed arithmetic mean (`sum / count`). |
| `approx_distinct` | `uint64` | Estimated distinct device count from HLL sketch. |
| `emitted_at_ns` | `int64` | Server timestamp when the window closed. |
| `partition_id` | `int32` | Source partition number that processed the window. |

---

### Entity: `WatermarkState`
Maintains the monotonic event-time clock per partition.

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `partition_id` | `int32` | Kafka/Redpanda partition ID. |
| `max_event_time_ns`| `int64` | Highest event timestamp observed in this partition. |
| `current_watermark_ns` | `int64` | Active watermark threshold (`max_event_time_ns - allowed_lateness_ns`). |
| `allowed_lateness_ns` | `int64` | Configurable allowed lateness bound (default: $5\text{ seconds}$). |
| `last_advance_time` | `int64` | System wall-clock timestamp of the last watermark progression. |

---

### Entity: `LateRecord` (Dead-Letter Payload)
Represents a record that arrived after its window was closed by the watermark.

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `reading` | `TelemetryReading` | The original raw telemetry reading. |
| `partition_id` | `int32` | The partition that received the late event. |
| `watermark_at_arrival_ns` | `int64` | Current watermark threshold when the event arrived. |
| `lateness_duration_ns` | `int64` | Latency beyond watermark (`watermark - timestamp_ns`). |

---

## 2. Window Boundary Assignment Rules

### Tumbling Windows (Fixed Duration $D$, Non-Overlapping)
For any event timestamp $T$:
$$\text{WindowStart} = \lfloor T / D \rfloor \times D$$
$$\text{WindowEnd} = \text{WindowStart} + D$$

### Sliding Windows (Duration $D$, Slide Interval $S$, where $S < D$)
An event timestamp $T$ belongs to all windows starting at $W_s$ such that:
$$T - D < W_s \le T \quad \text{and} \quad W_s \equiv 0 \pmod S$$
Each matching window receives the event update in the partition accumulator map.
