# Feature Specification: Stream Processing Engine and Event-Time Watermarking

**Feature Branch**: `002-stream-processor-core`

**Created**: 2026-10-21

**Status**: Draft

**Input**: User description: "Create the specification for milestone 002: Stream Processing Engine and Event-Time Watermarking. Feature Name: Stream Processing Engine with Event-Time Watermarking and Windowed Aggregations. Scope: Concurrency & Partition-Consumer Model (1 goroutine/partition, partition-local state, zero hot-path locks), Event-Time Watermarking Engine (track max seen event time, allowed lateness bound, zero wall-clock closes), Window Aggregation Engine (tumbling & sliding windows, close-time priority queue, count/sum/min/max/avg/HLL distinct counts), Late Data Handling & Dead-Letter Sink (never corrupt closed windows, route to telemetry.late), Offset Checkpointing & Failure Recovery (commit offsets only after emission/checkpoint, at-least-once idempotent recovery), Observability & Testing Invariants (watermark lag, consumer lag per partition, out-of-order test invariants)."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Deterministic Out-of-Order Windowed Aggregation (Priority: P1) 🎯 MVP

Industrial IoT fleets operate across unreliable wireless networks where sensor packets frequently arrive delayed or out of order. Operators and downstream alerting systems require accurate statistical rollups (e.g. 10-second tumbling windows and 60-second sliding windows) computed over true measurement event time rather than network arrival time. The stream processing engine accumulates readings into open windows, dynamically advancing an event-time watermark (`max_event_time - allowed_lateness`), and closes windows deterministically once the watermark passes the window's end time.

**Why this priority**: Windowed aggregation under late and out-of-order data is the foundational core of a real-time stream processor. Without event-time watermarking, window calculations are nondeterministic and easily corrupted by network jitter.

**Independent Test**: Feed a sequence of 500 telemetry records with timestamps shuffled out-of-order within an allowed lateness bound (e.g. 5 seconds) into the stream processor. Verify that all records land in their correct event-time windows and that the emitted window aggregates (count, min, max, sum, average, distinct devices) are mathematically identical to an in-order arrival stream.

**Acceptance Scenarios**:

1. **Given** an open 10-second tumbling window spanning `[10:00:00, 10:00:10)` and an allowed lateness of 5 seconds, **When** a reading with timestamp `10:00:04` arrives after an event timestamped `10:00:08`, **Then** the reading is incorporated into the open window and updates the window's count, sum, min, and max.
2. **Given** the partition watermark advances to `10:00:10` (triggered by an event timestamped `10:00:15`), **Then** the window `[10:00:00, 10:00:10)` is closed and emitted to the aggregate stream within 100 milliseconds, and its state is evicted from memory.

---

### User Story 2 - Partition-Local Parallel Processing & Concurrency Isolation (Priority: P1)

Industrial telemetry pipelines must scale horizontally across multi-core systems without degrading under cross-thread synchronization overhead. By leveraging partition keys hashed on `device_id`, the stream processor assigns exactly one dedicated worker goroutine per message broker partition. All window states, watermark clocks, and aggregation sketches reside in partition-local memory without locks on the ingestion hot path.

**Why this priority**: Enforces Constitution Principle I. Eliminating lock contention across partition consumers allows linear scaling with available CPU cores while guaranteeing strict per-device ordering.

**Independent Test**: Ingest concurrent streams from 10,000 distinct devices distributed across 12 partitions, and measure CPU lock contention and throughput scaling. Confirm that partition workers operate completely independently with zero shared mutex bottlenecks on record processing.

**Acceptance Scenarios**:

1. **Given** multiple telemetry readings for device `turbine-01` routed to partition 3, **When** consumed by the stream processor, **Then** all processing occurs exclusively within partition 3's dedicated worker goroutine.
2. **Given** partition 3 is actively aggregating high-frequency events, **When** partition 7 experiences a traffic spike, **Then** partition 3 continues processing with zero mutex blocking or latency cross-contamination.

---

### User Story 3 - Late Data Routing and Dead-Letter Isolation (Priority: P2)

Edge machinery operating in underground mines, oil rigs, or marine vessels may remain disconnected for hours before flushing buffered readings. Any telemetry record whose event timestamp falls behind an already-closed window's watermark is considered excessively late. The engine isolates these late arrivals by routing them to a designated late-data sink (`telemetry.late`) instead of silently dropping them or corrupting closed historical windows.

**Why this priority**: Preserves window closure immutability and memory bounds (Constitution Principle II) while guaranteeing complete auditability of late-arriving telemetry.

**Independent Test**: Transmit a telemetry reading with a timestamp 30 minutes in the past after the watermark has already advanced. Verify that the reading is published to the dead-letter sink with metadata detailing its arrival latency, without reopening closed windows.

**Acceptance Scenarios**:

1. **Given** window `[10:00:00, 10:00:10)` has closed and the partition watermark is at `10:00:15`, **When** an event arrives with event timestamp `10:00:07`, **Then** the processor identifies the event as excessively late, routes it to the late-data dead-letter queue, and increments the late-data metric counter.
2. **Given** late data arriving at the processor, **Then** the previously emitted aggregate for `[10:00:00, 10:00:10)` remains immutable.

---

### User Story 4 - Offset Checkpointing and Crash Recovery (Priority: P2)

Stream processing nodes can experience hardware failures, container restarts, or partition rebalances. To guarantee at-least-once processing without aggregate corruption, consumer offsets are committed to the message broker only after the window state and emitted aggregates are safely persisted. Upon restart, the processor resumes consumption from the last committed offset and recomputes open window state idempotently.

**Why this priority**: Enforces Constitution Principle III. Ensures pipeline durability and prevents lost or double-counted telemetry aggregates during failures.

**Independent Test**: Terminate a stream processor instance mid-window during high-volume ingestion, restart the instance, and verify that consumption resumes from the last-consistent checkpoint with zero aggregate loss or double-counting.

**Acceptance Scenarios**:

1. **Given** a stream processor actively aggregating an open window, **When** the process restarts unexpectedly, **Then** the consumer re-joins the consumer group and resumes from the last committed offset.
2. **Given** reprocessed events following a restart, **Then** duplicate records re-aggregate idempotently to produce the same final window aggregate.

---

### User Story 5 - Real-Time Streaming Observability and Watermark Diagnostics (Priority: P3)

Platform operations and site reliability engineers require granular insight into streaming performance, consumer lag, watermark progression, and late-data rates to detect device connectivity outages or downstream backpressure.

**Why this priority**: Essential for operational reliability and meeting service-level objectives in industrial production environments.

**Independent Test**: Scrape the Prometheus `/metrics` endpoint while processing synthetic telemetry streams with variable delay, and verify that watermark lag, open window count, and processing latencies update accurately in real time.

**Acceptance Scenarios**:

1. **Given** an active streaming consumer, **When** Prometheus scrapes `/metrics`, **Then** metrics for `pulse_stream_watermark_lag_seconds`, `pulse_stream_open_windows_count`, and `pulse_stream_late_events_total` are exposed per partition.
2. **Given** an edge device cluster buffering data offline, **When** late events arrive, **Then** `pulse_stream_late_events_total` increments and alerts on unusual latency spikes.

---

### Edge Cases

- **Partition Idleness / Starvation**: If a specific partition experiences a lull in traffic, its event-time watermark would stall without progression, preventing open windows from closing. The engine implements an idle partition timeout that advances the watermark using a heartbeat or system time threshold.
- **Clock Drift Across Edge Devices**: Two sensors on the same machine may have slightly unsynchronized clocks. The allowed lateness parameter (default: 5 seconds) provides an elastic buffer absorbing inter-device skew.
- **Extreme Burst of Out-of-Order Readings**: An edge gateway recovering from network partition flushes 50,000 readings at once. The engine processes the burst sequentially within the partition worker without memory exhaustion by capping active window priority queue depth.
- **Consumer Group Rebalance Storm**: Flapping consumer instances trigger partition reassignment. The engine flushes open state or clears partition-local memory safely upon revocation, acquiring clean offsets upon assignment.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST consume serialized telemetry records from the `telemetry.raw` topic using a dedicated consumer group.
- **FR-002**: System MUST assign exactly one dedicated worker goroutine per topic partition, ensuring all windowing, watermarking, and aggregation state is partition-local with zero cross-goroutine locks on the message hot path.
- **FR-003**: System MUST compute an event-time watermark per partition defined as `watermark = max_event_time - allowed_lateness`, where `allowed_lateness` is a configurable duration (default: 5 seconds).
- **FR-004**: System MUST evaluate window closure conditions strictly against the partition event-time watermark advancing past `window_end`; closures based on wall-clock system time are strictly prohibited.
- **FR-005**: System MUST support Tumbling Windows (non-overlapping fixed duration, e.g. 10s) and Sliding Windows (overlapping fixed duration with sliding interval, e.g. 60s window with 10s slide).
- **FR-006**: System MUST maintain an efficient close-time priority queue of active windows per partition to enable $O(1)$ discovery of expired windows upon watermark advancement.
- **FR-007**: System MUST compute and accumulate the following statistics per window key `(device_id, metric, window_start, window_end)`:
  - Event count (`uint64`)
  - Arithmetic sum (`float64`)
  - Minimum value (`float64`)
  - Maximum value (`float64`)
  - Arithmetic mean (`float64`)
  - HyperLogLog (HLL) sketch estimating approximate distinct value cardinality.
- **FR-008**: System MUST route any record with event timestamp $< \text{watermark}$ (arriving after window close) to a dead-letter sink or topic `telemetry.late` without modifying closed window aggregates.
- **FR-009**: System MUST commit consumer offsets to Redpanda only after window state/aggregates are published or checkpointed, guaranteeing at-least-once delivery.
- **FR-010**: System MUST expose Prometheus metrics for partition watermark lag, open window count, late event rate, and consumer lag.

### Key Entities

- **WindowKey**: Identifies a unique aggregation slice:
  - `device_id` (string): Machine or sensor identifier.
  - `metric_name` (string): Measured physical quantity.
  - `window_start_ns` (int64): Window boundary start timestamp in epoch nanoseconds.
  - `window_end_ns` (int64): Window boundary end timestamp in epoch nanoseconds.
- **WindowAggregate**: The computed statistical summary for an expired window:
  - `key` (WindowKey): Identifying coordinates.
  - `count` (uint64): Number of sensor readings in the window.
  - `sum` (float64): Sum of values.
  - `min` (float64): Minimum recorded value.
  - `max` (float64): Maximum recorded value.
  - `mean` (float64): Average recorded value.
  - `hll_cardinality` (uint64): Estimated distinct value count from HyperLogLog sketch.
  - `emitted_at_ns` (int64): Timestamp when the window was emitted.
- **WatermarkTracker**: Partition-local clock tracking:
  - `partition_id` (int32): Partition number.
  - `max_event_time_ns` (int64): Highest event timestamp observed in the partition.
  - `current_watermark_ns` (int64): Calculated watermark threshold.
  - `allowed_lateness_ns` (int64): Configured out-of-orderness tolerance.
- **LateEventRecord**: Encapsulates an event rejected by the watermark:
  - `reading` (TelemetryReading): Original payload.
  - `watermark_at_arrival_ns` (int64): Watermark threshold when the record was rejected.
  - `lateness_duration_ns` (int64): Delay past the watermark.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of out-of-order events arriving within the allowed lateness bound are correctly aggregated into their matching event-time windows.
- **SC-002**: Window-close-to-emission latency p95 is less than 50 milliseconds once the partition watermark crosses the window end boundary.
- **SC-003**: The stream processing engine sustains a throughput of at least 50,000 events per second across 12 partitions under sustained load.
- **SC-004**: 100% of late events arriving after window closure are routed to the late-data sink; zero closed windows are corrupted or modified.
- **SC-005**: Zero duplicate or missing window aggregates across mid-window crash injection and consumer recovery.

## Assumptions

- Messages in `telemetry.raw` are partitioned by `device_id`, guaranteeing all readings for a single device arrive on the same partition.
- In-memory aggregation state per partition fits comfortably within available RAM (e.g., $< 512\text{ MB}$ per partition) when closed windows are promptly evicted.
- Downstream aggregate output will initially be written to an in-memory channel or Redpanda topic (`telemetry.aggregates`) before the ChronoDB sink is added in Milestone 004.
