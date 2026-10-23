# Feature Specification: ChronoDB Polyglot Storage Sink & Historical Query Engine

**Feature Branch**: `004-chronodb-storage-query`

**Created**: 2026-10-23

**Status**: Draft

**Input**: User description: "Create milestone 004: ChronoDB polyglot storage sink for telemetry aggregates and historical query service"

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Idempotent Aggregate Ingestion & Polyglot Persistence (Priority: P1) 🎯 MVP

As a data platform operator, I want closed window aggregates consumed from the streaming broker and persisted into time-series storage idempotently, so that stream processor restarts or partition rebalances never cause double-counting, duplicate records, or data loss.

**Why this priority**: Foundational core requirement. Without durable, idempotent aggregate ingestion across the defined storage boundary, historical time-series analytics and trend evaluation cannot function.

**Independent Test**: Ingest 100 window aggregates into the sink, replay the same 100 aggregates from the message broker, and verify that the storage engine contains exactly 100 unique records with identical statistics and no duplicated counts or sums.

**Acceptance Scenarios**:

1. **Given** a stream of closed window aggregates on the broker topic, **When** the storage sink consumes them, **Then** records are persisted to storage sharded by device identifier with an idempotent composite key `(device_id, metric_name, window_start_ns, window_end_ns)`.
2. **Given** duplicate aggregates replayed after a consumer rebalance or crash, **When** re-ingested by the sink, **Then** existing storage entries are idempotently upserted with identical values and no counts or sums are doubled.
3. **Given** downstream storage latency or disk flush operations, **When** write throughput is throttled, **Then** backpressure naturally manifests as consumer group lag on the message broker rather than unbounded in-memory memory growth.

---

### User Story 2 - Historical Telemetry Time-Range Query Interface (Priority: P2)

As an analytics engineer or IoT dashboard, I want to query historical telemetry window aggregates for specific devices and metrics across arbitrary time ranges, so that operators can inspect past machine performance, spot degradation, and visualize telemetry trends.

**Why this priority**: Unlocks historical visibility for operators and dashboard clients to retrieve point-in-time and range telemetry with sub-second responsiveness.

**Independent Test**: Query a 24-hour time range for a device and verify that all matching aggregate intervals are returned in chronological order within 50ms with correct aggregations (count, sum, min, max, mean).

**Acceptance Scenarios**:

1. **Given** historical aggregates stored for device `turbine-01`, **When** a client submits a time-range query specifying device ID, metric name, start time, and end time, **Then** all matching aggregate intervals are returned sorted by window start time.
2. **Given** a query for a non-existent device or time range with no data, **When** executed, **Then** the service returns an empty result set with a 200 OK status without throwing errors.
3. **Given** invalid query parameters (e.g. `start_time > end_time` or missing `device_id`), **When** submitted, **Then** the query service returns a clear 400 Bad Request error with descriptive feedback.

---

### User Story 3 - Multi-Resolution Analytical Rollups & Downsampling (Priority: P2)

As a monitoring dashboard visualizing long time horizons (e.g., 7 days or 30 days), I want to query downsampled aggregates dynamically bucketed by coarser resolutions (e.g., 1-minute, 5-minute, 1-hour intervals), so that charts render smoothly without transferring millions of individual 10-second data points.

**Why this priority**: Long-range queries transferring raw high-frequency intervals exhaust client memory and network bandwidth. Dynamic rollups provide fast, bandwidth-efficient visualization.

**Independent Test**: Ingest 360 ten-second window aggregates (representing 1 hour of telemetry), request a rollup with a 5-minute bucket step, and verify exactly 12 aggregated buckets are returned with mathematically accurate rollups (combining sums, recalculating weighted means, finding global min/max).

**Acceptance Scenarios**:

1. **Given** fine-grained 10-second aggregates over a 24-hour period, **When** a query requests a 5-minute downsampling rollup, **Then** the service groups and aggregates intervals into 5-minute buckets, recomputing combined sums, overall min/max, total counts, and weighted means.
2. **Given** a requested rollup step that does not align evenly with window boundaries, **When** evaluated, **Then** each sub-window is deterministically assigned to the bucket containing its start time.

---

### User Story 4 - Device Discovery & Metric Metadata Catalog (Priority: P3)

As an edge operator or fleet supervisor, I want to discover all active devices and their recorded telemetry metrics in storage, so that UI applications can populate device selector dropdowns and metric navigation trees automatically.

**Why this priority**: Essential for self-service analytics and dashboard discovery across large fleets of industrial sensors.

**Independent Test**: Ingest aggregates from 5 distinct devices with 3 metrics each, call the metadata catalog API, and verify all 5 device IDs and their respective metric names are returned with first-seen and last-seen timestamps.

**Acceptance Scenarios**:

1. **Given** telemetry persisted from multiple devices, **When** a client requests the active devices catalog, **Then** a list of unique device IDs with their first-seen and last-seen timestamps is returned.
2. **Given** a specific device ID, **When** a client requests available metrics, **Then** a list of all metrics recorded for that device is returned with current value bounds.

---

### Edge Cases

- **Storage Writer Outages**: If the storage engine becomes temporarily unreachable, the consumer group pauses offset commits and relies on the broker's log retention without dropping incoming aggregates.
- **Out-of-Order Aggregate Arrivals**: Aggregates emitted out-of-order due to late-arriving telemetry are merged into the historical time index without corrupting adjacent intervals.
- **Massive Time-Range Queries**: Queries requesting millions of data points enforce a maximum interval limit per request (e.g. 10,000 points) and provide pagination tokens to prevent server memory exhaustion.
- **Sparse or Intermittent Data**: Time ranges with gaps (e.g. equipment power-off) return only intervals with data, without generating synthetic zero-value records.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST consume closed window aggregates from the streaming broker topic `telemetry.aggregates` across all configured partitions.
- **FR-002**: System MUST persist window aggregates into time-series storage using an idempotent upsert key composed of `(device_id, metric_name, window_start_ns, window_end_ns)`.
- **FR-003**: System MUST guarantee that replaying identical aggregates produces the exact same storage state without duplicating counts, sums, or creating redundant records.
- **FR-004**: System MUST partition storage sharding across storage instances deterministically by device identifier to ensure single-shard locality for device queries.
- **FR-005**: Storage write backpressure MUST propagate naturally via broker consumer lag without unbounded in-memory queue growth.
- **FR-006**: System MUST commit consumer offsets to the broker only after write batches are durably acknowledged by the storage layer.
- **FR-007**: System MUST provide a historical query interface supporting filtering by `device_id`, `metric_name`, `start_time_ns`, and `end_time_ns`.
- **FR-008**: Query responses MUST return aggregate intervals in strictly ascending chronological order by `window_start_ns`.
- **FR-009**: System MUST support dynamic downsampled rollups over configurable time bucket intervals (e.g. 1m, 5m, 1h), computing combined sums, global min/max, total counts, and weighted means.
- **FR-010**: System MUST provide an active device and metric catalog endpoint listing all known devices, recorded metrics, and temporal bounds.
- **FR-011**: System MUST expose health probe endpoints (`/healthz` and `/readyz`) indicating broker consumer state and storage engine connectivity.
- **FR-012**: System MUST expose Prometheus metrics for write throughput, batch write latency, query duration histograms, and storage consumer lag on port `:8084`.

### Key Entities

- **StoredWindowAggregate**: Represents a persisted aggregation interval for a physical sensor reading. Key attributes:
  - `device_id` (string): Machine or sensor identifier (1 to 128 characters).
  - `metric_name` (string): Telemetry measurement name.
  - `window_start_ns` (int64): Window interval start timestamp in nanoseconds.
  - `window_end_ns` (int64): Window interval close timestamp in nanoseconds.
  - `count` (uint64): Number of raw observations in window.
  - `sum` (float64): Arithmetic sum of observations.
  - `min` (float64): Minimum observed value.
  - `max` (float64): Maximum observed value.
  - `mean` (float64): Precomputed average value.
- **TimeSeriesQuery**: Represents a request for historical telemetry. Key attributes:
  - `device_id` (string): Target device identifier.
  - `metric_name` (string): Target metric.
  - `start_time_ns` (int64): Query window start boundary.
  - `end_time_ns` (int64): Query window end boundary.
  - `step_ns` (optional int64): Downsampling bucket size in nanoseconds.
- **RollupBucket**: Represents a coarser aggregated interval synthesized from multiple underlying window intervals. Key attributes:
  - `bucket_start_ns` (int64): Bucket start boundary.
  - `bucket_end_ns` (int64): Bucket end boundary.
  - `count` (uint64): Total accumulated sample count.
  - `sum` (float64): Total accumulated sum.
  - `min` (float64): Global minimum across intervals.
  - `max` (float64): Global maximum across intervals.
  - `mean` (float64): Weighted mean across intervals.
- **DeviceCatalogEntry**: Represents device metadata in the storage catalog. Key attributes:
  - `device_id` (string): Unique device identifier.
  - `metrics` (list of strings): Recorded metric names.
  - `first_seen_ns` (int64): Earliest recorded telemetry timestamp.
  - `last_seen_ns` (int64): Latest recorded telemetry timestamp.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% idempotent storage: replaying 10,000 aggregates generates zero duplicate rows, zero double-counted statistics, and identical query results.
- **SC-002**: Storage sink sustains $\ge 50,000$ aggregate writes per second under sustained streaming load.
- **SC-003**: p95 query latency is $< 50$ms for single-device 24-hour time-range queries (up to 8,640 intervals).
- **SC-004**: Multi-resolution analytical rollup queries over 7-day windows complete in $< 200$ms.
- **SC-005**: Storage write pauses or flushes propagate backpressure into broker consumer lag within 2 seconds without out-of-memory crashes.

---

## Assumptions

- Closed window aggregates are published to message broker topic `telemetry.aggregates` in Protobuf v3 format.
- Device identifiers are valid UTF-8 strings between 1 and 128 characters.
- Timestamps represent nanoseconds since the Unix epoch.
- Storage engine supports key-value range scanning and atomic multi-key batch writes.
