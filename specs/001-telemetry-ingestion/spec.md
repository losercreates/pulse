# Feature Specification: Telemetry Ingestion and Ingestion Gateway

**Feature Branch**: `001-telemetry-ingestion`

**Created**: 2026-10-20

**Status**: Draft

**Input**: User description: "Create the specification for milestone 001: Redpanda Telemetry Ingestion and Ingestion Gateway. Scope: Docker Compose setup for a single-node Redpanda cluster with topic provisioning (telemetry.raw, partitioned by device_id). Protobuf schema definition (TelemetryReading: device_id, timestamp, metric_name, value, unit, metadata). Go Ingestion Gateway supporting gRPC and HTTP/JSON endpoints that validate incoming readings and publish them with device_id partition keys. Metrics & Health check endpoints (Prometheus metrics for ingestion rate and error counts)."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Real-Time Telemetry Transmission via High-Throughput Ingestion (Priority: P1)

Industrial edge devices and fleet gateways continuously emit high-frequency telemetry readings (vibration, temperature, pressure, power). The ingestion system accepts these readings via low-latency streaming protocols and standard web protocols, immediately validating and acknowledging receipt so edge devices can flush local buffers.

**Why this priority**: Without a resilient, high-throughput ingestion endpoint, no sensor data enters the streaming pipeline. This forms the foundational entry point of the entire platform.

**Independent Test**: Can be verified by sending individual and batched telemetry readings via gRPC and HTTP endpoints and confirming immediate HTTP 202 / gRPC OK acknowledgement alongside arrival in the underlying message log.

**Acceptance Scenarios**:

1. **Given** a connected industrial sensor device emitting a valid telemetry payload, **When** the payload is sent via gRPC or HTTP to the ingestion gateway, **Then** the gateway validates the schema, acknowledges receipt within 10 milliseconds, and forwards the record to the message broker.
2. **Given** an edge gateway emitting a batch of 500 telemetry records, **When** the batch is posted to the ingestion batch endpoint, **Then** all valid records are ingested and the response confirms the count of successfully processed readings.

---

### User Story 2 - Device-Partitioned Ordered Message Delivery (Priority: P1)

Industrial telemetry requires strict per-device ordering for accurate windowed aggregation and stateful anomaly detection. The ingestion system maps every incoming reading to the streaming broker using the unique device identifier as the partition key.

**Why this priority**: Under Constitution Principle I, ordering guarantees are maintained at the partition level per device without cross-partition locking. Partitioning by `device_id` is a core architectural invariant.

**Independent Test**: Send a sequence of 1,000 monotonically timestamped readings for a single device across concurrent network connections, and verify they are all assigned to the same partition and consumed in strict order.

**Acceptance Scenarios**:

1. **Given** multiple readings originating from device `dev-4921`, **When** the gateway receives and publishes them to the telemetry stream, **Then** all readings for `dev-4921` are routed to the exact same partition determined by the hash of `dev-4921`.
2. **Given** concurrent streams from 100 distinct devices, **When** ingested concurrently, **Then** the traffic is evenly distributed across all provisioned partitions without hot-spot bottlenecks.

---

### User Story 3 - Ingestion Health & Operational Telemetry (Priority: P2)

Operations and infrastructure engineers need continuous visibility into the gateway's throughput, error rates, and connectivity to the message broker to ensure pipeline health.

**Why this priority**: Operational observability is essential to identify device network failures, broker backpressure, and packet drop spikes before downstream processors degrade.

**Independent Test**: Query the gateway health and metrics endpoints while running synthetic ingestion traffic, verifying that throughput counters, error rates, and broker connectivity states update in real time.

**Acceptance Scenarios**:

1. **Given** the gateway is running and connected to the broker, **When** an orchestrator sends a request to `/healthz`, **Then** the service returns HTTP 200 with component readiness status.
2. **Given** ingestion traffic is flowing through the gateway, **When** the metrics endpoint `/metrics` is scraped, **Then** standard metrics for ingestion rate, byte volume, and error counters are exposed accurately.

---

### User Story 4 - Clock Skew and Malformed Payload Defense (Priority: P2)

Edge sensors may suffer from firmware bugs, clock drift, or corrupted network frames. The ingestion system guards against invalid data by rejecting malformed payloads or readings with timestamps beyond sanity bounds (e.g. far future timestamps).

**Why this priority**: Prevents bad edge data from polluting downstream watermark engines or triggering false anomaly alerts.

**Independent Test**: Transmit payloads with missing required fields, non-numeric values, or future timestamps exceeding acceptable skew, and verify they are rejected with 400-series errors and tracked in error metrics.

**Acceptance Scenarios**:

1. **Given** a telemetry payload with a timestamp more than 15 minutes in the future, **When** received by the gateway, **Then** the gateway rejects the reading with a descriptive clock skew validation error.
2. **Given** a payload missing a `device_id` or `metric_name`, **When** received, **Then** the gateway returns an invalid payload status and increments the malformed data metric counter.

---

### Edge Cases

- **Broker Connection Interruption**: When the downstream message broker temporarily loses a partition leader or disconnects, the gateway buffers bounded pending messages and returns backpressure error codes (HTTP 503 / gRPC UNAVAILABLE) to edge devices rather than dropping records silently.
- **Extreme Clock Skew**: If a sensor emits an epoch timestamp of 0 or a year in the 1970s, it is rejected at the gateway boundary as malformed.
- **High-Cardinality Metric Names**: Arbitrary sensor metric strings are validated for length (maximum 128 characters) and allowed characters to prevent memory exhaustion.
- **Large Batches**: Batches exceeding maximum record thresholds (e.g., > 1,000 readings per payload) are rejected with a payload-too-large status to preserve gateway responsiveness.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept industrial telemetry readings via both gRPC streaming and HTTP/JSON endpoints.
- **FR-002**: System MUST validate all incoming telemetry readings to ensure presence of `device_id`, valid numeric `value`, non-empty `metric_name`, and timestamp within acceptable temporal bounds.
- **FR-003**: System MUST route every accepted telemetry record to the message log topic `telemetry.raw` partitioned strictly by `device_id`.
- **FR-004**: System MUST serialize messages using a standardized binary schema (Protocol Buffers) containing device identifier, timestamp, metric name, numeric reading, engineering units, and arbitrary string key-value metadata.
- **FR-005**: System MUST provision a local single-node messaging broker environment with pre-configured topic partitions supporting scalable per-device distribution.
- **FR-006**: System MUST expose an HTTP health check endpoint (`/healthz`) that checks broker connectivity and readiness.
- **FR-007**: System MUST expose standard Prometheus metrics reporting total messages ingested, ingestion rate, payload byte count, rejection rate, and publish latencies.
- **FR-008**: System MUST reject future-dated timestamps exceeding a configurable tolerance bound (default: 5 minutes) to protect downstream watermarking logic.

### Key Entities *(include if feature involves data)*

- **TelemetryReading**: A single sensor measurement containing:
  - `device_id` (string): Unique identifier of the emitting machine or sensor.
  - `timestamp` (int64 nanoseconds or ISO-8601): Event-time when the physical measurement occurred.
  - `metric_name` (string): Physical quantity measured (e.g., `bearing_temp`, `vibration_rms`, `motor_current`).
  - `value` (double): The numeric reading.
  - `unit` (string): Measurement unit (e.g., `celsius`, `mm/s`, `amperes`).
  - `metadata` (map<string, string>): Optional edge context (firmware version, sensor serial, location tag).
- **DeviceBatch**: A collection of `TelemetryReading` objects transmitted together from an edge gateway.
- **IngestionReceipt**: Acknowledgement payload returned to the caller indicating status, ingested count, and server reception timestamp.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Ingestion gateway sustains an ingestion rate of at least 50,000 events per second under concurrent load testing.
- **SC-002**: Ingestion acknowledgement response latency p99 is less than 10 milliseconds under normal operating conditions.
- **SC-003**: 100% of telemetry readings originating from a given device are dispatched to the broker using that device's identifier as the partition key.
- **SC-004**: 100% of malformed or out-of-spec telemetry requests are rejected with clear error codes within 5 milliseconds without crashing the gateway.
- **SC-005**: Service health probe accurately reflects broker outage within 2 seconds of broker unavailability.

## Assumptions

- Devices and edge gateways have basic retry logic with backoff when receiving HTTP 503 or gRPC UNAVAILABLE.
- Single-node Redpanda broker container is sufficient for local development and integration testing; production scaling will use a multi-node cluster.
- Ingestion Gateway is stateless and can be horizontally scaled behind a load balancer in future iterations.
- Device authentication and mTLS termination are assumed to occur at an ingress/proxy layer; the core gateway enforces data schema validation and partition routing.
