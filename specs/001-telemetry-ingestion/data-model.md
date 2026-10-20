# Phase 1 Data Model: Telemetry Ingestion and Ingestion Gateway

**Feature**: `001-telemetry-ingestion`
**Date**: 2026-10-20
**Status**: Complete

---

## 1. Domain Entities & Schemas

### Entity: `TelemetryReading`
Represents an individual physical sensor measurement emitted by an edge device or sensor node.

| Field Name | Type | Required | Constraints | Description |
| :--- | :--- | :---: | :--- | :--- |
| `device_id` | `string` | **Yes** | 1 to 128 characters; regex `^[a-zA-Z0-9_\-\.:]+$` | Unique identifier of the industrial device or sensor node. |
| `timestamp_ns`| `int64` | **Yes** | Unix epoch in nanoseconds; $\le \text{now} + 300\text{s}$, $\ge \text{now} - 86400\text{s}$ | Event-time when the physical sensor reading occurred. |
| `metric_name` | `string` | **Yes** | 1 to 128 characters; regex `^[a-zA-Z0-9_\-\.]+$` | Name of the telemetry metric (e.g. `bearing_temp`, `vibration_rms`). |
| `value` | `double` | **Yes** | IEEE 754 finite double; must not be `NaN`, `+Inf`, or `-Inf` | Numeric sensor reading. |
| `unit` | `string` | No | $\le 32$ characters | Engineering unit of measurement (e.g. `celsius`, `mm_s`, `rpm`). |
| `metadata` | `map<string, string>` | No | Max 16 key-value pairs; key $\le 64$ chars, value $\le 256$ chars | Arbitrary edge tags (e.g. `firmware_version: v2.1.0`, `line: 4`). |

---

### Entity: `TelemetryBatch`
Represents a collection of telemetry readings transmitted together by an edge gateway or offline device recovering connectivity.

| Field Name | Type | Required | Constraints | Description |
| :--- | :--- | :---: | :--- | :--- |
| `readings` | `repeated TelemetryReading` | **Yes** | Minimum 1 item, maximum 1,000 items per batch | Array of individual sensor measurements. |

---

### Entity: `IngestionReceipt`
Represents the synchronous server response acknowledging or rejecting an ingestion request.

| Field Name | Type | Required | Constraints | Description |
| :--- | :--- | :---: | :--- | :--- |
| `status` | `string` | **Yes** | Enum: `ACCEPTED`, `PARTIAL_SUCCESS`, `REJECTED` | Status of the ingestion transaction. |
| `processed_count` | `int32` | **Yes** | $\ge 0$ | Total number of readings successfully validated and dispatched. |
| `rejected_count` | `int32` | **Yes** | $\ge 0$ | Total number of readings rejected due to validation failures. |
| `server_time_ns` | `int64` | **Yes** | Unix epoch nanoseconds | Server timestamp when the receipt was issued. |
| `error_details` | `repeated ValidationError` | No | Populated if `rejected_count > 0` | Array of specific validation errors with field references. |

---

### Entity: `BrokerRecord` (Wire Format in Redpanda)
Represents the serialized message written to the `telemetry.raw` topic.

- **Topic**: `telemetry.raw`
- **Partition Key**: `device_id` (UTF-8 encoded string)
- **Value**: Binary Protobuf serialization of `TelemetryReading`
- **Timestamp**: Event time in milliseconds (`timestamp_ns / 1_000_000`)
- **Headers**:
  - `x-ingest-source`: `grpc` or `http`
  - `x-ingest-server-time-ns`: Server receipt timestamp

---

## 2. Validation Rules & Guardrails

1. **Device ID Integrity**:
   - `len(device_id) > 0 && len(device_id) <= 128`.
   - Cannot contain control characters, newlines, or whitespace.
2. **Clock Skew Bound**:
   - Future skew check: `reading.timestamp_ns <= (now_ns + 5 * 60 * 1e9)` (5 minutes max clock drift).
   - Past bound check: `reading.timestamp_ns >= (now_ns - 24 * 3600 * 1e9)` (24 hours max historical buffer).
   - Violations reject the reading immediately with error code `ERR_CLOCK_SKEW_OUT_OF_BOUNDS`.
3. **Finite Numeric Value**:
   - `!math.IsNaN(value) && !math.IsInf(value, 0)`.
   - Violations reject the reading immediately with error code `ERR_INVALID_NUMERIC_VALUE`.
4. **Batch Size Enforcement**:
   - HTTP payloads or gRPC batch calls with $> 1,000$ readings are rejected with error code `ERR_BATCH_TOO_LARGE`.
