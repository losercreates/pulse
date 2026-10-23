# HTTP Query API Specification: ChronoDB Historical Telemetry Service

**Feature**: `004-chronodb-storage-query`  
**Base URL**: `http://localhost:8084`  

---

## 1. Endpoints Overview

| Method | Endpoint | Description | Status Codes |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/v1/query` | Time-range query for raw window aggregates | `200`, `400`, `500` |
| `GET` | `/api/v1/query/rollup` | Dynamic downsampled multi-bucket rollup | `200`, `400`, `500` |
| `GET` | `/api/v1/devices` | Active device and metric discovery catalog | `200`, `500` |
| `GET` | `/healthz` | Storage service liveness probe | `200` |
| `GET` | `/readyz` | Storage service readiness probe (checks broker & DB) | `200`, `503` |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint | `200` |

---

## 2. API Definitions

### 2.1 Time-Range Query (`GET /api/v1/query`)

Retrieves historical stored window aggregates for a specific device and metric.

#### Query Parameters
- `device_id` (string, required): Unique machine or sensor identifier (1–128 chars).
- `metric_name` (string, required): Physical measurement name (e.g. `bearing_temp`).
- `start_time` (int64, required): Start boundary in nanoseconds (inclusive).
- `end_time` (int64, required): End boundary in nanoseconds (exclusive).
- `limit` (int, optional): Maximum records to return (default: `1000`, max: `10000`).

#### Success Response (`200 OK`)
```json
{
  "device_id": "turbine-01",
  "metric_name": "bearing_temp",
  "total_records": 2,
  "records": [
    {
      "window_start_ns": 1700000000000000000,
      "window_end_ns": 1700000010000000000,
      "count": 500,
      "sum": 36400.0,
      "min": 71.2,
      "max": 74.8,
      "mean": 72.8
    },
    {
      "window_start_ns": 1700000010000000000,
      "window_end_ns": 1700000020000000000,
      "count": 500,
      "sum": 36550.0,
      "min": 71.5,
      "max": 75.1,
      "mean": 73.1
    }
  ]
}
```

---

### 2.2 Analytical Downsampling Rollup (`GET /api/v1/query/rollup`)

Re-aggregates stored 10-second window aggregates into coarser time buckets (e.g. 1m, 5m, 1h).

#### Query Parameters
- `device_id` (string, required): Unique device identifier.
- `metric_name` (string, required): Measurement name.
- `start_time` (int64, required): Query start time in nanoseconds.
- `end_time` (int64, required): Query end time in nanoseconds.
- `step` (string, required): Duration string for bucket size (e.g., `1m`, `5m`, `1h`, or integer nanoseconds).

#### Success Response (`200 OK`)
```json
{
  "device_id": "turbine-01",
  "metric_name": "bearing_temp",
  "step_ns": 300000000000,
  "total_buckets": 1,
  "buckets": [
    {
      "bucket_start_ns": 1700000000000000000,
      "bucket_end_ns": 1700000300000000000,
      "count": 15000,
      "sum": 1093500.0,
      "min": 70.1,
      "max": 76.5,
      "mean": 72.9
    }
  ]
}
```

---

### 2.3 Device & Metric Catalog (`GET /api/v1/devices`)

Returns all known devices, active metrics, and temporal bounds.

#### Success Response (`200 OK`)
```json
{
  "total_devices": 1,
  "devices": [
    {
      "device_id": "turbine-01",
      "metrics": ["bearing_temp", "vibration", "oil_pressure"],
      "first_seen_ns": 1700000000000000000,
      "last_seen_ns": 1700086400000000000
    }
  ]
}
```
