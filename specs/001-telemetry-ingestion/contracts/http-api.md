# HTTP REST API Contract: Telemetry Ingestion Gateway

**Feature**: `001-telemetry-ingestion`
**Date**: 2026-10-20
**Base Port**: `8080`

---

## 1. Ingestion Endpoints

### `POST /api/v1/telemetry`
Ingests a single sensor telemetry reading.

#### Request Headers
- `Content-Type`: `application/json`

#### Request Body
```json
{
  "device_id": "sensor-turbine-01",
  "timestamp_ns": 1729437600000000000,
  "metric_name": "bearing_temp",
  "value": 78.4,
  "unit": "celsius",
  "metadata": {
    "site": "plant-north",
    "firmware": "v1.4.2"
  }
}
```

#### Responses
- **`202 Accepted`**: Reading validated and queued to Redpanda.
  ```json
  {
    "status": "ACCEPTED",
    "processed_count": 1,
    "rejected_count": 0,
    "server_time_ns": 1729437600005120000
  }
  ```
- **`400 Bad Request`**: Malformed payload or validation violation.
  ```json
  {
    "status": "REJECTED",
    "processed_count": 0,
    "rejected_count": 1,
    "server_time_ns": 1729437600002100000,
    "error_details": [
      {
        "field": "timestamp_ns",
        "error_code": "ERR_CLOCK_SKEW_OUT_OF_BOUNDS",
        "message": "Timestamp is more than 5 minutes in the future"
      }
    ]
  }
  ```
- **`503 Service Unavailable`**: Broker disconnected or backpressured.

---

### `POST /api/v1/telemetry/batch`
Ingests a batch of up to 1,000 telemetry readings.

#### Request Body
```json
{
  "readings": [
    {
      "device_id": "sensor-turbine-01",
      "timestamp_ns": 1729437600000000000,
      "metric_name": "bearing_temp",
      "value": 78.4,
      "unit": "celsius"
    },
    {
      "device_id": "sensor-turbine-01",
      "timestamp_ns": 1729437601000000000,
      "metric_name": "bearing_temp",
      "value": 79.1,
      "unit": "celsius"
    }
  ]
}
```

#### Responses
- **`202 Accepted`**: All readings accepted.
- **`207 Multi-Status / Partial Success`**: Some readings accepted, others rejected.
- **`413 Payload Too Large`**: Batch size exceeds 1,000 readings.

---

## 2. Observability & Health Endpoints

### `GET /healthz`
Readiness and liveness probe for orchestrators.

#### Responses
- **`200 OK`**: Gateway is running and healthy; active connection to Redpanda broker.
  ```json
  {
    "status": "UP",
    "broker_connected": true,
    "topic": "telemetry.raw",
    "partitions": 12,
    "uptime_seconds": 3600
  }
  ```
- **`503 Service Unavailable`**: Broker connection failed or topic missing.
  ```json
  {
    "status": "DOWN",
    "broker_connected": false,
    "error": "dial tcp 127.0.0.1:9092: connection refused"
  }
  ```

---

### `GET /metrics`
Standard Prometheus metrics scraping endpoint.

#### Exposed Metrics
- `pulse_ingest_records_total{protocol="http|grpc", status="success|rejected"}`
- `pulse_ingest_latency_seconds_bucket{protocol="http|grpc", le="..."}`
- `pulse_ingest_bytes_total{protocol="http|grpc"}`
- `pulse_ingest_clock_skew_rejections_total`
- `pulse_broker_publish_latency_seconds_bucket{le="..."}`
- `pulse_broker_connected{topic="telemetry.raw"}`
