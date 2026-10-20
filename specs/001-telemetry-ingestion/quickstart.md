# Quickstart Validation Guide: Telemetry Ingestion Gateway

**Feature**: `001-telemetry-ingestion`
**Date**: 2026-10-20
**Status**: Ready for Implementation

---

## 1. Prerequisites

- **Docker & Docker Compose**: For running the local Redpanda container.
- **Go 1.23+**: For compiling and executing the Ingestion Gateway.
- **Protocol Buffer Compiler**: `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc` (or `buf`).
- **cURL & grpcurl** (optional): For testing endpoints from the CLI.

---

## 2. Infrastructure Setup: Start Redpanda

Start the local single-node Redpanda broker defined in `deployments/docker-compose.yml`:

```bash
docker compose -f deployments/docker-compose.yml up -d redpanda
```

Verify that Redpanda is running and the `telemetry.raw` topic with 12 partitions exists:

```bash
docker compose -f deployments/docker-compose.yml exec redpanda rpk cluster info
docker compose -f deployments/docker-compose.yml exec redpanda rpk topic list
```

---

## 3. Generate Protobuf Bindings

Compile the Protobuf v3 definitions into Go code:

```bash
protoc --proto_path=specs/001-telemetry-ingestion/contracts \
  --go_out=proto/v1 --go_opt=paths=source_relative \
  --go-grpc_out=proto/v1 --go-grpc_opt=paths=source_relative \
  specs/001-telemetry-ingestion/contracts/telemetry.proto
```

---

## 4. Run the Ingestion Gateway

Start the service locally:

```bash
go run cmd/gateway/main.go
```
*Outputs: HTTP listening on `:8080`, gRPC listening on `:50051`, connected to Redpanda at `localhost:9092`.*

---

## 5. End-to-End Verification Scenarios

### Scenario A: Health & Readiness Check
Verify gateway connectivity to Redpanda:
```bash
curl -s http://localhost:8080/healthz | jq
```
*Expected Output: `{"status": "UP", "broker_connected": true, "topic": "telemetry.raw", "partitions": 12}`*

---

### Scenario B: HTTP Single Ingestion
Send a valid telemetry measurement:
```bash
curl -X POST http://localhost:8080/api/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "turbine-alpha-01",
    "timestamp_ns": '$(date +%s%N)',
    "metric_name": "bearing_temp",
    "value": 72.8,
    "unit": "celsius"
  }' | jq
```
*Expected Output: HTTP `202 Accepted` with `{"status": "ACCEPTED", "processed_count": 1}`*

---

### Scenario C: Clock-Skew Rejection
Send a payload with a future timestamp $> 5$ minutes:
```bash
curl -i -X POST http://localhost:8080/api/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "turbine-alpha-01",
    "timestamp_ns": '$(($(date +%s%N) + 600000000000))',
    "metric_name": "bearing_temp",
    "value": 72.8
  }'
```
*Expected Output: HTTP `400 Bad Request` with `ERR_CLOCK_SKEW_OUT_OF_BOUNDS`.*

---

### Scenario D: Inspect Published Records in Redpanda
Consume messages from the topic and verify the partition key is `turbine-alpha-01`:
```bash
docker compose -f deployments/docker-compose.yml exec redpanda \
  rpk topic consume telemetry.raw -n 1 --format '%k | %v | %p\n'
```
*Expected Output: Partition key is `turbine-alpha-01`, and the record is assigned to a deterministic partition `0..11`.*

---

### Scenario E: Prometheus Metrics
Check ingestion throughput metrics:
```bash
curl -s http://localhost:8080/metrics | grep pulse_ingest
```
*Expected Output: Incremented counters for `pulse_ingest_records_total` and latency histograms.*

---

## 6. Running Automated Tests

```bash
# Run unit validation tests
go test -v ./internal/gateway/validator/...

# Run Redpanda integration tests (requires docker compose running)
go test -v -tags=integration ./tests/integration/...

# Run concurrent ingestion benchmark
go test -bench=BenchmarkIngestionThroughput -benchtime=5s ./tests/bench/...
```
