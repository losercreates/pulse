# Quickstart Validation Guide: ChronoDB Storage Sink & Query Service

**Feature**: `004-chronodb-storage-query`  
**Date**: 2026-10-23  
**Status**: Ready for Implementation  

---

## 1. Prerequisites

- **Redpanda Cluster**: Running with `telemetry.raw`, `telemetry.aggregates`, `telemetry.alerts` (12 partitions each).
- **Go 1.23+**: Installed locally.
- **Protobuf Generation**: `telemetry.proto`, `aggregates.proto`, `alerts.proto`, and `chronodb.proto` compiled in `proto/v1/`.

---

## 2. Infrastructure Setup & Build

```bash
# 1. Start Redpanda
make up

# 2. Compile Protobufs & Build Binaries
make proto
make build

# 3. Start Storage Service
./bin/chronod
```

Verify that the storage query service is healthy on port `:8084`:
```bash
curl -s http://localhost:8084/healthz | jq
```
*Expected Output:*
```json
{
  "status": "UP",
  "system": "pulse-chronodb-storage"
}
```

---

## 3. End-to-End Validation Scenarios

### Scenario A: Ingestion of Aggregates to ChronoDB
1. Ingest 1,000 telemetry readings into the gateway (`/api/v1/telemetry/batch`).
2. Verify stream processor closes windows and emits aggregates to `telemetry.aggregates`.
3. Check ChronoDB storage write metrics:
```bash
curl -s http://localhost:8084/metrics | grep pulse_storage_writes_total
```
*Expected Result: Increments by number of emitted aggregates.*

---

### Scenario B: Idempotency Under Offset Replay (Constitution Principle III)
1. Stop the `chronod` storage daemon.
2. Reset consumer group offset for `telemetry.aggregates` back by 100 offsets:
```bash
docker compose -f deployments/docker-compose.yml exec redpanda \
  rpk group seek pulse-chronodb-consumer --to-offset 0 -t telemetry.aggregates
```
3. Restart `./bin/chronod`.
4. Query the stored aggregate count and compare with initial count:
```bash
curl -s "http://localhost:8084/api/v1/query?device_id=turbine-01&metric_name=bearing_temp&start_time=0&end_time=2000000000000000000" | jq .total_records
```
*Expected Result: Total record count is unchanged; zero duplicate rows, zero doubled sums.*

---

### Scenario C: Historical Time-Range Query & Rollup Downsampling
1. Execute a raw window aggregate query:
```bash
curl -s "http://localhost:8084/api/v1/query?device_id=turbine-01&metric_name=bearing_temp&start_time=1700000000000000000&end_time=1700000060000000000" | jq
```
2. Execute a downsampled 1-minute rollup query:
```bash
curl -s "http://localhost:8084/api/v1/query/rollup?device_id=turbine-01&metric_name=bearing_temp&start_time=1700000000000000000&end_time=1700000060000000000&step=1m" | jq
```
*Expected Result: Sub-windows are grouped into single 1-minute bucket with exact combined count and weighted mean.*

---

### Scenario D: Device Discovery Catalog
```bash
curl -s http://localhost:8084/api/v1/devices | jq
```
*Expected Result: Returns list of distinct devices and their recorded metric names.*

---

## 4. Automated Tests & Invariants

```bash
# Run unit tests for storage engine, sharding, and rollup calculation
go test -v ./internal/storage/...

# Run idempotency chaos integration test
go test -v ./tests/integration/idempotent_storage_test.go

# Run storage ingestion and query performance benchmark
go test -bench=. -benchmem ./tests/bench/storage_bench_test.go
```
