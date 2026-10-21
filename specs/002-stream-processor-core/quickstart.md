# Quickstart Validation Guide: Stream Processing Engine & Watermarks

**Feature**: `002-stream-processor-core`
**Date**: 2026-10-21
**Status**: Ready for Implementation

---

## 1. Prerequisites

- **Docker Compose**: Redpanda cluster with topics `telemetry.raw`, `telemetry.aggregates`, and `telemetry.late`.
- **Go 1.23+**: Installed locally.
- **Compiled Protobufs**: `telemetry.proto` and `aggregates.proto` compiled in `proto/v1/`.

---

## 2. Infrastructure Setup: Start Broker & Topics

Start Redpanda:
```bash
make up
```

Verify that topics `telemetry.raw`, `telemetry.aggregates`, and `telemetry.late` exist:
```bash
docker compose -f deployments/docker-compose.yml exec redpanda rpk topic list
```

---

## 3. Run the Stream Processor

Start the processor binary:
```bash
go run cmd/processor/main.go
```
*Outputs: Joined consumer group `pulse-stream-processors`, assigned partitions `0..11`, listening on `telemetry.raw`, metrics on `:8081`.*

---

## 4. End-to-End Verification Scenarios

### Scenario A: Out-of-Order Event Handling & Window Closure
Publish a series of out-of-order events within the 5-second allowed lateness bound:
1. Event 1: Device `turbine-01`, Time: `T+2s`, Value: `50.0`
2. Event 2: Device `turbine-01`, Time: `T+8s`, Value: `60.0` (Watermark advances to `T+3s`)
3. Event 3: Device `turbine-01`, Time: `T+4s`, Value: `55.0` (Arrives *after* T+8s, but timestamp $T+4s > \text{watermark}$, so accepted into `[T, T+10s)`)
4. Event 4: Device `turbine-01`, Time: `T+15s`, Value: `70.0` (Watermark advances to $T+10s$, triggering window `[T, T+10s)` close!)

Verify the emitted aggregate:
```bash
docker compose -f deployments/docker-compose.yml exec redpanda \
  rpk topic consume telemetry.aggregates -n 1 --format '%k | %v\n'
```
*Expected Output: Window `[T, T+10s)` emitted with count = 3, min = 50.0, max = 60.0, mean = 55.0.*

---

### Scenario B: Late Data Dead-Letter Routing
Publish a record with an event timestamp far behind the current watermark (e.g. 1 hour in the past):
```bash
curl -X POST http://localhost:8080/api/v1/telemetry \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "turbine-01",
    "timestamp_ns": '$(($(date +%s%N) - 3600000000000))',
    "metric_name": "bearing_temp",
    "value": 99.9
  }'
```

Verify the record is routed to `telemetry.late` rather than modifying the closed window:
```bash
docker compose -f deployments/docker-compose.yml exec redpanda \
  rpk topic consume telemetry.late -n 1
```

---

### Scenario C: Streaming Observability Metrics
Inspect stream processor internal metrics:
```bash
curl -s http://localhost:8081/metrics | grep pulse_stream
```
*Expected Metrics:*
- `pulse_stream_watermark_lag_seconds`
- `pulse_stream_open_windows_count`
- `pulse_stream_windows_closed_total`
- `pulse_stream_late_events_total`

---

## 5. Automated Tests & Property Invariance

```bash
# Run unit and watermark simulation tests
go test -v ./internal/processor/watermark/...
go test -v ./internal/processor/window/...

# Run property-based arrival order invariance test
go test -v ./tests/property/...

# Run stream processor benchmark
go test -bench=BenchmarkWindowAggregation -benchtime=5s ./tests/bench/...
```
