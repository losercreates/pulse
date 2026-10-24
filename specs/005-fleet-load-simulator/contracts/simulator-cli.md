# Contract: Simulator Command-Line Interface (`bin/simulator`)

**Feature**: `005-fleet-load-simulator`  
**Date**: 2026-10-24  
**Status**: Completed  

---

## Command Synopsis

```bash
bin/simulator [options]
```

---

## Command-Line Options & Flags

| Flag | Shorthand | Type | Default | Description | Environment Variable |
| :--- | :---: | :---: | :---: | :--- | :--- |
| `--rate` | `-r` | `int` | `50000` | Target aggregate events per second | `SIM_TARGET_RATE` |
| `--devices` | `-d` | `int` | `1000` | Number of distinct virtual devices in fleet | `SIM_NUM_DEVICES` |
| `--duration` | `-t` | `duration`| `60s` | Test execution duration (e.g. `30s`, `5m`) | `SIM_DURATION` |
| `--transport` | | `string` | `grpc` | Transport protocol: `grpc` (streaming) or `http` (batch) | `SIM_TRANSPORT` |
| `--gateway-grpc` | | `string` | `localhost:50051` | Target Ingestion Gateway gRPC address | `GATEWAY_GRPC_ADDR` |
| `--gateway-http` | | `string` | `http://localhost:8080` | Target Ingestion Gateway HTTP endpoint | `GATEWAY_HTTP_ADDR` |
| `--chrono-query` | | `string` | `http://localhost:8084` | ChronoDB query endpoint for latency verification | `CHRONODB_QUERY_ADDR` |
| `--profile` | `-p` | `string` | `steady` | Traffic profile: `steady`, `burst`, `offline`, `anomaly`, `mixed` | `SIM_PROFILE` |
| `--burst-multiplier`| | `float` | `3.0` | Throughput multiplier during burst traffic | `SIM_BURST_MULTIPLIER` |
| `--burst-duration` | | `duration`| `15s` | Duration of each burst surge | `SIM_BURST_DURATION` |
| `--offline-pct` | | `float` | `0.05` | Fraction of fleet undergoing offline disconnect ($0.05 = 5\%$) | `SIM_OFFLINE_PCT` |
| `--offline-duration`| | `duration`| `30s` | Duration devices buffer locally before backfill | `SIM_OFFLINE_DURATION` |
| `--anomaly-pct` | | `float` | `0.02` | Percentage of readings with injected anomalies | `SIM_ANOMALY_PCT` |
| `--canary-interval`| | `duration`| `10s` | Interval between end-to-end latency canary probes | `SIM_CANARY_INTERVAL` |
| `--json` | | `bool` | `false` | Output machine-readable JSON summary report on stdout | `SIM_JSON` |
| `--workers` | `-w` | `int` | `16` | Number of concurrent sender worker goroutines | `SIM_WORKERS` |

---

## Exit Codes

| Code | Name | Condition |
| :---: | :--- | :--- |
| `0` | `SUCCESS` | Simulation completed and all SLA performance targets satisfied ($\ge 95\%$ target rate, p95 latency $< 2.0\text{s}$, error rate $< 0.1\%$). |
| `1` | `SLA_VIOLATION` | Simulation completed but one or more SLA performance gates were violated. |
| `2` | `GATEWAY_UNAVAILABLE` | Ingestion Gateway could not be reached after maximum connection retries. |
| `130`| `INTERRUPTED` | Process terminated via `SIGINT` (Ctrl+C). Outputs summary of completed duration. |

---

## Standard Output Examples

### Interactive Terminal Mode (Default)

```text
Pulse Fleet Load Simulator — Running
Target: 50,000 events/sec | Fleet: 1,000 devices | Transport: gRPC streaming
Profile: Steady-State | Elapsed: 00:00:25 / 00:01:00

Throughput:     51,240 events/sec  [=============================>         ]  102.5%
Total Sent:     1,281,000 readings
Failed / Dropped: 0 (0.000%)
Active Workers: 16 / 16 (100% capacity)
Canary Latency: p50: 1.12s | p90: 1.45s | p95: 1.62s  (SLA: < 2.0s [PASS])
```

### JSON Report Mode (`--json`)

```json
{
  "timestamp": "2026-10-24T05:46:00Z",
  "config": {
    "target_rate": 50000,
    "num_devices": 1000,
    "duration_seconds": 60,
    "transport": "grpc",
    "profile": "steady"
  },
  "results": {
    "total_sent": 3052100,
    "total_failed": 0,
    "error_rate_pct": 0.0,
    "achieved_rate": 50868.3,
    "peak_rate": 52400.0,
    "canary_probes_sent": 6,
    "canary_probes_resolved": 6,
    "canary_p50_latency_ms": 1120.5,
    "canary_p95_latency_ms": 1620.0
  },
  "sla_verdict": {
    "compliant": true,
    "throughput_sla_met": true,
    "latency_sla_met": true,
    "error_sla_met": true,
    "violations": []
  }
}
```
