# Quickstart: Milestone 010 — Industrial Edge Bridge & CEP Engine

**Feature**: `010-edge-bridge-cep`  
**Date**: 2026-10-30  

---

## 1. Quick Verification Overview

This milestone introduces two standalone subsystems:
1. **`pulse-bridge`**: Industrial edge daemon connecting MQTT brokers and Modbus PLCs to `pulse-gateway` with disk-backed offline store-and-forward buffering.
2. **`pulse-processor` CEP Engine**: In-memory, zero-lock pattern matching engine evaluating temporal sequences (`FOLLOWED_BY`, `WITHIN`) and cross-metric formulas.

---

## 2. Configuration Files

### Sample Edge Bridge Config (`deployments/config/bridge.yaml`)
```yaml
bridge_id: "bridge-factory-01"
gateway_endpoint: "localhost:50051"
spool_dir: "/tmp/pulse-bridge-spool"
max_spool_bytes: 524288000 # 500MB
batch_linger: 10ms
batch_max_records: 500

mqtt:
  - broker_url: "tcp://localhost:1883"
    client_id: "pulse-bridge-plant1"
    topics:
      - "factory/+/+/telemetry"
    format: "json"

modbus:
  - endpoint: "localhost:5020"
    slave_id: 1
    poll_interval: 200ms
    registers:
      - device_id: "motor-01"
        metric_name: "oil_pressure"
        unit: "psi"
        function_code: 3
        start_address: 40001
        data_type: "float32_be"
        multiplier: 1.0
```

### Sample CEP Rule Definition (`deployments/config/cep_rules.yaml`)
```yaml
rules:
  - rule_id: "RULE-BEARING-FAILURE-CASCADE"
    name: "Bearing Mechanical & Thermal Cascade"
    device_pattern: "turbine-*"
    window_duration: 30s
    pattern_type: "SEQUENCE"
    severity: "CRITICAL"
    sequence_steps:
      - step_index: 0
        metric_name: "vibration"
        condition: "z_score >= 3.5"
      - step_index: 1
        metric_name: "bearing_temp"
        condition: "z_score >= 3.0"

  - rule_id: "RULE-THERMAL-EFFICIENCY-DROP"
    name: "Turbine Thermal Efficiency Degradation"
    device_pattern: "turbine-*"
    window_duration: 10s
    pattern_type: "EXPRESSION"
    expression: "power_mw / fuel_flow < 0.70"
    severity: "WARNING"
```

---

## 3. Running Edge Bridge & Simulating Offline Buffering

```bash
# 1. Build binaries
make build
go build -o bin/pulse-bridge ./cmd/bridge

# 2. Start Gateway and Redpanda
make up
./bin/gateway &

# 3. Start pulse-bridge
./bin/pulse-bridge --config deployments/config/bridge.yaml

# 4. Simulate Network Severing
# Kill or stop Gateway (:50051)
killall gateway

# 5. Observe Offline Buffering
# Edge bridge detects disconnect, logs warning, and spools incoming readings to disk
ls -lh /tmp/pulse-bridge-spool/

# 6. Restore Gateway
./bin/gateway &

# 7. Observe Backfill
# Bridge flushes queued spool segments in timestamp sequence before resuming live stream
```

---

## 4. Testing Complex Event Processing (CEP)

```bash
# Run unit and integration tests for CEP pattern matching and expression evaluation
go test -v -race ./internal/processor/cep/...

# Run Edge Bridge protocol adapter and offline spooler tests
go test -v -race ./internal/bridge/...

# Run full multi-service test suite
make test
```

---

## 5. Live Rule Reloading

```bash
# Stream Processor exposes an administrative reload endpoint
curl -X POST http://localhost:8081/api/v1/rules/reload

# Or via SIGHUP
kill -HUP $(pidof processor)
```

