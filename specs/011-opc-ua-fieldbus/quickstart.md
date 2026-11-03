# Quickstart & Verification Guide: Milestone 011 — Native OPC-UA Binary Fieldbus Adapter

**Branch**: `011-opc-ua-fieldbus`  
**Date**: 2026-11-03  
**Spec**: [spec.md](spec.md)

---

## 1. Prerequisites

- Go 1.26+ installed locally.
- Working directory set to repository root (`/home/mayukh/projects/pulse`).
- Local build tooling: `make build` compiles `bin/bridge` and `bin/gateway`.

---

## 2. Configuration Setup

Create a test configuration file at `/tmp/pulse-bridge-opcua-test.yaml`:

```yaml
bridge:
  gateway_endpoint: "127.0.0.1:50051"
  spool_dir: "/tmp/pulse-opcua-spool"
  flush_batch_size: 100
  flush_interval: "25ms"

adapters:
  opcua:
    - name: "opcua-sim-plc"
      endpoint: "opc.tcp://127.0.0.1:4840"
      security_policy: "None"
      security_mode: "None"
      auth_mode: "anonymous"
      publish_interval: "100ms"
      keep_alive_interval: "2s"
      connect_timeout: "3s"
      nodes:
        - node_id: "ns=2;s=Turbine.RPM"
          device_id: "turbine-01"
          metric_name: "rotor_speed_rpm"
          sampling_interval: "50ms"
        - node_id: "ns=2;s=Turbine.Vibration"
          device_id: "turbine-01"
          metric_name: "vibration_g"
          deadband_type: "Absolute"
          deadband_value: 0.05
      discovery:
        enabled: true
        root_node_id: "ns=0;i=85"
        path_filter: "Objects/Turbine/*"
        max_depth: 3
        auto_subscribe: true
```

---

## 3. End-to-End Verification Scenarios

### Scenario 1: Subscription Handshake & Telemetry Streaming
1. **Launch Ingestion Perimeter**:
   ```bash
   bin/gateway --config configs/gateway.yaml &
   ```
2. **Launch Bridge with OPC-UA Adapter**:
   ```bash
   bin/bridge --config /tmp/pulse-bridge-opcua-test.yaml
   ```
3. **Expected Outcome**:
   - Bridge output logs:
     ```text
     [opcua-sim-plc] Connected to opc.tcp://127.0.0.1:4840
     [opcua-sim-plc] Session activated (SessionId: ns=1;g=...)
     [opcua-sim-plc] Created subscription (Id: 1, Interval: 100ms)
     [opcua-sim-plc] Registered 2 monitored items
     ```
   - Telemetry readings arrive at `pulse-gateway` with accurate values and nanosecond timestamps.

---

### Scenario 2: Dynamic Address Space Discovery
1. **Verify Discovered Nodes**:
   - Query the bridge administrative API or inspect metrics:
     ```bash
     curl -s http://127.0.0.1:9095/metrics | grep pulse_bridge_opcua_monitored_items_total
     ```
2. **Expected Outcome**:
   - Monitored items count reflects both static nodes and dynamically discovered nodes matching `Objects/Turbine/*`.

---

### Scenario 3: Gateway Disconnect & Store-and-Forward Spooling
1. **Sever Gateway Connection**: Stop `pulse-gateway` (`kill -STOP <gateway-pid>`).
2. **Observe Spooling**:
   - OPC-UA data changes continue arriving at the bridge.
   - Inspect spool directory:
     ```bash
     ls -la /tmp/pulse-opcua-spool/
     ```
   - Segment files with `.log` extensions containing `PUBR` binary framing are written to disk.
3. **Restore Gateway**: Resume `pulse-gateway` (`kill -CONT <gateway-pid>`).
4. **Expected Outcome**:
   - Bridge automatically reconnects to gateway over gRPC.
   - Spooled records flush in strict chronological timestamp order.
   - Spool directory empties (`BufferedCount == 0`). Zero readings lost.

---

### Scenario 4: Automated PLC Reconnection
1. **Simulate PLC Restart**: Terminate the OPC-UA server process.
2. **Observe Bridge Reaction**:
   - Keep-alive heartbeat timeout triggers within 2 seconds.
   - Bridge logs:
     ```text
     [opcua-sim-plc] Keep-alive timeout, connection lost. Entering reconnect loop...
     ```
3. **Restart OPC-UA Server**: Bring the server back online.
4. **Expected Outcome**:
   - Bridge connects, activates a new session, reconstitutes subscriptions, and resumes streaming within $<5$ seconds without human intervention.

