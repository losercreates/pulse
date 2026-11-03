# Phase 1 Data Model: Milestone 011 — Native OPC-UA Binary Fieldbus Adapter

**Branch**: `011-opc-ua-fieldbus`  
**Date**: 2026-11-03  
**Spec**: [spec.md](spec.md)

---

## 1. Entity Definitions

### 1.1 `OPCUAAdapterConfig`
Top-level configuration structure defining connection, security, authentication, and subscription parameters for an OPC-UA fieldbus endpoint.

```yaml
name: "opcua-turbine-line1"
endpoint: "opc.tcp://192.168.10.45:4840"
security_policy: "Basic256Sha256"      # None | Basic256Sha256
security_mode: "SignAndEncrypt"        # None | Sign | SignAndEncrypt
auth_mode: "username"                  # anonymous | username | certificate
username: "opc_operator"
password: "env:OPCUA_PASSWORD"
cert_file: "/etc/pulse/certs/client.crt"
key_file: "/etc/pulse/certs/client.key"
publish_interval: "100ms"
keep_alive_interval: "5s"
connect_timeout: "5s"
request_timeout: "3s"
nodes: [...]
discovery: {...}
```

| Field | Type | Validation Rules | Description |
|---|---|---|---|
| `Name` | `string` | Required, unique, alphanumeric with hyphens | Unique identifier for adapter instance. |
| `Endpoint` | `string` | Required, format `opc.tcp://<host>:<port>` | Remote OPC-UA server address. |
| `SecurityPolicy` | `string` | Enum: `None`, `Basic256Sha256` | Cryptographic security policy for secure channel. |
| `SecurityMode` | `string` | Enum: `None`, `Sign`, `SignAndEncrypt` | Message protection mode. |
| `AuthMode` | `string` | Enum: `anonymous`, `username`, `certificate` | Session user identity token type. |
| `PublishInterval`| `Duration` | $> 10\text{ms}$, default `100ms` | Requested server subscription publishing interval. |
| `KeepAliveInterval`| `Duration` | $> 500\text{ms}$, default `5s` | Max interval before server sends empty keep-alive. |
| `ConnectTimeout` | `Duration` | $> 500\text{ms}$, default `5s` | TCP dial and secure channel handshake timeout. |
| `RequestTimeout` | `Duration` | $> 500\text{ms}$, default `3s` | RPC timeout for synchronous services (Browse, etc). |

---

### 1.2 `MonitoredItemConfig`
Defines an individual variable node monitored for data change notifications.

| Field | Type | Validation Rules | Description |
|---|---|---|---|
| `NodeID` | `string` | Required, e.g. `ns=2;s=Turbine.RPM` or `ns=2;i=1001` | OPC-UA Node identifier. |
| `DeviceID` | `string` | Required, non-empty identifier | Mapped Pulse target device identifier. |
| `MetricName` | `string` | Required, non-empty identifier | Mapped Pulse metric name (e.g. `rotor_speed_rpm`). |
| `SamplingInterval`| `Duration` | $\ge 0\text{ms}$ (0 = fastest/server rate) | Server sampling interval for this node. |
| `QueueSize` | `uint32` | $\ge 1$, default `10` | Server-side notification queue depth. |
| `DiscardOldest` | `bool` | Default `true` | Queue overflow policy on the OPC-UA server. |
| `DeadbandType` | `string` | Enum: `None`, `Absolute`, `Percent` | Data change deadband filter mode. |
| `DeadbandValue`| `float64` | $\ge 0.0$ | Magnitude of change required to emit notification. |

---

### 1.3 `DiscoveryConfig`
Defines rules for dynamic address space traversal and variable auto-subscription.

| Field | Type | Validation Rules | Description |
|---|---|---|---|
| `Enabled` | `bool` | Default `false` | Enables address space browsing on startup. |
| `RootNodeID` | `string` | Default `ns=0;i=85` (Objects Folder) | Starting NodeID for hierarchical traversal. |
| `PathFilter` | `string` | Glob pattern, e.g. `Objects/Turbines/*/*` | Path expression for variable inclusion. |
| `MaxDepth` | `int` | Range `1..25`, default `10` | Maximum tree traversal depth to prevent runaway crawls. |
| `AutoSubscribe` | `bool` | Default `true` | Automatically register discovered nodes as monitored items. |
| `DefaultDeviceID`| `string` | Optional fallback template | Device ID template for discovered nodes (e.g. `{parent}`). |

---

### 1.4 `OPCUASessionState`
Tracks runtime operational state and protocol handles inside the adapter.

```text
               ┌────────────────┐
               │  DISCONNECTED  │ ◄──────────────────────────────┐
               └───────┬────────┘                                │
                       │ Net.Dial + HEL/ACK + OPN                │ Connection dropped /
                       ▼                                         │ Keep-alive expired
               ┌────────────────┐                                │
               │   CONNECTING   │ ───────────────────────────────┤
               └───────┬────────┘                                │
                       │ CreateSession + ActivateSession         │
                       ▼                                         │
               ┌────────────────┐                                │
               │   AUTHENTICATED│ ───────────────────────────────┤
               └───────┬────────┘                                │
                       │ CreateSubscription + MonitoredItems     │
                       ▼                                         │
               ┌────────────────┐                                │
               │   SUBSCRIBED   │ ───────────────────────────────┘
               └────────────────┘
```

| Attribute | Type | Description |
|---|---|---|
| `State` | `SessionState` | Current state enum: `Disconnected`, `Connecting`, `Authenticated`, `Subscribed`. |
| `ChannelID` | `uint32` | Negotiated OPC-UA SecureChannel identifier. |
| `TokenID` | `uint32` | Current security token identifier. |
| `SessionID` | `NodeID` | Unique server-assigned session identifier. |
| `AuthToken` | `NodeID` | Authentication token supplied in request headers. |
| `SubscriptionID`| `uint32` | Server-assigned subscription identifier. |
| `MonitoredItems` | `map[uint32]MonitoredItem` | Active items indexed by client handle. |
| `LastHeartbeat` | `time.Time` | Timestamp of last received Publish or keep-alive response. |

---

### 1.5 `DataValueReading`
Decoded intermediate representation of an OPC-UA `DataChangeNotification`.

| Field | Type | Description |
|---|---|---|
| `ClientHandle` | `uint32` | Handle identifying the corresponding `MonitoredItemConfig`. |
| `Value` | `float64` | Normalized IEEE 754 floating-point numeric value. |
| `RawVariant` | `any` | Original parsed variant value (bool, integer, float, string). |
| `StatusCode` | `uint32` | OPC-UA 32-bit status code (0 = Good, 0x40000000 = Uncertain, 0x80000000 = Bad). |
| `SourceTimestamp`| `int64` | Source timestamp in nanoseconds since Unix epoch (0 if not supplied by PLC). |
| `ServerTimestamp`| `int64` | Server transmission timestamp in nanoseconds since Unix epoch. |

---

## 2. Mapping to Canonical `TelemetryReading`

When a `DataValueReading` is processed, the adapter converts it into the system-wide canonical Protobuf `TelemetryReading` (`proto/v1/telemetry.proto`):

```protobuf
message TelemetryReading {
  string device_id = 1;        // From MonitoredItemConfig.DeviceID
  string metric_name = 2;      // From MonitoredItemConfig.MetricName
  double value = 3;            // Normalized float64 from DataValueReading.Value
  int64 timestamp_ns = 4;      // DataValueReading.SourceTimestamp (or ServerTimestamp fallback)
  map<string, string> metadata = 5; // { "protocol": "opcua", "node_id": "...", "status_code": "0x00000000" }
}
```

