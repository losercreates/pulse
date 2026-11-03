# Interface & Wire Contract: Milestone 011 — Native OPC-UA Binary Fieldbus Adapter

**Branch**: `011-opc-ua-fieldbus`  
**Date**: 2026-11-03  
**Spec**: [spec.md](spec.md)

---

## 1. ProtocolAdapter Go Interface Contract

The OPC-UA fieldbus adapter implements the unified `adapter.ProtocolAdapter` interface defined in `internal/bridge/adapter/adapter.go`:

```go
package opcua

import (
    "context"
    "github.com/losercreates/pulse/internal/bridge/adapter"
    telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

type Adapter struct {
    // Unexported connection state, configuration, and subscription handles
}

// NewAdapter constructs an OPC-UA adapter instance from validated configuration.
func NewAdapter(cfg Config) (*Adapter, error)

// Name returns the unique adapter identifier.
func (a *Adapter) Name() string

// Start initiates the TCP connection, activates secure session, registers subscriptions,
// and streams decoded TelemetryReadings into the provided channel.
func (a *Adapter) Start(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) error

// Stop terminates active subscriptions, closes secure channel, and releases network sockets.
func (a *Adapter) Stop() error

// Status returns current connection health, message counts, and error tallies.
func (a *Adapter) Status() adapter.AdapterStatus
```

---

## 2. OPC-UA Binary Wire Framing (`opc.tcp://`)

All OPC-UA TCP messages adhere to the Part 6 Binary wire encoding. Every frame starts with an 8-byte header formatted in Little-Endian byte order:

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|       Message Type (3 ASCII bytes: "HEL", "OPN", "MSG")       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Chunk Type (1 ASCII: 'F')     |      Message Size (32-bit LE) |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                 SecureChannelId (32-bit uint LE)              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   Payload Bytes (Length - 12)                 |
|                              ...                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Handshake Sequence
1. **Hello / Acknowledge (`HEL` / `ACK`)**:
   - Client sends `HEL` with protocol version (0), receive/send buffer sizes (e.g. 65,536 bytes), max message size, and max chunk count.
   - Server replies with `ACK` with accepted buffer sizes.
2. **Open Secure Channel (`OPN`)**:
   - Client sends `OPN` containing SecurityPolicy URI (e.g. `http://opcfoundation.org/UA/SecurityPolicy#None` or `#Basic256Sha256`), Client Certificate, and requested token lifetime (e.g. 300,000ms).
   - Server returns SecurityToken (ChannelId, TokenId, CreatedAt, RevisedLifetime).
3. **Session Activation (`MSG`)**:
   - `CreateSessionRequest`: ClientDescription, EndpointUrl, SessionName, ClientNonce, ClientCertificate.
   - `ActivateSessionRequest`: UserIdentityToken (Anonymous, UserNameIdentityToken, or X509IdentityToken), Signature.
4. **Subscription Setup (`MSG`)**:
   - `CreateSubscriptionRequest`: RequestedPublishingInterval (100ms), RequestedLifetimeCount (60), RequestedMaxKeepAliveCount (10), Priority (0).
   - `CreateMonitoredItemsRequest`: SubscriptionId, TimestampsToReturn (Both), MonitoredItemCreateRequests array.
5. **Continuous Publishing Loop (`MSG`)**:
   - Client maintains a pipeline of `PublishRequest` messages (depth: 2–5).
   - Server returns `PublishResponse` when data changes occur or keep-alive expires, containing `DataChangeNotification` and `MonitoredItemNotification` arrays.

---

## 3. Configuration Contract (`bridge.yaml`)

```yaml
bridge:
  gateway_endpoint: "127.0.0.1:50051"
  spool_dir: "/var/lib/pulse/bridge/spool"
  
adapters:
  opcua:
    - name: "opcua-boiler-plc"
      endpoint: "opc.tcp://192.168.1.120:4840"
      security_policy: "None"
      security_mode: "None"
      auth_mode: "anonymous"
      publish_interval: "100ms"
      keep_alive_interval: "5s"
      connect_timeout: "5s"
      nodes:
        - node_id: "ns=2;s=Boiler.SteamPressure"
          device_id: "boiler-01"
          metric_name: "steam_pressure_bar"
          sampling_interval: "50ms"
        - node_id: "ns=2;s=Boiler.FlameTemp"
          device_id: "boiler-01"
          metric_name: "flame_temp_celsius"
          deadband_type: "Absolute"
          deadband_value: 0.5
      discovery:
        enabled: true
        root_node_id: "ns=0;i=85"
        path_filter: "Objects/Boiler/*"
        max_depth: 5
        auto_subscribe: true
```

---

## 4. Prometheus Metrics Contract

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `pulse_bridge_opcua_active_subscriptions` | Gauge | `adapter` | Count of active OPC-UA server subscriptions. |
| `pulse_bridge_opcua_monitored_items_total` | Gauge | `adapter` | Total registered monitored items across all sessions. |
| `pulse_bridge_opcua_notifications_total` | Counter | `adapter`, `status` | Total data change notifications received from server. |
| `pulse_bridge_opcua_connection_state` | Gauge | `adapter` | Connection state (0=Disconnected, 1=Connecting, 2=Connected, 3=Subscribed). |
| `pulse_bridge_opcua_errors_total` | Counter | `adapter`, `type` | Counter of OPC-UA protocol, network, and timeout errors. |
| `pulse_bridge_opcua_browse_duration_seconds` | Histogram | `adapter` | Latency distribution of address space discovery browsing. |

