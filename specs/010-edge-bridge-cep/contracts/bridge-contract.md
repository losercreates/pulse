# Contract: Industrial Edge Bridge Interfaces

**Feature**: `010-edge-bridge-cep`  
**Date**: 2026-10-30  

---

## 1. ProtocolAdapter (Source Driver Interface)

```go
package bridge

import (
    "context"
    telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// ProtocolAdapter defines the lifecycle and message streaming interface for edge protocol drivers
// (MQTT v3/v5, Sparkplug B, Modbus TCP).
type ProtocolAdapter interface {
    // Name returns the unique adapter identifier (e.g. "mqtt-primary", "modbus-plc-01").
    Name() string

    // Start connects to the industrial fieldbus/broker and begins streaming telemetry.
    Start(ctx context.Context, out chan<- *telemetryv1.TelemetryReading) error

    // Stop terminates connection loops and releases network resources.
    Stop() error

    // Status returns health and operational diagnostic metrics.
    Status() AdapterStatus
}

type AdapterStatus struct {
    Connected       bool
    MessagesReceived uint64
    ErrorsTotal     uint64
    LastError       string
}
```

---

## 2. OfflineSpooler (Disk-Backed FIFO Buffer Interface)

```go
package bridge

import telemetryv1 "github.com/losercreates/pulse/proto/v1"

// OfflineSpooler persists unacknowledged telemetry records to local disk during network disconnects
// and re-reads them in chronological sequence upon reconnection.
type OfflineSpooler interface {
    // Write appends a batch of telemetry records to the local spool.
    Write(readings []*telemetryv1.TelemetryReading) error

    // ReadNext retrieves the next available FIFO batch from disk. Returns nil when empty.
    ReadNext(maxRecords int) ([]*telemetryv1.TelemetryReading, error)

    // Ack commits the read offset, permitting disk segment cleanup.
    Ack(count int) error

    // BufferedCount returns the total number of unread records currently spooled on disk.
    BufferedCount() int64

    // Close flushes open segment handles.
    Close() error
}
```

---

## 3. GatewayForwarder (Upstream gRPC Shipper Interface)

```go
package bridge

import (
    "context"
    telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// GatewayForwarder manages authenticated client-side streaming to pulse-gateway over mTLS.
type GatewayForwarder interface {
    // ForwardBatch sends a batch of telemetry readings over active gRPC stream.
    ForwardBatch(ctx context.Context, batch []*telemetryv1.TelemetryReading) (*telemetryv1.IngestionReceipt, error)

    // IsConnected reports whether the upstream gateway is reachable.
    IsConnected() bool

    // Close terminates the gRPC connection pool.
    Close() error
}
```

