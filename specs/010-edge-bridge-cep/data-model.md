# Data Model: Milestone 010 — Industrial Edge Bridge & Complex Event Processing (CEP) Engine

**Feature**: `010-edge-bridge-cep`  
**Date**: 2026-10-30  
**Status**: Completed  

---

## 1. Edge Bridge Configuration & Protocol Adapter Models

### EdgeBridgeConfig
Master configuration model for `pulse-bridge`:

```go
type EdgeBridgeConfig struct {
    BridgeID         string                `json:"bridge_id" yaml:"bridge_id"` // Unique bridge identifier (e.g. "bridge-plant-alpha")
    GatewayEndpoint  string                `json:"gateway_endpoint" yaml:"gateway_endpoint"` // "gateway.pulse.internal:50051"
    TLSClientCert    string                `json:"tls_client_cert" yaml:"tls_client_cert"`   // Path to mTLS client cert
    TLSClientKey     string                `json:"tls_client_key" yaml:"tls_client_key"`     // Path to mTLS client private key
    TLSCACert        string                `json:"tls_ca_cert" yaml:"tls_ca_cert"`           // Path to platform CA cert
    APIKey           string                `json:"api_key" yaml:"api_key"`                   // Optional fallback API key
    SpoolDir         string                `json:"spool_dir" yaml:"spool_dir"`               // Local disk path for offline buffer
    MaxSpoolBytes    int64                 `json:"max_spool_bytes" yaml:"max_spool_bytes"`   // Max buffer allocation (e.g. 5GB)
    BatchLinger      time.Duration         `json:"batch_linger" yaml:"batch_linger"`         // Micro-batch flush window (e.g. 10ms)
    BatchMaxRecords  int                   `json:"batch_max_records" yaml:"batch_max_records"` // Micro-batch size (e.g. 1000)
    MQTTAdapters     []MQTTAdapterConfig   `json:"mqtt" yaml:"mqtt"`
    ModbusAdapters   []ModbusAdapterConfig `json:"modbus" yaml:"modbus"`
}
```

### MQTTAdapterConfig
Defines MQTT broker connection and topic translation mappings:

```go
type MQTTAdapterConfig struct {
    BrokerURL     string            `json:"broker_url" yaml:"broker_url"` // "tcp://localhost:1883"
    ClientID      string            `json:"client_id" yaml:"client_id"`   // "pulse-bridge-mqtt"
    Username      string            `json:"username,omitempty" yaml:"username,omitempty"`
    Password      string            `json:"password,omitempty" yaml:"password,omitempty"`
    Topics        []string          `json:"topics" yaml:"topics"`         // ["factory/+/+/telemetry", "spBv1.0/#"]
    Format        string            `json:"format" yaml:"format"`         // "json" or "sparkplug_b"
    TopicPattern  string            `json:"topic_pattern,omitempty" yaml:"topic_pattern,omitempty"` // Regex / token template
    DeviceIDField string            `json:"device_id_field,omitempty" yaml:"device_id_field,omitempty"`
}
```

### ModbusAdapterConfig & RegisterDefinition
Defines Modbus TCP cyclic polling configuration:

```go
type ModbusAdapterConfig struct {
    Endpoint      string               `json:"endpoint" yaml:"endpoint"`       // "192.168.1.100:502"
    SlaveID       uint8                `json:"slave_id" yaml:"slave_id"`       // Modbus Unit / Slave ID
    PollInterval  time.Duration        `json:"poll_interval" yaml:"poll_interval"` // e.g. 100ms
    Timeout       time.Duration        `json:"timeout" yaml:"timeout"`         // e.g. 2s
    Registers     []RegisterDefinition `json:"registers" yaml:"registers"`
}

type RegisterDefinition struct {
    DeviceID      string  `json:"device_id" yaml:"device_id"`       // "generator-01"
    MetricName    string  `json:"metric_name" yaml:"metric_name"`   // "oil_pressure"
    Unit          string  `json:"unit" yaml:"unit"`                 // "psi"
    FunctionCode  uint8   `json:"function_code" yaml:"function_code"` // 3 (Holding) or 4 (Input)
    StartAddress  uint16  `json:"start_address" yaml:"start_address"` // e.g. 40001
    DataType      string  `json:"data_type" yaml:"data_type"`       // "int16", "uint16", "float32_be", "float32_le"
    Multiplier    float64 `json:"multiplier" yaml:"multiplier"`     // Linear scale factor (e.g. 0.1)
    Offset        float64 `json:"offset" yaml:"offset"`             // Linear offset
}
```

---

## 2. Store-and-Forward Offline Buffer Data Structures

Disk buffer layout organized as append-only chunk segments:

```text
/var/lib/pulse/bridge/
├── manifest.json            # Read/write segment pointers & state
├── spool_000001.log         # Binary framed telemetry records
├── spool_000002.log
└── ...
```

### BufferFrameHeader
Fixed 16-byte binary header per buffered record:

```text
+-----------------------+-----------------------+-----------------------+-----------------------+
| Magic (4 bytes)       | Frame Length (4 bytes)| CRC32 (4 bytes)       | Flags (4 bytes)       |
| 0x50554252 ("PUBR")   | uint32 (Big-Endian)   | IEEE uint32           | 0x01 = Protobuf       |
+-----------------------+-----------------------+-----------------------+-----------------------+
```

```go
type SpoolManifest struct {
    Version          int    `json:"version"`           // 1
    CurrentWriteSeq  uint64 `json:"current_write_seq"` // Current segment file index
    CurrentReadSeq   uint64 `json:"current_read_seq"`  // Unacknowledged read segment file index
    ReadByteOffset   int64  `json:"read_byte_offset"`  // Byte position in read segment
    TotalBufferedOps uint64 `json:"total_buffered_ops"`
    TotalDiskBytes   int64  `json:"total_disk_bytes"`
}
```

---

## 3. Complex Event Processing (CEP) Rule Models

### CEPRule
Declarative rule definition loaded from YAML/JSON:

```go
type CEPRule struct {
    RuleID          string          `json:"rule_id" yaml:"rule_id"`                   // "RULE-VIBE-TEMP-CASCADE"
    Name            string          `json:"name" yaml:"name"`                         // "Bearing Thermal Runaway Cascade"
    Description     string          `json:"description" yaml:"description"`
    DevicePattern   string          `json:"device_pattern" yaml:"device_pattern"`     // "turbine-*" glob pattern
    WindowDuration  time.Duration   `json:"window_duration" yaml:"window_duration"`   // Maximum sequence evaluation window (e.g. 45s)
    PatternType     string          `json:"pattern_type" yaml:"pattern_type"`         // "SEQUENCE" or "EXPRESSION"
    SequenceSteps   []PatternStep   `json:"sequence_steps,omitempty" yaml:"sequence_steps,omitempty"`
    Expression      string          `json:"expression,omitempty" yaml:"expression,omitempty"` // "power_mw / fuel_flow < 0.70"
    Severity        string          `json:"severity" yaml:"severity"`                 // "WARNING" or "CRITICAL"
    Cooldown        time.Duration   `json:"cooldown" yaml:"cooldown"`                 // Suppress re-firing for duration
}

type PatternStep struct {
    StepIndex       int             `json:"step_index" yaml:"step_index"`
    MetricName      string          `json:"metric_name" yaml:"metric_name"`           // "vibration"
    Condition       string          `json:"condition" yaml:"condition"`               // "value > threshold" or "z_score >= 3.5"
    Threshold       float64         `json:"threshold,omitempty" yaml:"threshold,omitempty"`
    ZScoreThreshold float64         `json:"z_score_threshold,omitempty" yaml:"z_score_threshold,omitempty"`
}
```

### SequenceCandidate (Partition-Local State)
In-flight state tracking active sequence progression for a device within a partition worker:

```go
type SequenceCandidate struct {
    RuleID        string            `json:"rule_id"`
    DeviceID      string            `json:"device_id"`
    CurrentStep   int               `json:"current_step"` // 0, 1, 2...
    StartTimeNs   int64             `json:"start_time_ns"` // Event-time of Step 0 match
    DeadlineNs    int64             `json:"deadline_ns"`   // StartTimeNs + WindowDuration
    MatchedEvents []CandidateEvent  `json:"matched_events"`
}

type CandidateEvent struct {
    StepIndex   int       `json:"step_index"`
    MetricName  string    `json:"metric_name"`
    Value       float64   `json:"value"`
    TimestampNs int64     `json:"timestamp_ns"`
}
```

---

## 4. Composite Incident Synthesis Event Model

When a multi-stage sequence or cross-metric formula matches, a `CompositeIncidentEvent` is emitted to `telemetry.alerts`:

```json
{
  "incident_id": "comp-inc-8f12a9c4-001",
  "rule_id": "RULE-VIBE-TEMP-CASCADE",
  "device_id": "turbine-01",
  "severity": "CRITICAL",
  "status": "FIRING",
  "summary": "Bearing Thermal Runaway: vibration spike > 4.2σ preceded bearing_temp surge within 28.4s",
  "detected_time_ns": 1730300028400000000,
  "initiating_metric": {
    "metric_name": "vibration",
    "value": 8.45,
    "z_score": 4.21,
    "timestamp_ns": 1730300000000000000
  },
  "correlated_symptoms": [
    {
      "metric_name": "bearing_temp",
      "value": 104.2,
      "z_score": 3.84,
      "delta_time_seconds": 28.4,
      "timestamp_ns": 1730300028400000000
    }
  ]
}
```

