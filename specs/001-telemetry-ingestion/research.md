# Phase 0 Research: Telemetry Ingestion and Ingestion Gateway

**Feature**: `001-telemetry-ingestion`
**Date**: 2026-10-20
**Status**: Complete

---

## 1. Message Broker Selection & Configuration

### Decision
Deploy **Redpanda** as a single-node container via Docker Compose for local development and testing, with auto-provisioning of the primary topic `telemetry.raw` configured with 12 partitions.

### Rationale
- **Kafka-API Compatibility**: Redpanda is 100% compatible with the Apache Kafka v2+ protocol, enabling the use of standard Kafka client libraries while eliminating JVM overhead and ZooKeeper/KRaft complexity.
- **Predictable Latency & Low Resource Footprint**: Built in C++ with a thread-per-core architecture, Redpanda achieves sub-millisecond p99 latency under heavy concurrent write loads.
- **Partitioning Model**: 12 partitions allow testing concurrent consumer goroutines and parallel hash distribution matching the target core count of standard developer machines.

### Alternatives Considered
- **Apache Kafka (Bitnami/KRaft)**: High memory usage (>1.5 GB JVM heap), slower container startup time, and higher operational friction for local testing.
- **NATS JetStream**: Excellent lightweight message broker, but lacks native partitioned ordered-per-key log semantics matching industry-standard Kafka/Flink streaming interview standards.

---

## 2. Go Kafka Client Library

### Decision
Use **`twmb/franz-go`** as the Kafka client library for the Go Ingestion Gateway.

### Rationale
- **Pure Go (Zero CGo)**: Unlike `confluent-kafka-go` (which binds to C `librdkafka`), `franz-go` compiles cleanly on all platforms with zero C toolchain dependencies.
- **Zero-Allocation & Memory Pooling**: Built specifically for extreme throughput ($>1,000,000\text{ msgs/sec}$), offering buffer reuse pools that minimize Go GC pause times under sustained 50,000 events/sec load.
- **Deterministic Key Partitioning**: Provides built-in Murmur2 and FNV-1a hash partitioners, ensuring `device_id` always hashes to the exact same partition across all broker instances.
- **Native Support for Client-Side Batching**: Configurable batch linger and compression (snappy / zstd) to optimize network throughput while maintaining low latency.

### Alternatives Considered
- **`confluentinc/confluent-kafka-go`**: Requires CGo and `librdkafka` C libraries, complicating cross-compilation, Docker image builds, and sanitizers.
- **`segmentio/kafka-go`**: Pure Go, but higher memory allocation per message and less active maintenance compared to `franz-go`.
- **`Shopify/sarama`**: Older architecture with higher goroutine overhead and memory churn under high batch volumes.

---

## 3. Ingestion Transport Architecture: Dual gRPC and HTTP/JSON

### Decision
Expose two concurrent transport servers from a single Go binary:
1. **gRPC Server (`:50051`)**: For high-volume binary streaming and edge gateways with protobuf support.
2. **HTTP/JSON Server (`:8080`)**: For standard REST webhooks, legacy edge devices, `/metrics` (Prometheus), and `/healthz` probes.
Both transports forward decoded readings into a unified internal producer pipeline.

### Rationale
- **Protocol Flexibility**: Industrial edge devices vary widely; modern IoT gateways prefer binary gRPC streams for efficiency, whereas HTTP/JSON enables simple curl-based debugging and legacy device integration.
- **Shared Validation & Producer Core**: Isolates transport decoding from business logic, ensuring identical schema validation, clock-skew checks, and Redpanda dispatch logic.

### Alternatives Considered
- **gRPC-only**: Would alienate simple HTTP/JSON edge devices and complicate health/metric scraping probes without adding extra proxy layers like grpc-gateway.
- **HTTP/JSON-only**: Inefficient serialization and HTTP/1.1 head-of-line blocking would make sustaining $\ge 50,000\text{ events/sec}$ significantly harder on modest hardware.

---

## 4. Serialization Format: Protocol Buffers v3

### Decision
Define `TelemetryReading` and `TelemetryBatch` using **Protocol Buffers v3**, compiling Go structs via `protoc-gen-go` and `protoc-gen-go-grpc`.

### Rationale
- **Wire Efficiency**: Binary protobuf serialization reduces payload size by 60–80% compared to JSON, saving network bandwidth and broker disk I/O.
- **Schema Evolution & Stability**: Constitution Principle II and Operational Constraints require backwards/forwards schema compatibility; Protobuf field tags and default-value semantics safely handle firmware updates.
- **Direct Broker Payload**: The binary-encoded protobuf bytes can be published directly into Redpanda `telemetry.raw` without intermediate transformations.

### Alternatives Considered
- **JSON over Kafka**: High serialization CPU overhead and inflated payload sizes.
- **Apache Avro**: Requires an external Schema Registry service, adding unnecessary infrastructure complexity to the local developer workflow.

---

## 5. Ingestion Validation & Clock Skew Guardrails

### Decision
Implement strict, fail-fast validation in Go before dispatching to Redpanda:
1. `device_id` must be non-empty and $\le 128$ characters.
2. `metric_name` must be non-empty and $\le 128$ characters.
3. `timestamp`: Event timestamp must not be more than **5 minutes in the future** (to prevent future-skew corrupting downstream watermarks) nor more than **24 hours in the past** (to prevent unbounded out-of-order buffering).
4. `value`: Must be a valid IEEE 754 floating-point number (rejecting `NaN` and `+Inf` / `-Inf`).

### Rationale
- Safeguards the downstream Stream Processor (Milestone 002) against watermark skew, partition stalling, and false-positive anomaly alerts.
- Validates at the network perimeter; rejects malformed readings immediately with HTTP 400 / gRPC `InvalidArgument`.

---

## 6. Observability: Prometheus Metrics & Health Checking

### Decision
Integrate `prometheus/client_golang` directly into the gateway:
- `telemetry_ingested_total{protocol="grpc|http", status="success|error"}` (Counter)
- `telemetry_ingestion_duration_seconds{protocol="grpc|http"}` (Histogram with sub-10ms buckets)
- `telemetry_payload_bytes_total{protocol="grpc|http"}` (Counter)
- `telemetry_clock_skew_rejected_total` (Counter)
- `/healthz`: Periodically pings the Redpanda broker metadata to ensure cluster connectivity.

### Rationale
- Enables direct observability into throughput, latency percentiles ($p50, p95, p99$), and error rates.
- Ensures the gateway can be monitored and validated during load testing to satisfy Success Criteria SC-001 and SC-002.
