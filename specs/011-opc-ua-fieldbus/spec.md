# Feature Specification: Milestone 011 — Native OPC-UA Binary Fieldbus Adapter

**Feature Branch**: `011-opc-ua-fieldbus`  
**Created**: 2026-11-03  
**Status**: Draft  
**Input**: User description: "Milestone 011: Native OPC-UA Binary Fieldbus Adapter - Implement a pure Go binary OPC-UA driver for pulse-bridge with real-time monitored item subscriptions, dynamic node discovery, and integration with the offline disk spooler."

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Real-Time OPC-UA Monitored Item Subscriptions & Telemetry Ingestion (Priority: P1) 🎯 MVP

As an Industrial Automation Engineer, I need the edge bridge daemon to establish a direct binary session with industrial OPC-UA servers (e.g. Siemens S7-1500, Beckhoff TwinCAT, Kepware, Rockwell), create server-side subscriptions, and monitor target variable nodes for real-time value changes so that machine telemetry is captured instantly upon modification without wasteful and laggy cyclic polling.

**Why this priority**: Monitored item subscriptions represent the standard modern communication pattern in industrial automation. Unlike cyclic polling (which wastes network bandwidth and strains PLC CPUs), event-driven subscriptions allow the edge bridge to capture sub-millisecond state transitions and high-frequency sensor shifts immediately when they occur.

**Independent Test**: Connect the edge bridge to an OPC-UA test server with simulated numeric variables; create subscriptions for 50 distinct NodeIDs; publish value changes at varying rates (10Hz to 100Hz); verify all changes are decoded into canonical telemetry records with original source timestamps and streamed over secure transport to the ingestion gateway in real time.

**Acceptance Scenarios**:
1. **Given** an OPC-UA server exposing variable nodes with various data types (Boolean, Int16, Int32, UInt32, Float, Double, String), **When** a monitored item's value changes on the server, **Then** the edge bridge receives the data change notification, extracts the value and timestamp, normalizes it to a canonical telemetry reading, and dispatches it upstream.
2. **Given** a monitored item configured with a deadband filter (absolute or percent), **When** value fluctuations remain within the deadband threshold, **Then** the server suppresses notifications and the bridge processes no redundant telemetry.
3. **Given** an incoming data value with an OPC-UA StatusCode indicating degraded quality (e.g., `Uncertain`, `BadSensorFailure`), **When** the bridge constructs the telemetry reading, **Then** the reading includes quality status metadata, and values marked with fatal `Bad` status are tracked with diagnostic error metrics.

---

### User Story 2 - Resilient Connection State Machine & Offline Spooler Handoff (Priority: P1)

As a Field Reliability Engineer, I need the OPC-UA adapter to handle field network interruptions, server reboots, and upstream WAN disconnects gracefully, maintaining subscription keep-alives and seamlessly redirecting telemetry into the local disk-backed FIFO spooler during outages so that zero machine data is lost.

**Why this priority**: Industrial plant floors and edge installations experience frequent network instability, maintenance cycles, and PLC restarts. Without automatic reconnection, session renegotiation, and tight integration with the offline disk spooler, transient disruptions would result in unrecoverable telemetry gaps.

**Independent Test**: Establish an active OPC-UA subscription streaming telemetry through the bridge; abruptly terminate the upstream gateway connection; verify telemetry is spooled to the local append-only disk buffer; reconnect the gateway and verify spooled records are flushed in chronological order; simulate an OPC-UA server reboot; verify the bridge detects connection loss via keep-alive timeouts, enters exponential backoff reconnection, and automatically re-subscribes once the server becomes available.

**Acceptance Scenarios**:
1. **Given** an active OPC-UA session, **When** upstream connectivity to the ingestion gateway fails, **Then** all incoming subscription notifications are written directly to the local disk FIFO spooler (`PUBR` framing with CRC32 verification) without dropping records.
2. **Given** an abrupt network disconnect between the edge bridge and the OPC-UA server, **When** heartbeat/keep-alive acknowledgments fail past the configured timeout, **Then** the bridge marks the session disconnected, closes orphaned sockets, and initiates periodic reconnection attempts with exponential backoff.
3. **Given** a re-established OPC-UA connection after server downtime, **When** the bridge connects and creates a new session, **Then** it automatically reconstitutes all configured subscriptions and monitored items without requiring a process restart.

---

### User Story 3 - Dynamic Address Space Browsing & Variable Discovery (Priority: P2)

As a Plant Commissioning Engineer, I need the edge bridge to recursively browse the OPC-UA server's hierarchical address space starting from root or folder nodes, discover available variables matching configured browse paths or filter expressions, and automatically subscribe to them so that onboarding new equipment does not require manual entry of hundreds of static NodeIDs.

**Why this priority**: Modern industrial machines expose complex nested address spaces (e.g., `Objects -> Factory -> Line1 -> DriveUnit -> Status`). Requiring engineers to manually look up and hardcode raw numeric or GUID NodeIDs is error-prone, labor-intensive, and brittle to minor PLC program changes.

**Independent Test**: Point the edge bridge at an OPC-UA server with a nested folder structure containing 200 variables under `Objects/PlantA/Turbines`; configure a discovery rule matching `Objects/PlantA/Turbines/*/*_temp`; verify the bridge discovers all matching variable nodes and subscribes to them without manual configuration of individual NodeIDs.

**Acceptance Scenarios**:
1. **Given** an OPC-UA server address space with nested folders and object nodes, **When** discovery is triggered with a root path and pattern filter, **Then** the bridge recursively browses child references, filters nodes of `Variable` class, and outputs a catalog of discovered variable identifiers.
2. **Given** dynamic discovery configured in automatic subscription mode, **When** variable nodes match the filter criteria, **Then** the bridge immediately registers monitored items for all matching nodes using configured default sampling parameters.
3. **Given** an address space containing circular references or deep hierarchical nesting, **When** the bridge browses the hierarchy, **Then** it enforces a configurable maximum recursion depth and tracks visited node identifiers to prevent infinite loops.

---

### User Story 4 - Industrial Security Profiles & Authentication Modes (Priority: P2)

As an Industrial Cybersecurity Engineer, I need the OPC-UA adapter to support industry-standard security policies and user authentication mechanisms so that communication across factory networks complies with ISA/IEC 62443 industrial cybersecurity standards.

**Why this priority**: Operational technology (OT) networks mandate encrypted fieldbus communication to prevent eavesdropping and unauthorized command injection. Supporting standard security policies and credential models enables deployment across secure production zones as well as isolated lab environments.

**Independent Test**: Connect the edge bridge to an OPC-UA server enforcing `Basic256Sha256` encryption with certificate exchange and username/password credentials; verify successful session activation and encrypted communication; attempt connection with an untrusted client certificate and verify the connection is rejected with an authentication failure.

**Acceptance Scenarios**:
1. **Given** an OPC-UA server configured with security policy `Basic256Sha256` and message mode `SignAndEncrypt`, **When** the bridge establishes a secure channel, **Then** it presents its client certificate, validates the server certificate, and negotiates an encrypted communication channel.
2. **Given** an isolated development or benchmark environment, **When** security policy `None` is explicitly configured, **Then** the bridge connects without cryptographic overhead.
3. **Given** an OPC-UA server requiring user authentication, **When** the session is activated, **Then** the bridge provides credentials using the configured token type (anonymous, username/password, or X.509 certificate).

---

## Edge Cases

- **Server Subscription Overload**: What happens when an OPC-UA server rejects subscription creation due to reaching its internal resource or monitored item limit?
  - *Resolution*: The bridge logs an alert with the server's diagnostic error code, attempts fallback to batching multiple monitored items across smaller subscriptions, and enters a degraded operating mode while continuing to service successfully created subscriptions.
- **Torn or Rapidly Flapping Network Links**: How does the bridge handle network links that rapidly drop and re-establish every few seconds?
  - *Resolution*: A connection hysteresis and debounce timer prevents thrashing the OPC-UA server with rapid session handshake renegotiations. Reconnection backoff increases exponentially up to a configured ceiling.
- **Timestamp Skew & Source vs. Server Timestamps**: How does the bridge resolve discrepancies between the PLC's internal source timestamp and the OPC-UA server's publication timestamp?
  - *Resolution*: The bridge prefers the OPC-UA `SourceTimestamp` if valid and non-zero; if the source timestamp is omitted or corrupted, it falls back to `ServerTimestamp`, and finally to edge bridge reception time, recording the timestamp source attribute in the telemetry metadata.
- **NodeID Re-indexing after PLC Firmware Update**: What happens if a PLC reboot re-assigns numeric NodeIDs for the same logical variables?
  - *Resolution*: For dynamic discovery mode using browse paths, the bridge re-resolves browse paths to new NodeIDs upon session reconnection. For static string-based NodeIDs (standard in modern PLCs), identifiers remain stable across reboots.
- **High-Velocity Data Change Storms**: What happens when thousands of monitored items change simultaneously during an emergency plant trip?
  - *Resolution*: Incoming notification chunks are buffered in memory and throttled to prevent memory exhaustion, overflowing to the disk FIFO spooler if in-memory queue high-water marks are exceeded.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST establish binary TCP connections to standard OPC-UA server endpoints (`opc.tcp://<host>:<port>`).
- **FR-002**: System MUST support OPC-UA Security Policies `None` and `Basic256Sha256` with message security modes `None`, `Sign`, and `SignAndEncrypt`.
- **FR-003**: System MUST support client authentication using Anonymous, Username/Password, and X.509 Client Certificate credentials.
- **FR-004**: System MUST support creating and managing server-side subscriptions with configurable publishing intervals (default: 100ms) and keep-alive intervals.
- **FR-005**: System MUST support creating Monitored Items across numeric (`ns=i;i=...`) and string (`ns=i;s=...`) NodeID types.
- **FR-006**: System MUST parse OPC-UA `DataValue` payloads across standard primitive data types including Boolean, Byte, Int16, Int32, Int64, UInt16, UInt32, UInt64, Float (single precision IEEE 754), Double (double precision IEEE 754), and String.
- **FR-007**: System MUST map parsed OPC-UA variables into canonical telemetry readings, extracting metric name, device identifier, float value, and nanosecond timestamp.
- **FR-008**: System MUST evaluate OPC-UA `StatusCode` values, flagging non-good readings (`Uncertain` or `Bad`) and emitting diagnostic metrics.
- **FR-009**: System MUST provide hierarchical address space browsing starting from configurable root nodes (default: `ns=0;i=85` Objects folder) with pattern matching for automatic variable discovery.
- **FR-010**: System MUST enforce configurable maximum recursion depth and circular reference detection during address space browsing.
- **FR-011**: System MUST implement a connection state machine with automatic heartbeat/keep-alive detection, connection loss detection, and exponential backoff reconnection.
- **FR-012**: System MUST integrate directly with the existing disk-backed FIFO spooler, diverting OPC-UA telemetry to disk whenever upstream transmission to the gateway is unavailable.
- **FR-013**: System MUST flush spooled OPC-UA records in strict chronological order upon upstream link restoration before returning to live streaming.
- **FR-014**: System MUST expose Prometheus metrics for active subscriptions, monitored items, notification throughput, connection state transitions, and error counters.
- **FR-015**: System MUST support declarative YAML configuration defining server endpoints, security credentials, static monitored item lists, and dynamic discovery rules.

### Key Entities

- **OPC-UA Endpoint**: Represents a remote industrial server connection target, defining URL (`opc.tcp://...`), security policy, authentication credentials, and timeout configurations.
- **Subscription Session**: Represents an active stateful OPC-UA communication channel, managing session tokens, subscription IDs, keep-alive timers, and negotiated publishing intervals.
- **Monitored Item**: Represents a specific variable node registered on the server for change detection, specifying NodeID, client handle, sampling interval, queue size, and deadband filter.
- **Discovered Node**: Represents a variable node located via address space browsing, containing browse path, NodeID, display name, data type, and parent hierarchy.
- **Canonical Fieldbus Reading**: The normalized output structure carrying device ID, metric name, timestamp in nanoseconds, floating-point value, and status quality code.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: System captures and processes value changes from at least 1,000 simultaneously monitored items on a single edge bridge instance without exceeding 100MB of resident memory.
- **SC-002**: End-to-end latency from server value change notification to ingestion gateway dispatch is under 15 milliseconds under nominal network conditions.
- **SC-003**: In the event of an upstream network disconnect lasting 1 hour, zero telemetry data is lost, with 100% of spooled records delivered in chronological order upon reconnection.
- **SC-004**: System automatically detects an abrupt OPC-UA server crash and re-establishes subscriptions within 10 seconds of the server returning online.
- **SC-005**: Dynamic address space discovery successfully identifies and catalogs all matching variables across a 5,000-node hierarchy in under 5 seconds.
- **SC-006**: 100% pure Go implementation with zero external C/C++ library dependencies, compiling to a single static binary.

---

## Assumptions

- **Target Equipment**: The remote equipment exposes an OPC-UA server compliant with OPC-UA Part 4 (Services) and Part 6 (Mappings - Binary Protocol).
- **Network Protocol**: Field communication uses the standard binary TCP protocol (`opc.tcp://`) on port 4840 or custom ports; legacy COM/DCOM OPC (OPC DA) is explicitly out of scope.
- **Timestamp Integrity**: The remote OPC-UA server maintains synchronized time (via NTP or PTP) to ensure source timestamps reflect accurate physical event times.
- **Security Defaults**: Development and local simulation environments default to `None` security policy; production deployments require `Basic256Sha256` or equivalent encryption.
- **Storage Reuse**: The disk spooler mechanism implemented in Milestone 010 (`PUBR` framing, CRC32 verification, atomic directory manifest) is reused directly without requiring a new storage format.

