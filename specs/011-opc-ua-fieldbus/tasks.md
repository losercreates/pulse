# Tasks: Milestone 011 — Native OPC-UA Binary Fieldbus Adapter

**Input**: Design documents from `/specs/011-opc-ua-fieldbus/`  
**Prerequisites**: `plan.md`, `spec.md`, `research.md`, `data-model.md`, `contracts/opcua-adapter-contract.md`, `quickstart.md`

---

## Phase 1: Setup (Shared Infrastructure & Protocol Contracts)

**Purpose**: Configuration structures, OPC-UA protocol types, and Prometheus metric instrumentation.

- [ ] T001 Extend bridge configuration models and YAML parser with `OPCUAAdapterConfig`, `MonitoredItemConfig`, and `DiscoveryConfig` in `internal/bridge/config/config.go`
- [ ] T002 [P] Implement core OPC-UA data types, NodeID representations, Variant encoders/decoders, and StatusCode lookups in `internal/bridge/adapter/opcua/types.go`
- [ ] T003 [P] Register OPC-UA Prometheus gauges, counters, and histograms (`pulse_bridge_opcua_*`) in `internal/bridge/metrics/metrics.go`

---

## Phase 2: Foundational (Binary Framing & Secure Channel State Machine)

**Purpose**: Core binary message wire framing, channel handshake, and test harness required before any user stories can proceed.

**⚠️ CRITICAL**: Foundational tasks must be completed before user story implementation.

- [ ] T004 [P] Implement OPC-UA TCP binary wire framing (`HEL`/`ACK`, `OPN`, `MSG` headers, chunking, and byte ordering) in `internal/bridge/adapter/opcua/binary.go`
- [ ] T005 [P] Implement SecureChannel handshake, security token negotiation, and session state machine in `internal/bridge/adapter/opcua/channel.go`
- [ ] T006 [P] Create mock OPC-UA TCP binary server test harness for deterministic protocol unit tests in `internal/bridge/adapter/opcua/mock_server_test.go`

**Checkpoint**: Foundation ready — binary communication channel can establish and authenticate with OPC-UA endpoints.

---

## Phase 3: User Story 1 - Real-Time Monitored Item Subscriptions & Telemetry Ingestion (Priority: P1) 🎯 MVP

**Goal**: Edge bridge establishes an OPC-UA session, creates server-side subscriptions, registers monitored items, decodes incoming `DataChangeNotification` frames, and streams normalized `TelemetryReading` Protobuf records to the gateway.

**Independent Test**: Connect the adapter to the mock OPC-UA server; register monitored items across diverse primitive types (Boolean, Int16, Int32, UInt32, Float, Double, String); trigger data changes; verify normalized `TelemetryReading` messages are streamed into the bridge's ingestion channel with correct values, timestamps, and status metadata.

### Tests for User Story 1

- [ ] T007 [P] [US1] Create unit tests for subscription request creation, monitored item registration, and `DataChangeNotification` decoding in `internal/bridge/adapter/opcua/subscription_test.go`

### Implementation for User Story 1

- [ ] T008 [P] [US1] Implement `CreateSubscriptionRequest` and `CreateMonitoredItemsRequest` protocol builders in `internal/bridge/adapter/opcua/subscription.go`
- [ ] T009 [US1] Implement pipelined `PublishRequest` dispatch loop and `DataChangeNotification` payload unpacker in `internal/bridge/adapter/opcua/publish.go`
- [ ] T010 [US1] Implement `Adapter` struct fulfilling the `adapter.ProtocolAdapter` interface (`Name`, `Start`, `Stop`, `Status`) in `internal/bridge/adapter/opcua/adapter.go`
- [ ] T011 [US1] Integrate OPC-UA adapter instantiation into bridge coordinator and daemon entrypoint in `internal/bridge/coordinator.go` and `cmd/bridge/main.go`

**Checkpoint**: User Story 1 operational. Standalone `pulse-bridge` streams real-time OPC-UA subscription telemetry to `pulse-gateway` (MVP complete).

---

## Phase 4: User Story 2 - Resilient Connection State Machine & Offline Spooler Handoff (Priority: P1)

**Goal**: Adapter detects network drops or server restarts via keep-alive timeouts, enters exponential backoff reconnection, re-establishes subscriptions, and redirects incoming telemetry to the local disk FIFO spooler during upstream gateway disconnects.

**Independent Test**: Sever gateway connectivity while streaming; verify OPC-UA telemetry is spooled to disk (`PUBR` log files); restore gateway, verify chronological backfill with zero lost records; terminate OPC-UA server, observe keep-alive timeout detection, verify automatic re-subscription upon server reboot.

### Tests for User Story 2

- [ ] T012 [P] [US2] Create unit tests for keep-alive timeouts, exponential backoff reconnection loops, and offline spooler diversion in `internal/bridge/adapter/opcua/resilience_test.go`

### Implementation for User Story 2

- [ ] T013 [US2] Implement keep-alive heartbeat monitor and automatic subscription recovery in `internal/bridge/adapter/opcua/channel.go`
- [ ] T014 [US2] Implement seamless disk FIFO spooler handoff and operational status tracking during upstream network partitions in `internal/bridge/adapter/opcua/adapter.go`

**Checkpoint**: User Story 2 operational. Industrial edge bridge survives field network disconnects, server restarts, and upstream WAN outages without telemetry loss.

---

## Phase 5: User Story 3 - Dynamic Address Space Browsing & Variable Discovery (Priority: P2)

**Goal**: Edge bridge recursively browses the OPC-UA server address space from root or folder nodes, discovers variables matching path glob patterns, and automatically registers them as monitored items.

**Independent Test**: Configure dynamic discovery against a hierarchical mock server with nested folders and circular references; verify cycle detection prevents infinite loops; verify all matching variables are discovered and auto-subscribed.

### Tests for User Story 3

- [ ] T015 [P] [US3] Create unit tests for `Browse` service request encoding, response decoding, and circular reference detection in `internal/bridge/adapter/opcua/browse_test.go`

### Implementation for User Story 3

- [ ] T016 [US3] Implement recursive address space crawler with depth limits, reference type filtering, and glob path matching in `internal/bridge/adapter/opcua/browse.go`
- [ ] T017 [US3] Integrate dynamic discovery into adapter startup lifecycle and register discovered items with active subscriptions in `internal/bridge/adapter/opcua/adapter.go`

**Checkpoint**: User Story 3 operational. Plant commissioning engineers can onboard complex PLCs via path patterns without manually entering hundreds of NodeIDs.

---

## Phase 6: User Story 4 - Industrial Security Profiles & Authentication Modes (Priority: P2)

**Goal**: Support OPC-UA security policies `None` and `Basic256Sha256` (`SignAndEncrypt`), with Anonymous, Username/Password, and X.509 client certificate user identity tokens.

**Independent Test**: Connect adapter to mock server enforcing `Basic256Sha256` with client certificate exchange and username credentials; verify successful session activation and encrypted communication; verify connection rejection on invalid credentials.

### Tests for User Story 4

- [ ] T018 [P] [US4] Create unit tests for security token exchange, asymmetric signature validation, and identity token encoding in `internal/bridge/adapter/opcua/security_test.go`

### Implementation for User Story 4

- [ ] T019 [US4] Implement `Basic256Sha256` symmetric and asymmetric encryption, signature verification, and certificate validation in `internal/bridge/adapter/opcua/security.go`
- [ ] T020 [US4] Implement UserIdentityToken builders (Anonymous, UserNameIdentityToken, X509IdentityToken) in `internal/bridge/adapter/opcua/auth.go`

**Checkpoint**: User Story 4 operational. Edge bridge complies with ISA/IEC 62443 fieldbus encryption and authentication requirements.

---

## Phase 7: Polish, Integration Verification & Cross-Cutting Concerns

**Purpose**: End-to-end integration tests, throughput benchmarks, documentation, and configuration samples.

- [ ] T021 [P] Implement end-to-end integration test validating OPC-UA adapter, bridge coordinator, offline disk spooler, and gateway in `tests/integration/bridge_opcua_test.go`
- [ ] T022 [P] Implement high-frequency notification decoding benchmark ($\ge 25,000\text{ ev/s}$) in `internal/bridge/adapter/opcua/opcua_bench_test.go`
- [ ] T023 Update reference bridge configuration in `configs/bridge.yaml` with documented OPC-UA adapter configuration blocks
- [ ] T024 Validate all quickstart verification scenarios documented in `specs/011-opc-ua-fieldbus/quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately.
- **Foundational (Phase 2)**: Depends on Phase 1 — BLOCKS all user story implementations.
- **User Stories (Phase 3+)**: All depend on Phase 2 completion.
  - **User Story 1 (P1 - MVP)**: Can start after Phase 2 — core functionality.
  - **User Story 2 (P1)**: Depends on US1 (enhances channel and coordinator error handling).
  - **User Story 3 (P2)**: Depends on US1 (uses subscription and adapter handles).
  - **User Story 4 (P2)**: Depends on Phase 2 & US1 (enhances secure channel handshake).
- **Polish (Phase 7)**: Depends on completion of all user story phases.

### Parallel Opportunities

- **Phase 1**: T002 (Types) and T003 (Metrics) can run in parallel with T001 (Config).
- **Phase 2**: T004 (Binary Framing), T005 (Channel State), and T006 (Mock Server) can run in parallel.
- **Phase 3 (US1)**: T007 (Tests) and T008 (Subscription builders) can run in parallel.
- **Phase 4 (US2)**: T012 (Tests) can run in parallel with foundational work.
- **Phase 5 (US3)**: T015 (Tests) can run in parallel with US1.
- **Phase 6 (US4)**: T018 (Tests) can run in parallel with US1.
- **Phase 7**: T021 (Integration Test) and T022 (Benchmark) can run in parallel.

---

## Implementation Strategy

### MVP First (User Story 1 Only)
1. Complete Phase 1 (Setup: Config, Types, Metrics).
2. Complete Phase 2 (Foundational: Binary Framing, Channel State Machine, Mock Server).
3. Complete Phase 3 (User Story 1: Monitored Item Subscriptions & Telemetry Ingestion).
4. **STOP and VALIDATE**: Run `subscription_test.go` against mock server to verify MVP.

### Incremental Delivery
1. Add User Story 2 (Resilient Reconnection & Spooler Handoff) $\to$ verify edge WAN fault tolerance.
2. Add User Story 3 (Dynamic Address Space Discovery) $\to$ verify tag auto-discovery.
3. Add User Story 4 (Industrial Security Profiles) $\to$ verify `Basic256Sha256` encryption.
4. Execute Polish & Verification (Integration tests and benchmarks).

