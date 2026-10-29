# Feature Specification: Milestone 009 — Security & Access Control

**Feature Branch**: `009-security-access-control`  
**Created**: 2026-10-29  
**Status**: Draft  
**Input**: User description: "Create milestone 009: Security & Access Control with edge device authentication (mTLS and API keys), device identity anti-spoofing, ChronoDB query API authentication and Role-Based Access Control (RBAC), decoupled webhook HMAC-SHA256 signature verification, inter-service Kubernetes NetworkPolicies, and secret management"

---

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Edge Device Ingestion Authentication & Anti-Spoofing (Priority: P1) 🎯 MVP

As an Industrial IoT Security Officer, I need all sensor readings submitted to the Ingestion Gateway (HTTP and gRPC) to be strictly authenticated via Mutual TLS (mTLS) client certificates or cryptographically validated API keys, and verified to ensure the authenticated client identity matches the payload's `device_id`, so that unauthorized clients cannot inject fraudulent sensor data, impersonate turbines/sensors, or pollute stream-processing windows.

**Why this priority**: Ingestion is the perimeter boundary of the entire platform. Without client authentication and device identity binding, rogue actors or misconfigured devices can inject malicious telemetry, trigger false anomaly cascades, and poison analytical rollups.

**Independent Test**: Configure the Ingestion Gateway with a trusted CA and device API key store; submit valid readings with a valid client certificate whose Common Name matches `device_id` and observe 200 OK / gRPC OK; submit readings with an unauthenticated client and observe immediate 401 Unauthorized / gRPC `Unauthenticated`; submit readings with a valid certificate for `device-A` while the payload asserts `device_id: device-B` and observe 403 Forbidden / gRPC `PermissionDenied` anti-spoofing rejection.

**Acceptance Scenarios**:
1. **Given** an edge gateway connecting via gRPC or HTTPS with a valid X.509 client certificate signed by the platform CA, **When** submitting telemetry where `reading.device_id` matches the certificate CN/SAN, **Then** the request is accepted, authenticated device metadata is attached to the request context, and the reading is forwarded to Redpanda.
2. **Given** an edge device connecting without a client certificate or API key, **When** submitting telemetry to `/api/v1/telemetry` or gRPC `IngestStream`, **Then** the gateway rejects the request with HTTP 401 Unauthorized or gRPC `Unauthenticated` status code, incrementing `pulse_ingest_auth_failures_total`.
3. **Given** an authenticated edge device with identity `turbine-01`, **When** it attempts to submit a telemetry reading with `device_id: turbine-02`, **Then** the gateway detects the identity mismatch, rejects the payload with HTTP 403 Forbidden or gRPC `PermissionDenied`, logs an audit warning, and discards the record before broker publishing.
4. **Given** a resource-constrained edge device using Bearer API Key authentication over TLS, **When** submitting telemetry with a valid pre-shared key, **Then** the key is verified using constant-time cryptographic comparison (`subtle.ConstantTimeCompare`) to prevent timing side-channel attacks.

---

### User Story 2 - ChronoDB Query API Authentication & Role-Based Access Control (Priority: P1)

As a Platform Data Governance Administrator, I need ChronoDB historical time-series and rollup query endpoints (HTTP and gRPC) to enforce authentication via Bearer JWT / API tokens and evaluate fine-grained Role-Based Access Control (RBAC) permissions (`Viewer`, `Operator`, `Admin`), so that unauthorized consumers cannot inspect sensitive operational metrics, device catalogs, or internal downsampled data.

**Why this priority**: Telemetry data often contains proprietary operational IP (e.g. turbine efficiency, grid voltages, thermal anomalies). Unrestricted read access exposes historical telemetry and active device topology to unauthorized internal and external parties.

**Independent Test**: Issue query requests to `/api/v1/query`, `/api/v1/query/rollup`, and `/api/v1/devices` with tokens assigned `Viewer`, `Operator`, and unauthenticated headers; verify unauthenticated requests return 401; verify `Viewer` tokens can read time-series queries but cannot access administrative endpoints; verify role-restricted device scopes (e.g. tenant device prefix isolation) restrict queries strictly to permitted device ranges.

**Acceptance Scenarios**:
1. **Given** an unauthenticated HTTP or gRPC client, **When** querying `/api/v1/query`, `/api/v1/query/rollup`, or `/api/v1/devices`, **Then** ChronoDB responds immediately with HTTP 401 Unauthorized or gRPC `Unauthenticated`, returning a structured JSON error.
2. **Given** a client presenting a valid JWT signed by the platform issuer with role `Viewer`, **When** querying historical time series or rollups within their authorized device scope, **Then** the query executes successfully and returns requested aggregate records.
3. **Given** a client presenting a token with restricted device scope `allowed_devices: ["turbine-alpha-*"]`, **When** requesting data for `turbine-beta-01`, **Then** ChronoDB rejects the query with HTTP 403 Forbidden or gRPC `PermissionDenied`.
4. **Given** expired or signature-invalid JWT tokens, **When** querying any storage endpoint, **Then** the request is rejected with HTTP 401 Unauthorized and detailed audit log entry without leaking cryptographic secrets.

---

### User Story 3 - Decoupled Webhook Integrity & HMAC-SHA256 Signature Verification (Priority: P2)

As a Downstream Security Operations Engineer, I need all outbound incident alerts dispatched by `alertd` to include a cryptographically signed HMAC-SHA256 signature header (`X-Pulse-Signature`) and timestamp header (`X-Pulse-Timestamp`), so that receiving webhook endpoints (e.g. PagerDuty, Slack, custom incident webhooks) can verify payload authenticity, detect tampering, and reject replay attacks.

**Why this priority**: Without cryptographic authenticity verification, alert webhook endpoints are susceptible to forged incident injections or replay attacks, potentially triggering false operational shutdowns or emergency escalations.

**Independent Test**: Configure `alertd` with an HMAC secret; trigger an anomaly alert; inspect the outbound HTTP request headers to verify `X-Pulse-Signature: sha256=<hex>` and `X-Pulse-Timestamp: <unix_sec>`; calculate the expected HMAC-SHA256 of the raw body and timestamp using the secret; assert exact match; verify replay tolerance rejects requests with timestamps older than 300 seconds.

**Acceptance Scenarios**:
1. **Given** an anomaly alert dispatched by `alertd`, **When** sending the HTTP POST payload to the configured webhook destination, **Then** `alertd` computes `HMAC-SHA256(secret, timestamp + "." + body)` and attaches headers `X-Pulse-Signature` and `X-Pulse-Timestamp`.
2. **Given** an incident receiving endpoint using the Pulse Webhook Verifier, **When** evaluating incoming webhook payloads, **Then** it validates the signature in constant time and checks that the timestamp is within $\pm 300\text{ seconds}$ of current wall-clock time to prevent replay attacks.
3. **Given** a webhook payload modified in transit or signed with an incorrect secret, **When** the receiver verifies the payload, **Then** verification fails and the incident notification is dropped with an authentication error.

---

### User Story 4 - Kubernetes NetworkPolicies, Secret Injection & TLS in Transit (Priority: P2)

As a Cloud Infrastructure & Security Architect, I need the Pulse Kubernetes deployment to enforce strict default-deny `NetworkPolicy` rules, dedicated ServiceAccounts, and volume-mounted Kubernetes Secrets for sensitive credentials (signing keys, TLS certificates, broker credentials), so that inter-service communication is locked down, compromised pods cannot traverse the internal network, and plaintext secrets are absent from Git and ConfigMaps.

**Why this priority**: Defense-in-depth requires that microservice boundaries are enforced at the network layer. If a pod is compromised, network policies prevent unauthorized lateral movement (e.g., reaching the database or broker directly).

**Independent Test**: Deploy the Helm chart with `security.networkPolicies.enabled=true`; test pod connectivity to verify that only `gateway` and `processor` can communicate with Redpanda; verify `alertd` cannot be accessed from public ingress; verify `chronod` is accessible only by authorized query clients; inspect all container specifications and verify zero plaintext secrets in environment variables or manifests.

**Acceptance Scenarios**:
1. **Given** Kubernetes `NetworkPolicy` objects enabled in the Helm chart, **When** inspecting cluster traffic rules, **Then** default-deny ingress is enforced for all namespaces, with explicit ingress rules allowing only designated service-to-service communication paths.
2. **Given** sensitive configuration (mTLS CA certificates, HMAC webhook signing keys, JWT secrets), **When** deploying via Helm, **Then** credentials are provisioned via Kubernetes `Secret` objects and mounted as read-only files (`mode: 0400`) rather than embedded in ConfigMaps or plaintext environment variables.
3. **Given** all 5 microservice containers running under distroless non-root security contexts (`UID 65532`, `readOnlyRootFilesystem: true`, `drop: ["ALL"]`), **When** secret volumes are mounted, **Then** permissions allow strictly non-root read access without privilege escalation.

---

## Edge Cases

- **Certificate Expiration & Graceful Rotation**: When edge client certificates or server TLS certificates expire, connections must fail with distinct TLS alert errors without crashing the gateway. The gateway must support hot-reloading CA certificates from disk without dropping active connections.
- **Clock Skew on Replay Protection**: When validating webhook timestamps or JWT `iat`/`exp` claims, the system must allow a configurable clock skew window ($\pm 60\text{s}$) to avoid false-positive rejections due to minor NTP drift across distributed nodes.
- **Constant-Time Verification Against Timing Attacks**: All API key, token hash, and HMAC signature comparisons must use constant-time algorithms (`crypto/subtle.ConstantTimeCompare`) to ensure comparison duration does not leak secret bytes.
- **High-Throughput Hot-Path Rate Limiting**: If an authenticated edge device goes rogue and emits 100,000 requests/sec, the token-bucket rate limiter must reject excess traffic with HTTP 429 Too Many Requests in $< 0.1\text{ms}$ without degrading throughput for other concurrent partitions or acquiring global mutexes (Constitution Principle I).
- **Corrupted or Malformed Authentication Headers**: Malformed `Authorization` headers (e.g. non-ASCII characters, truncation, missing `Bearer ` prefix) must return 401 with sanitized error descriptions without panicking or triggering memory allocations.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Ingestion Gateway MUST support Mutual TLS (mTLS) authentication with configurable trusted CA certificates for edge client authentication.
- **FR-002**: Ingestion Gateway MUST verify that the authenticated client certificate identity (CN or SAN) matches the telemetry payload's `device_id` (anti-spoofing enforcement).
- **FR-003**: Ingestion Gateway MUST support pre-shared API Key authentication over TLS for edge devices, verified using constant-time comparison.
- **FR-004**: Ingestion Gateway MUST reject all unauthenticated and spoofed requests with HTTP 401/403 or gRPC `Unauthenticated`/`PermissionDenied` before publishing to message brokers.
- **FR-005**: Ingestion Gateway MUST enforce token-bucket rate limiting per authenticated device or API key, responding with HTTP 429 when limits are exceeded.
- **FR-006**: ChronoDB HTTP and gRPC query servers MUST require Bearer JWT or API token authentication on `/api/v1/query`, `/api/v1/query/rollup`, and `/api/v1/devices`.
- **FR-007**: ChronoDB query services MUST evaluate Role-Based Access Control (RBAC) claims (`Viewer`, `Operator`, `Admin`), denying unauthorized endpoints with 403 Forbidden.
- **FR-008**: ChronoDB query services MUST support device scope restrictions in token claims, preventing clients from querying device IDs outside their authorized scope.
- **FR-009**: Alert Dispatcher (`alertd`) MUST generate cryptographic HMAC-SHA256 signatures for all outbound webhook requests, attached via `X-Pulse-Signature` and `X-Pulse-Timestamp` headers.
- **FR-010**: Alert Dispatcher MUST support replay attack prevention by including a signed epoch timestamp, allowing receivers to reject requests older than 300 seconds.
- **FR-011**: Pulse platform MUST provide an SDK / middleware helper for downstream webhook receivers to verify HMAC signatures and replay timestamps.
- **FR-012**: Helm chart MUST provide Kubernetes `NetworkPolicy` manifests enforcing default-deny ingress and least-privilege traffic flow between gateway, broker, processor, alertd, and chronod.
- **FR-013**: Helm chart MUST manage all TLS certificates, JWT secrets, and HMAC keys as Kubernetes `Secret` resources mounted read-only (`mode: 0400`) to non-root containers (`UID 65532`).
- **FR-014**: All services MUST emit structured security audit log events for authentication failures, permission denials, and spoofing attempts without logging raw secrets.
- **FR-015**: Gateway, Processor, Alertd, and ChronoDB MUST export Prometheus metrics for authentication outcomes (`pulse_ingest_auth_successes_total`, `pulse_ingest_auth_failures_total`, `pulse_storage_auth_failures_total`).

---

### Key Entities

- **DeviceIdentity**: Cryptographic identity extracted from client certificate Subject Common Name (CN), Subject Alternative Name (SAN), or verified API key claims.
- **AuthContext**: Request-scoped security context carrying authenticated caller identity, tenant, assigned roles (`Viewer`, `Operator`, `Admin`), and permitted device scopes.
- **TokenClaims**: Standardized JWT claims (`sub`, `role`, `scopes`, `iss`, `exp`, `iat`, `allowed_devices`).
- **WebhookSignaturePayload**: Canonical string format `timestamp + "." + raw_body` used as input for HMAC-SHA256 computation.
- **SecurityAuditRecord**: Structured audit event recording timestamp, client IP, authenticated identity, requested URI/method, outcome (`ALLOWED`, `DENIED`), and failure reason.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of unauthenticated or identity-mismatched ingestion requests are rejected at the gateway perimeter before reaching the Redpanda broker hot path.
- **SC-002**: Authentication verification overhead at the Ingestion Gateway adds $< 0.5\text{ms}$ (p95) to request processing latency under sustained 50,000 events/sec throughput.
- **SC-003**: 100% of ChronoDB query requests without valid credentials or outside permitted role scopes are rejected with HTTP 401 or 403.
- **SC-004**: Webhook receiver HMAC-SHA256 signature verification detects 100% of payload tampering, mismatched secrets, and replayed requests older than 300 seconds.
- **SC-005**: Kubernetes `NetworkPolicy` enforcement blocks 100% of unauthorized lateral traffic between non-communicating service pods.
- **SC-006**: Zero plaintext secrets exist in repository source code, Docker images, Helm ConfigMaps, or version-controlled manifests.

---

## Assumptions

- Edge device public keys or pre-shared keys are provisioned through an existing device management lifecycle; automated device certificate enrollment (e.g. SCEP / EST) is handled out-of-band.
- For local testing and development environments, a local CA and self-signed certificate generation script (`scripts/generate_certs.sh`) is provided to streamline integration testing.
- ChronoDB JWT tokens use standard HMAC-SHA256 (`HS256`) or RSA/ECDSA (`RS256`/`ES256`) signing with pre-shared keys or public key certificates.
- Internal Redpanda broker traffic in production leverages SASL/SCRAM or mTLS, while local docker-compose retains plaintext listeners for rapid development.

