# Data Model: Milestone 009 — Security & Access Control

**Feature**: `009-security-access-control`  
**Date**: 2026-10-29  
**Status**: Completed  

---

## 1. Security Context & Identity Data Structures

### DeviceIdentity
Extracted during TLS handshake (from X.509 client certificate) or from validated API keys:

```go
type DeviceIdentity struct {
    DeviceID        string    // Canonical device identifier (e.g. "turbine-01")
    AuthMethod      string    // "mTLS" or "APIKey"
    Issuer          string    // Certificate Authority Common Name or Key Issuer
    Subject         string    // Certificate Subject DN or API Key Subject
    IssuedAt        time.Time // Certificate/Key issuance time
    ExpiresAt       time.Time // Certificate/Key expiration time
    RateLimitTokens int       // Configured requests/sec burst limit
}
```

### AuthContext (Request-Scoped Identity & Permissions)
Attached to the `context.Context` of every authenticated HTTP or gRPC request:

```go
type AuthContext struct {
    CallerID       string            // Authenticated caller (device ID, user email, or service account)
    AuthType       string            // "mTLS", "APIKey", or "JWT"
    Role           Role              // RoleViewer, RoleOperator, or RoleAdmin
    AllowedDevices []string          // Glob patterns for accessible devices (e.g. ["turbine-alpha-*"])
    Claims         map[string]any    // Additional token claims
    AuthenticatedAt time.Time        // Verification timestamp
}

type Role string

const (
    RoleViewer   Role = "Viewer"   // Read historical telemetry & catalog
    RoleOperator Role = "Operator" // Read rollups & operational metrics
    RoleAdmin    Role = "Admin"    // Administrative actions, schema, compaction
)
```

### TokenClaims (ChronoDB Query JWT Format)
Standard JWT claims payload with custom Pulse authorization claims:

```json
{
  "iss": "pulse-auth",
  "sub": "analyst@pulse.internal",
  "aud": "pulse-chronodb",
  "role": "Viewer",
  "allowed_devices": [
    "turbine-alpha-*",
    "generator-01"
  ],
  "exp": 1730003600,
  "nbf": 1730000000,
  "iat": 1730000000,
  "jti": "c2b5d4a1-8e9f-4b3c-9a1d-7e8f2a3b4c5d"
}
```

### WebhookSignaturePayload (Alert Dispatcher)
The canonical byte representation signed with HMAC-SHA256:

```text
Canonical String = {X-Pulse-Timestamp} + "." + {Raw_JSON_Body}
Signature        = "sha256=" + HexEncode(HMAC-SHA256(secret, Canonical String))
```

Example HTTP Headers:
```http
POST /api/v1/incidents HTTP/1.1
Host: incident-receiver.internal
Content-Type: application/json
X-Pulse-Signature: sha256=a8f23c914e7a688b1b22998a445ec92fbc789bfa816e87d6e5a4d3c2b1a0f9e8
X-Pulse-Timestamp: 1730000000
Content-Length: 342

{"incident_id":"inc-001","device_id":"turbine-01","metric":"bearing_temp",...}
```

### SecurityAuditRecord
Structured audit event recorded when security decisions, rate limit events, or violations occur:

```json
{
  "timestamp": "2026-10-29T03:45:00.123456Z",
  "event_type": "AUTH_FAILURE",
  "service": "pulse-gateway",
  "client_ip": "10.244.1.45:51234",
  "auth_method": "mTLS",
  "claimed_device_id": "turbine-02",
  "cert_device_id": "turbine-01",
  "outcome": "DENIED",
  "reason": "device_id spoofing attempt: certificate identity does not match payload",
  "http_status": 403,
  "grpc_code": 7
}
```

---

## 2. On-Disk Secrets & File Layout

When mounted from Kubernetes Secrets into distroless containers (`readOnlyRootFilesystem: true`, non-root user `UID 65532`):

```text
/etc/pulse/secrets/
├── tls/
│   ├── ca.crt                  # Trusted Certificate Authority bundle (0444)
│   ├── server.crt              # Microservice TLS server certificate (0444)
│   └── server.key              # Microservice TLS private key (0400)
├── apikeys/
│   └── device_keys.json        # Pre-shared edge device API key mapping (0400)
├── jwt/
│   └── jwt_secret.key          # HMAC or RSA/ECDSA verification key (0400)
└── webhook/
    └── hmac_signing_secret     # Webhook outbound signing secret (0400)
```

---

## 3. Kubernetes NetworkPolicy Traffic Matrix

```text
+-------------------+--------------------+------------------------+------------------------------------+
| Source Service    | Destination        | Allowed Ports          | Purpose                            |
+-------------------+--------------------+------------------------+------------------------------------+
| External Ingress  | pulse-gateway      | 8080 (HTTP), 50051 (gRPC)| Ingest edge sensor telemetry      |
| pulse-gateway     | redpanda           | 9092 (Kafka)           | Publish raw telemetry              |
| pulse-processor   | redpanda           | 9092 (Kafka)           | Consume raw, publish agg/alerts    |
| pulse-alertd      | redpanda           | 9092 (Kafka)           | Consume anomaly alerts             |
| pulse-alertd      | External / Egress  | 443 / 80 (HTTPS/HTTP)  | Dispatch signed webhook alerts     |
| pulse-chronod     | redpanda           | 9092 (Kafka)           | Consume windowed aggregates        |
| Query Clients     | pulse-chronod      | 8084 (HTTP), 50054 (gRPC)| Historical time-series queries    |
| Prometheus Scraper| All Pods           | Metrics Ports          | Scrape /metrics endpoints          |
+-------------------+--------------------+------------------------+------------------------------------+
```
*All other inter-service egress and ingress traffic is blocked by default.*

