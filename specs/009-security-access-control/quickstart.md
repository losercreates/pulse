# Quickstart: Milestone 009 — Security & Access Control Validation Guide

This guide demonstrates how to validate the end-to-end security architecture of Pulse, including mTLS, API key validation, device identity anti-spoofing, ChronoDB query RBAC, and webhook HMAC signature verification.

---

## 1. Prerequisites & Test Key Generation

Generate a local test CA and client/server certificates using the provided helper:

```bash
# Generate test Root CA, Gateway server certificate, and client certificates for turbine-01 and turbine-02
./scripts/generate_certs.sh
```

This creates:
- `certs/ca.crt`, `certs/ca.key` (Root CA)
- `certs/gateway.crt`, `certs/gateway.key` (Gateway Server TLS)
- `certs/turbine-01.crt`, `certs/turbine-01.key` (Client mTLS Cert with CN=turbine-01)
- `certs/turbine-02.crt`, `certs/turbine-02.key` (Client mTLS Cert with CN=turbine-02)

---

## 2. Edge Device Ingestion Authentication & Anti-Spoofing

### Test Scenario 1A: Valid mTLS Ingestion (Expected: 200 OK)
Submit telemetry using the client certificate for `turbine-01` where the payload matches:

```bash
curl -X POST https://localhost:8080/api/v1/telemetry \
  --cacert certs/ca.crt \
  --cert certs/turbine-01.crt \
  --key certs/turbine-01.key \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "turbine-01",
    "metric_name": "bearing_temp",
    "value": 72.5,
    "timestamp_ns": 1730000000000000000
  }'
# Output: HTTP 200 OK {"status": "SUCCESS", "receipt": {...}}
```

### Test Scenario 1B: Device Identity Spoofing Rejection (Expected: 403 Forbidden)
Submit telemetry using `turbine-01` client certificate, but falsely asserting `device_id: turbine-02`:

```bash
curl -X POST https://localhost:8080/api/v1/telemetry \
  --cacert certs/ca.crt \
  --cert certs/turbine-01.crt \
  --key certs/turbine-01.key \
  -H "Content-Type: application/json" \
  -d '{
    "device_id": "turbine-02",
    "metric_name": "bearing_temp",
    "value": 99.9,
    "timestamp_ns": 1730000000000000000
  }'
# Output: HTTP 403 Forbidden {"status": "REJECTED", "error": "device_id spoofing detected: certificate CN does not match payload device_id"}
```

### Test Scenario 1C: Unauthenticated Perimeter Rejection (Expected: 401 Unauthorized)
Submit telemetry without client certificates or headers:

```bash
curl -X POST https://localhost:8080/api/v1/telemetry \
  --cacert certs/ca.crt \
  -H "Content-Type: application/json" \
  -d '{"device_id":"turbine-01","metric_name":"temp","value":25.0}'
# Output: HTTP 401 Unauthorized {"status": "REJECTED", "error": "client authentication required"}
```

### Test Scenario 1D: Valid API Key Ingestion (Expected: 200 OK)
Submit telemetry with a pre-shared bearer API key:

```bash
curl -X POST https://localhost:8080/api/v1/telemetry \
  --cacert certs/ca.crt \
  -H "Authorization: Bearer dev-key-turbine-01-secret" \
  -H "Content-Type: application/json" \
  -d '{"device_id":"turbine-01","metric_name":"temp","value":25.0,"timestamp_ns":1730000000000000000}'
# Output: HTTP 200 OK
```

---

## 3. ChronoDB Query API Authentication & RBAC

### Test Scenario 2A: Query with Valid Viewer Role Token (Expected: 200 OK)
```bash
# Generate test JWT with role: Viewer, allowed_devices: ["turbine-01*"]
TOKEN=$(./bin/pulse-auth token --sub analyst@pulse --role Viewer --devices "turbine-01*")

curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8084/api/v1/query?device_id=turbine-01&metric_name=bearing_temp&start_time=0&end_time=2000000000000000000"
# Output: HTTP 200 OK {"device_id": "turbine-01", "records": [...]}
```

### Test Scenario 2B: Query Unauthorized Device Outside Token Scope (Expected: 403 Forbidden)
```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8084/api/v1/query?device_id=generator-09&metric_name=voltage&start_time=0&end_time=2000000000000000000"
# Output: HTTP 403 Forbidden {"error": "access denied: device_id outside authorized token scope"}
```

---

## 4. Webhook Integrity & HMAC-SHA256 Verification

Inspect an outbound alert dispatched by `alertd` to a test receiver:

```bash
# Verify headers on incoming webhook
# Headers:
#   X-Pulse-Signature: sha256=4f6a...
#   X-Pulse-Timestamp: 1730000000

# Webhook Verifier test:
go test -v ./internal/alert/sink -run TestWebhookSink_HMACSignatureVerification
# Output: PASS (verifies payload tampering detection and 300s replay rejection)
```

---

## 5. Kubernetes Helm Packaging & NetworkPolicy Validation

Validate that NetworkPolicy manifests render with zero syntax errors:

```bash
helm template pulse deployments/helm/pulse --set security.networkPolicies.enabled=true | grep -A 10 "kind: NetworkPolicy"
```

