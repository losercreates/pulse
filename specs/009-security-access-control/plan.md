# Implementation Plan: Milestone 009 — Security & Access Control

**Branch**: `009-security-access-control` | **Date**: 2026-10-29 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/009-security-access-control/spec.md`

---

## Summary

Implement an enterprise-grade security and access control subsystem for Pulse conforming strictly to Constitution Principles I–V:
1. **Edge Perimeter Authentication & Anti-Spoofing**: Enforce Mutual TLS (mTLS) with X.509 client certificate verification and constant-time pre-shared API keys on Ingestion Gateway HTTP (`:8080`) and gRPC (`:50051`). Cryptographically bind authenticated client identity (CN/SAN) to `reading.device_id`, instantly rejecting unauthenticated or spoofed telemetry before message broker publication.
2. **ChronoDB Query API Authentication & RBAC**: Protect historical time-series and rollup endpoints (`:8084`, `:50054`) with Bearer JWT tokens, evaluating `Viewer`, `Operator`, and `Admin` role scopes and device tenant isolation patterns.
3. **Decoupled Webhook Integrity (HMAC-SHA256)**: Sign all outbound alert notifications from `alertd` with `X-Pulse-Signature` and `X-Pulse-Timestamp` headers, providing replay attack defense ($\pm 300\text{s}$) and constant-time verification.
4. **Kubernetes NetworkPolicies & Secret Management**: Provide default-deny inter-service NetworkPolicies in the Helm chart, and transition credentials to read-only Kubernetes Secrets (`mode: 0400`) mounted to non-root distroless containers (`UID 65532`).

---

## Technical Context

**Language/Version**: Go 1.26+ (pure Go, zero CGo, static compilation `CGO_ENABLED=0`).

**Primary Dependencies**: Standard library (`crypto/tls`, `crypto/hmac`, `crypto/sha256`, `crypto/subtle`, `encoding/base64`, `encoding/json`), Google gRPC (`google.golang.org/grpc`, `credentials`), and Prometheus (`github.com/prometheus/client_golang`).

**Storage**: In-memory read-only credentials cache; Kubernetes Secrets mounted at `/etc/pulse/secrets/`.

**Testing**: Go unit tests with `-race`, security penetration/spoofing tests (`TestGateway_AntiSpoofingValidation`), timing side-channel verification, and Helm NetworkPolicy template testing.

**Target Platform**: Linux containers (`gcr.io/distroless/static-debian12:nonroot`, UID `65532:65532`) and Kubernetes 1.28+.

**Project Type**: Microservices perimeter security, gRPC/HTTP interceptors, and Kubernetes security manifests.

**Performance Goals**: $< 0.5\text{ms}$ authentication overhead at 50,000 events/sec throughput; $< 0.05\text{ms}$ rate-limiting overhead.

**Constraints**: Zero-lock hot-path ingestion (Principle I), zero CGo dependencies, zero plaintext secrets in Git/ConfigMaps, and constant-time comparisons.

**Scale/Scope**: Supporting up to 10,000 concurrent edge devices and 100,000 req/sec total perimeter throughput.

---

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Compliance Status | Analysis & Evidence |
|---|:---:|---|
| **I. Partition-Local State & Zero-Lock Ingestion** | ✅ PASS | Gateway authentication utilizes connection-level TLS state and atomic pre-loaded key mappings. Verification involves zero cross-goroutine locks or global mutexes on the ingestion path. |
| **II. Event-Time Watermarking** | ✅ PASS | Security filters unauthenticated and spoofed readings at the perimeter before event timestamps enter watermark calculation. |
| **III. Idempotent Ingestion & At-Least-Once Delivery** | ✅ PASS | Authenticated readings forwarded to Redpanda retain deterministic primary composite keys `(device_id, metric, window_start, window_end)`. |
| **IV. Decoupled Asynchronous Side-Effects** | ✅ PASS | Webhook HMAC signing executes inside the isolated `alertd` worker pool; signing operations never backpressure stream ingestion. |
| **V. Explicit Polyglot Storage Boundary** | ✅ PASS | ChronoDB query token authorization occurs at the REST/gRPC boundary before passing query ranges to the storage engine. |

---

## Project Structure

### Documentation (this feature)

```text
specs/009-security-access-control/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output: mTLS, JWT RBAC, HMAC signing & NetworkPolicies
├── data-model.md        # Phase 1 output: Security context, token claims, audit records
├── quickstart.md        # Phase 1 output: Validation guide, cert generation, curl & gRPC tests
├── contracts/           # Phase 1 output: Auth contracts & NetworkPolicy specifications
│   ├── auth-contract.md
│   └── networkpolicy-contract.md
├── checklists/
│   └── requirements.md  # Specification quality checklist
└── tasks.md             # Phase 2 output (/speckit-tasks command - generated next)
```

### Source Code Layout

```text
internal/auth/
├── identity.go          # DeviceIdentity, AuthContext, and Role definitions
├── mtls.go              # mTLS certificate parser, SAN/CN extractor, and CA pool loader
├── apikey.go            # Pre-shared API key validator with subtle.ConstantTimeCompare
├── jwt.go               # Pure Go JWT validator, claims parser, and RBAC evaluator
├── ratelimit.go         # Token-bucket rate limiter for perimeter DoS mitigation
├── webhook.go           # HMAC-SHA256 signer and constant-time verifier with replay protection
├── http_middleware.go   # HTTP authentication & anti-spoofing middleware
├── grpc_interceptor.go  # gRPC unary and stream authentication interceptors
└── auth_test.go         # Comprehensive unit tests for mTLS, JWT, HMAC, and anti-spoofing

internal/gateway/
├── config/config.go     # Added TLS, mTLS CA, API keys, and rate limit configuration
├── server/http.go       # Integrated HTTP authentication & rate-limiting middleware
└── server/grpc.go       # Integrated gRPC auth interceptor & device binding checks

internal/storage/
├── config/config.go     # Added JWT secret and RBAC configuration
└── server/http.go       # Integrated ChronoDB query JWT authentication & RBAC middleware

internal/alert/
├── config/config.go     # Added HMAC webhook signing secret
└── sink/webhook.go      # Integrated outbound X-Pulse-Signature & X-Pulse-Timestamp headers

deployments/helm/pulse/
├── values.yaml          # Added security: tls, mtls, auth, and networkPolicies parameters
└── templates/
    ├── networkpolicy.yaml # Default-deny ingress & least-privilege service NetworkPolicies
    └── secrets.yaml       # Kubernetes Secret templates for certs, keys, and tokens

scripts/
└── generate_certs.sh    # Script for local test CA, server, and client mTLS certificate generation
```

---

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| Pure Go JWT Parser | Zero external dependencies (`CGO_ENABLED=0`, minimal attack surface) | External JWT libraries often introduce transitive dependencies or complex verification options prone to `alg: none` exploits. |
| In-Memory Rate Limiting | Hot-path DoS protection without external Redis | External cache introduces network hop on 50k ev/s hot path, violating Principle I. |
