# Contract: Authentication, Authorization & Cryptographic Signing Interfaces

**Feature**: `009-security-access-control`  
**Date**: 2026-10-29  

---

## 1. GatewayAuthenticator (Edge Perimeter Authentication & Anti-Spoofing)

```go
package auth

import (
    "context"
    "net/http"
)

// GatewayAuthenticator authenticates edge clients connecting over HTTP or gRPC
// and verifies that the client identity matches the payload device_id.
type GatewayAuthenticator interface {
    // AuthenticateHTTP inspects TLS client certificates (mTLS) or the Authorization header.
    AuthenticateHTTP(r *http.Request) (*DeviceIdentity, error)

    // AuthenticateGRPC extracts peer certificates from gRPC credentials or metadata.
    AuthenticateGRPC(ctx context.Context) (*DeviceIdentity, error)

    // ValidateDeviceBinding enforces anti-spoofing: identity.DeviceID MUST match claimedDeviceID.
    ValidateDeviceBinding(identity *DeviceIdentity, claimedDeviceID string) error
}
```

---

## 2. TokenAuthorizer (ChronoDB Query Authentication & RBAC)

```go
package auth

import (
    "context"
    "net/http"
)

// TokenAuthorizer validates Bearer JWT tokens and evaluates RBAC roles and device scope boundaries.
type TokenAuthorizer interface {
    // AuthorizeHTTP validates the HTTP Authorization header against the required role and device scope.
    AuthorizeHTTP(r *http.Request, requiredRole Role, targetDeviceID string) (*AuthContext, error)

    // AuthorizeGRPC validates gRPC metadata authorization against the required role and device scope.
    AuthorizeGRPC(ctx context.Context, requiredRole Role, targetDeviceID string) (*AuthContext, error)
}
```

---

## 3. WebhookSigner & WebhookVerifier (Decoupled Alert Dispatcher)

```go
package auth

import "time"

// WebhookSigner generates cryptographic HMAC-SHA256 headers for outbound incident alerts.
type WebhookSigner interface {
    // Sign generates the canonical HMAC-SHA256 signature string for timestamp + "." + body.
    Sign(timestamp time.Time, body []byte) (signature string, timestampStr string)
}

// WebhookVerifier validates incoming alert notifications on webhook receiver endpoints.
type WebhookVerifier interface {
    // Verify evaluates the X-Pulse-Signature and X-Pulse-Timestamp headers against the raw body.
    // Defends against timing attacks via constant-time comparison and rejects replay attacks older than maxClockSkew.
    Verify(signatureHeader, timestampHeader string, body []byte, maxClockSkew time.Duration) (bool, error)
}
```

---

## 4. RateLimiter (Perimeter Ingestion Hot-Path Defense)

```go
package auth

import "time"

// RateLimiter enforces token-bucket rate limiting per authenticated device or IP.
type RateLimiter interface {
    // Allow checks whether a request for key (device_id or client_ip) is permitted under rate limits.
    // Returns allowed boolean and retry duration if rate limited.
    Allow(key string) (allowed bool, retryAfter time.Duration)
}
```

