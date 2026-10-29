package auth

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// GatewayHTTPMiddleware creates an HTTP middleware enforcing authentication and rate limiting.
func GatewayHTTPMiddleware(
	authenticator GatewayAuthenticator,
	rateLimiter RateLimiter,
	audit AuditLogger,
) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			clientIP := r.RemoteAddr

			// Authenticate
			identity, err := authenticator.AuthenticateHTTP(r)
			if err != nil {
				if audit != nil {
					audit.LogAuthFailure(clientIP, "HTTP", err.Error(), http.StatusUnauthorized, 16) // Unauthenticated
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"status": "UNAUTHORIZED",
					"error":  "authentication required: " + err.Error(),
				})
				return
			}

			// Rate Limit per device
			if rateLimiter != nil {
				allowed, retryAfter := rateLimiter.Allow(identity.DeviceID)
				if !allowed {
					if audit != nil {
						audit.LogRateLimited(clientIP, identity.DeviceID)
					}
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
					w.WriteHeader(http.StatusTooManyRequests)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"status": "RATE_LIMITED",
						"error":  "per-device rate limit exceeded",
					})
					return
				}
			}

			// Store identity in context
			ctx := WithDeviceIdentity(r.Context(), identity)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RejectSpoofedHTTP logs and writes a 403 Forbidden response when device anti-spoofing fails.
func RejectSpoofedHTTP(
	w http.ResponseWriter,
	r *http.Request,
	audit AuditLogger,
	claimedDeviceID, certDeviceID string,
	err error,
) {
	if audit != nil {
		audit.LogSpoofingAttempt(r.RemoteAddr, "HTTP", claimedDeviceID, certDeviceID, http.StatusForbidden, 7) // PermissionDenied
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "REJECTED",
		"error":  err.Error(),
	})
}

