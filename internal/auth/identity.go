package auth

import (
	"context"
	"errors"
	"path"
	"strings"
	"time"
)

// Common security error sentinels.
var (
	ErrUnauthenticated       = errors.New("authentication required")
	ErrForbidden             = errors.New("access forbidden: insufficient permissions")
	ErrSpoofedIdentity       = errors.New("device identity anti-spoofing violation: payload device_id does not match authenticated identity")
	ErrInvalidSignature      = errors.New("invalid cryptographic signature")
	ErrTokenExpired          = errors.New("token has expired")
	ErrTokenNotYetValid      = errors.New("token is not yet valid")
	ErrInvalidToken          = errors.New("invalid authentication token")
	ErrRateLimitExceeded     = errors.New("rate limit exceeded")
	ErrDeviceScopeForbidden  = errors.New("access to requested device_id is outside authorized scope")
	ErrMissingCredentials    = errors.New("missing credentials")
	ErrInvalidWebhookHeaders = errors.New("missing or malformed webhook signature headers")
	ErrWebhookReplay         = errors.New("webhook timestamp outside allowed clock skew window")
)

// Role represents an authorization tier in ChronoDB.
type Role string

const (
	RoleViewer   Role = "Viewer"   // Read historical telemetry & catalog
	RoleOperator Role = "Operator" // Read rollups, operational metrics & Viewer actions
	RoleAdmin    Role = "Admin"    // Administrative actions, compaction & Operator actions
)

// RoleHierarchy defines the permission rank: Admin > Operator > Viewer.
var roleHierarchy = map[Role]int{
	RoleViewer:   1,
	RoleOperator: 2,
	RoleAdmin:    3,
}

// IsValid returns true if the role is one of the recognized roles.
func (r Role) IsValid() bool {
	_, ok := roleHierarchy[r]
	return ok
}

// Satisfies returns true if role r meets or exceeds requiredRole.
func (r Role) Satisfies(required Role) bool {
	rankR, okR := roleHierarchy[r]
	rankReq, okReq := roleHierarchy[required]
	if !okR || !okReq {
		return false
	}
	return rankR >= rankReq
}

// DeviceIdentity represents an edge device identity extracted during TLS handshake or API key validation.
type DeviceIdentity struct {
	DeviceID        string    `json:"device_id"`
	AuthMethod      string    `json:"auth_method"` // "mTLS" or "APIKey"
	Issuer          string    `json:"issuer,omitempty"`
	Subject         string    `json:"subject,omitempty"`
	IssuedAt        time.Time `json:"issued_at,omitempty"`
	ExpiresAt       time.Time `json:"expires_at,omitempty"`
	RateLimitTokens int       `json:"rate_limit_tokens,omitempty"`
}

// AuthContext represents request-scoped identity and authorization details attached to context.
type AuthContext struct {
	CallerID        string         `json:"caller_id"`
	AuthType        string         `json:"auth_type"` // "mTLS", "APIKey", or "JWT"
	Role            Role           `json:"role"`
	AllowedDevices  []string       `json:"allowed_devices,omitempty"`
	Claims          map[string]any `json:"claims,omitempty"`
	AuthenticatedAt time.Time      `json:"authenticated_at"`
}

// HasAccessToDevice checks if the AuthContext permits operations for the given device ID.
// If AllowedDevices is empty or contains "*", access is granted to all devices.
// Patterns support globbing via path.Match (e.g., "turbine-alpha-*").
func (ac *AuthContext) HasAccessToDevice(deviceID string) bool {
	if ac == nil {
		return false
	}
	if len(ac.AllowedDevices) == 0 {
		return true
	}
	for _, pattern := range ac.AllowedDevices {
		if pattern == "*" || pattern == deviceID {
			return true
		}
		matched, err := path.Match(pattern, deviceID)
		if err == nil && matched {
			return true
		}
		// Also support simple prefix matching with wildcard if path.Match failed
		if strings.HasSuffix(pattern, "*") && strings.HasPrefix(deviceID, strings.TrimSuffix(pattern, "*")) {
			return true
		}
	}
	return false
}

// TokenClaims represents standard and Pulse-specific JWT claims.
type TokenClaims struct {
	Issuer         string   `json:"iss,omitempty"`
	Subject        string   `json:"sub,omitempty"`
	Audience       string   `json:"aud,omitempty"`
	Role           Role     `json:"role,omitempty"`
	AllowedDevices []string `json:"allowed_devices,omitempty"`
	ExpiresAt      int64    `json:"exp,omitempty"`
	NotBefore      int64    `json:"nbf,omitempty"`
	IssuedAt       int64    `json:"iat,omitempty"`
	ID             string   `json:"jti,omitempty"`
}

// Valid validates temporal claims against the provided time.
func (c *TokenClaims) Valid(now time.Time) error {
	nowUnix := now.Unix()
	if c.ExpiresAt > 0 && nowUnix > c.ExpiresAt {
		return ErrTokenExpired
	}
	if c.NotBefore > 0 && nowUnix < c.NotBefore {
		return ErrTokenNotYetValid
	}
	return nil
}

// Context keys
type contextKey string

const (
	deviceIdentityKey contextKey = "pulse_device_identity"
	authContextKey    contextKey = "pulse_auth_context"
)

// WithDeviceIdentity stores the DeviceIdentity in context.
func WithDeviceIdentity(ctx context.Context, id *DeviceIdentity) context.Context {
	return context.WithValue(ctx, deviceIdentityKey, id)
}

// FromDeviceIdentity retrieves the DeviceIdentity from context.
func FromDeviceIdentity(ctx context.Context) (*DeviceIdentity, bool) {
	val, ok := ctx.Value(deviceIdentityKey).(*DeviceIdentity)
	return val, ok && val != nil
}

// WithAuthContext stores the AuthContext in context.
func WithAuthContext(ctx context.Context, ac *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey, ac)
}

// FromAuthContext retrieves the AuthContext from context.
func FromAuthContext(ctx context.Context) (*AuthContext, bool) {
	val, ok := ctx.Value(authContextKey).(*AuthContext)
	return val, ok && val != nil
}

