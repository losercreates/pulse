package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
)

// Supported signing algorithms.
const (
	AlgHS256 = "HS256"
	AlgEdDSA = "EdDSA"
)

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// JWTValidator validates JWT tokens using HMAC-SHA256 (HS256) or Ed25519 (EdDSA).
type JWTValidator struct {
	hs256Secret      []byte
	ed25519PubKey    ed25519.PublicKey
	expectedIssuer   string
	expectedAudience string
	clockSkew        time.Duration
}

// NewHS256Validator creates a validator verifying HS256-signed tokens.
func NewHS256Validator(secret []byte, expectedIssuer, expectedAudience string) *JWTValidator {
	return &JWTValidator{
		hs256Secret:      secret,
		expectedIssuer:   expectedIssuer,
		expectedAudience: expectedAudience,
		clockSkew:        time.Minute,
	}
}

// NewEd25519Validator creates a validator verifying Ed25519-signed tokens.
func NewEd25519Validator(pubKey ed25519.PublicKey, expectedIssuer, expectedAudience string) *JWTValidator {
	return &JWTValidator{
		ed25519PubKey:    pubKey,
		expectedIssuer:   expectedIssuer,
		expectedAudience: expectedAudience,
		clockSkew:        time.Minute,
	}
}

// SetClockSkew configures the allowed clock drift tolerance.
func (v *JWTValidator) SetClockSkew(d time.Duration) {
	v.clockSkew = d
}

// SignHS256 creates a signed JWT string using HS256.
func SignHS256(claims TokenClaims, secret []byte) (string, error) {
	header := jwtHeader{Alg: AlgHS256, Typ: "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := headerB64 + "." + claimsB64

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := mac.Sum(nil)
	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return signingInput + "." + sigB64, nil
}

// SignEd25519 creates a signed JWT string using Ed25519.
func SignEd25519(claims TokenClaims, privKey ed25519.PrivateKey) (string, error) {
	header := jwtHeader{Alg: AlgEdDSA, Typ: "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := headerB64 + "." + claimsB64

	sig := ed25519.Sign(privKey, []byte(signingInput))
	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return signingInput + "." + sigB64, nil
}

// Validate parses and validates a JWT token string.
func (v *JWTValidator) Validate(tokenStr string) (*TokenClaims, error) {
	if tokenStr == "" {
		return nil, ErrMissingCredentials
	}

	// Strip "Bearer " prefix if present
	if strings.HasPrefix(strings.ToLower(tokenStr), "bearer ") {
		tokenStr = strings.TrimSpace(tokenStr[7:])
	}

	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: malformed token structure", ErrInvalidToken)
	}

	headerB64, payloadB64, sigB64 := parts[0], parts[1], parts[2]

	headerBytes, err := decodeBase64URL(headerB64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid header encoding", ErrInvalidToken)
	}

	var hdr jwtHeader
	if err := json.Unmarshal(headerBytes, &hdr); err != nil {
		return nil, fmt.Errorf("%w: invalid header json", ErrInvalidToken)
	}

	// Strictly reject 'none' algorithm
	if strings.ToLower(hdr.Alg) == "none" {
		return nil, fmt.Errorf("%w: 'none' algorithm forbidden", ErrInvalidSignature)
	}

	sigBytes, err := decodeBase64URL(sigB64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid signature encoding", ErrInvalidToken)
	}

	signingInput := headerB64 + "." + payloadB64

	switch hdr.Alg {
	case AlgHS256:
		if len(v.hs256Secret) == 0 {
			return nil, fmt.Errorf("%w: hs256 secret not configured", ErrInvalidSignature)
		}
		mac := hmac.New(sha256.New, v.hs256Secret)
		mac.Write([]byte(signingInput))
		expectedSig := mac.Sum(nil)
		if subtle.ConstantTimeCompare(sigBytes, expectedSig) != 1 {
			return nil, ErrInvalidSignature
		}

	case AlgEdDSA:
		if len(v.ed25519PubKey) == 0 {
			return nil, fmt.Errorf("%w: ed25519 public key not configured", ErrInvalidSignature)
		}
		if !ed25519.Verify(v.ed25519PubKey, []byte(signingInput), sigBytes) {
			return nil, ErrInvalidSignature
		}

	default:
		return nil, fmt.Errorf("%w: unsupported algorithm %q", ErrInvalidSignature, hdr.Alg)
	}

	payloadBytes, err := decodeBase64URL(payloadB64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid payload encoding", ErrInvalidToken)
	}

	var claims TokenClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("%w: invalid claims payload", ErrInvalidToken)
	}

	// Validate temporal validity
	now := time.Now()
	if claims.ExpiresAt > 0 && now.After(time.Unix(claims.ExpiresAt, 0).Add(v.clockSkew)) {
		return nil, ErrTokenExpired
	}
	if claims.NotBefore > 0 && now.Before(time.Unix(claims.NotBefore, 0).Add(-v.clockSkew)) {
		return nil, ErrTokenNotYetValid
	}

	// Validate issuer if required
	if v.expectedIssuer != "" && claims.Issuer != v.expectedIssuer {
		return nil, fmt.Errorf("%w: issuer mismatch (expected %q, got %q)", ErrInvalidToken, v.expectedIssuer, claims.Issuer)
	}

	// Validate audience if required
	if v.expectedAudience != "" && claims.Audience != v.expectedAudience {
		return nil, fmt.Errorf("%w: audience mismatch (expected %q, got %q)", ErrInvalidToken, v.expectedAudience, claims.Audience)
	}

	return &claims, nil
}

// Authorize validates token, verifies role satisfies requiredRole, and checks device scope permissions.
func (v *JWTValidator) Authorize(tokenStr string, requiredRole Role, targetDeviceID string) (*AuthContext, error) {
	claims, err := v.Validate(tokenStr)
	if err != nil {
		return nil, err
	}

	// Check role
	if !claims.Role.Satisfies(requiredRole) {
		return nil, fmt.Errorf("%w: required %s, provided %s", ErrForbidden, requiredRole, claims.Role)
	}

	// Check device scope boundary
	if targetDeviceID != "" && len(claims.AllowedDevices) > 0 {
		matched := false
		for _, pattern := range claims.AllowedDevices {
			if pattern == "*" || pattern == targetDeviceID {
				matched = true
				break
			}
			m, _ := path.Match(pattern, targetDeviceID)
			if m {
				matched = true
				break
			}
			if strings.HasSuffix(pattern, "*") && strings.HasPrefix(targetDeviceID, strings.TrimSuffix(pattern, "*")) {
				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("%w: device %s not permitted in scopes %v", ErrDeviceScopeForbidden, targetDeviceID, claims.AllowedDevices)
		}
	}

	return &AuthContext{
		CallerID:        claims.Subject,
		AuthType:        "JWT",
		Role:            claims.Role,
		AllowedDevices:  claims.AllowedDevices,
		AuthenticatedAt: time.Now(),
	}, nil
}

// TokenAuthorizer validates Bearer JWT tokens and evaluates RBAC roles and device scope boundaries.
type TokenAuthorizer interface {
	AuthorizeHTTP(r *http.Request, requiredRole Role, targetDeviceID string) (*AuthContext, error)
	AuthorizeGRPC(ctx context.Context, requiredRole Role, targetDeviceID string) (*AuthContext, error)
}

// AuthorizeHTTP validates the HTTP Authorization header against the required role and device scope.
func (v *JWTValidator) AuthorizeHTTP(r *http.Request, requiredRole Role, targetDeviceID string) (*AuthContext, error) {
	if v == nil {
		return &AuthContext{Role: RoleAdmin}, nil
	}
	authHdr := r.Header.Get("Authorization")
	if authHdr == "" {
		return nil, ErrUnauthenticated
	}
	return v.Authorize(authHdr, requiredRole, targetDeviceID)
}

// AuthorizeGRPC validates gRPC metadata authorization against the required role and device scope.
func (v *JWTValidator) AuthorizeGRPC(ctx context.Context, requiredRole Role, targetDeviceID string) (*AuthContext, error) {
	if v == nil {
		return &AuthContext{Role: RoleAdmin}, nil
	}
	var token string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("authorization"); len(vals) > 0 {
			raw := vals[0]
			if strings.HasPrefix(strings.ToLower(raw), "bearer ") {
				token = strings.TrimSpace(raw[7:])
			} else {
				token = strings.TrimSpace(raw)
			}
		}
	}
	if token == "" {
		return nil, ErrUnauthenticated
	}
	return v.Authorize(token, requiredRole, targetDeviceID)
}

func decodeBase64URL(s string) ([]byte, error) {
	// Support unpadded or padded Base64URL
	if l := len(s) % 4; l > 0 {
		s += strings.Repeat("=", 4-l)
	}
	data, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
	}
	return data, nil
}
