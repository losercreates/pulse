package auth

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func generateTestCert(t *testing.T, commonName string, dnsNames []string) *x509.Certificate {
	t.Helper()
	return &x509.Certificate{
		SerialNumber: big.NewInt(100),
		Subject: pkix.Name{
			CommonName: commonName,
		},
		DNSNames:  dnsNames,
		NotBefore: time.Now().Add(-1 * time.Hour),
		NotAfter:  time.Now().Add(24 * time.Hour),
	}
}

func TestExtractDeviceIdentityFromCert(t *testing.T) {
	t.Run("Valid CommonName", func(t *testing.T) {
		cert := generateTestCert(t, "turbine-01", nil)
		id, err := ExtractDeviceIdentityFromCert(cert)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id.DeviceID != "turbine-01" {
			t.Errorf("expected DeviceID turbine-01, got %s", id.DeviceID)
		}
		if id.AuthMethod != "mTLS" {
			t.Errorf("expected AuthMethod mTLS, got %s", id.AuthMethod)
		}
	})

	t.Run("Fallback to SAN DNSName", func(t *testing.T) {
		cert := generateTestCert(t, "", []string{"sensor-42.pulse.local"})
		id, err := ExtractDeviceIdentityFromCert(cert)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id.DeviceID != "sensor-42.pulse.local" {
			t.Errorf("expected DeviceID sensor-42.pulse.local, got %s", id.DeviceID)
		}
	})

	t.Run("Missing CN and SAN", func(t *testing.T) {
		cert := generateTestCert(t, "", nil)
		_, err := ExtractDeviceIdentityFromCert(cert)
		if err == nil {
			t.Fatal("expected error for cert without CN or SAN")
		}
	})

	t.Run("Nil cert", func(t *testing.T) {
		_, err := ExtractDeviceIdentityFromCert(nil)
		if err == nil {
			t.Fatal("expected error for nil cert")
		}
	})
}

func TestAPIKeyValidator(t *testing.T) {
	v := NewAPIKeyValidator(
		APIKeyEntry{Key: "secret-key-1", DeviceID: "turbine-01"},
		APIKeyEntry{Key: "expired-key", DeviceID: "turbine-02", ExpiresAt: time.Now().Add(-1 * time.Hour)},
	)

	t.Run("Valid key", func(t *testing.T) {
		id, err := v.Validate("secret-key-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id.DeviceID != "turbine-01" {
			t.Errorf("expected turbine-01, got %s", id.DeviceID)
		}
		if id.AuthMethod != "APIKey" {
			t.Errorf("expected APIKey, got %s", id.AuthMethod)
		}
	})

	t.Run("Invalid key", func(t *testing.T) {
		_, err := v.Validate("non-existent-key")
		if err != ErrUnauthenticated {
			t.Errorf("expected ErrUnauthenticated, got %v", err)
		}
	})

	t.Run("Empty key", func(t *testing.T) {
		_, err := v.Validate("")
		if err != ErrMissingCredentials {
			t.Errorf("expected ErrMissingCredentials, got %v", err)
		}
	})

	t.Run("Expired key", func(t *testing.T) {
		_, err := v.Validate("expired-key")
		if err != ErrTokenExpired {
			t.Errorf("expected ErrTokenExpired, got %v", err)
		}
	})

	t.Run("Load from JSON file", func(t *testing.T) {
		tmpDir := t.TempDir()
		keyFile := filepath.Join(tmpDir, "keys.json")
		content := `{"my-api-key": "turbine-99"}`
		if err := os.WriteFile(keyFile, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}

		v2 := NewAPIKeyValidator()
		if err := v2.LoadFile(keyFile); err != nil {
			t.Fatalf("failed to load key file: %v", err)
		}
		id, err := v2.Validate("my-api-key")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id.DeviceID != "turbine-99" {
			t.Errorf("expected turbine-99, got %s", id.DeviceID)
		}
	})
}

func TestAntiSpoofingValidation(t *testing.T) {
	auth := NewGatewayAuthenticator(NewAPIKeyValidator())

	t.Run("Match succeeds", func(t *testing.T) {
		id := &DeviceIdentity{DeviceID: "turbine-01"}
		err := auth.ValidateDeviceBinding(id, "turbine-01")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("Mismatch fails with ErrSpoofedIdentity", func(t *testing.T) {
		id := &DeviceIdentity{DeviceID: "turbine-01"}
		err := auth.ValidateDeviceBinding(id, "turbine-02")
		if err == nil || !strings.Contains(err.Error(), "spoofing") {
			t.Fatalf("expected spoofing violation error, got: %v", err)
		}
	})

	t.Run("Nil identity fails", func(t *testing.T) {
		err := auth.ValidateDeviceBinding(nil, "turbine-01")
		if err != ErrUnauthenticated {
			t.Fatalf("expected ErrUnauthenticated, got: %v", err)
		}
	})
}

func TestJWTValidator(t *testing.T) {
	secret := []byte("pulse-super-secret-jwt-key-32b!")
	v := NewHS256Validator(secret, "pulse-auth", "pulse-chronodb")

	t.Run("Valid token and authorization", func(t *testing.T) {
		now := time.Now().Unix()
		claims := TokenClaims{
			Issuer:         "pulse-auth",
			Subject:        "analyst@pulse.internal",
			Audience:       "pulse-chronodb",
			Role:           RoleOperator,
			AllowedDevices: []string{"turbine-alpha-*", "turbine-01"},
			ExpiresAt:      now + 3600,
			IssuedAt:       now,
		}

		token, err := SignHS256(claims, secret)
		if err != nil {
			t.Fatalf("sign error: %v", err)
		}

		// Viewer role requirement should be satisfied by Operator
		ctx, err := v.Authorize(token, RoleViewer, "turbine-01")
		if err != nil {
			t.Fatalf("authorize error: %v", err)
		}
		if ctx.CallerID != "analyst@pulse.internal" {
			t.Errorf("expected caller ID, got %s", ctx.CallerID)
		}
		if ctx.Role != RoleOperator {
			t.Errorf("expected Operator role, got %s", ctx.Role)
		}

		// Wildcard device scope match
		_, err = v.Authorize(token, RoleViewer, "turbine-alpha-05")
		if err != nil {
			t.Fatalf("expected wildcard device scope to succeed, got: %v", err)
		}

		// Unauthorized device scope
		_, err = v.Authorize(token, RoleViewer, "turbine-beta-01")
		if err == nil || !strings.Contains(err.Error(), "not permitted") {
			t.Fatalf("expected device scope failure, got: %v", err)
		}

		// Admin role requirement should fail for Operator
		_, err = v.Authorize(token, RoleAdmin, "turbine-01")
		if err == nil || !strings.Contains(err.Error(), "required Admin") {
			t.Fatalf("expected role failure, got: %v", err)
		}
	})

	t.Run("Expired token", func(t *testing.T) {
		now := time.Now().Unix()
		claims := TokenClaims{
			Issuer:    "pulse-auth",
			ExpiresAt: now - 3600,
		}
		token, _ := SignHS256(claims, secret)
		_, err := v.Validate(token)
		if err != ErrTokenExpired {
			t.Errorf("expected ErrTokenExpired, got %v", err)
		}
	})

	t.Run("Signature tampering", func(t *testing.T) {
		claims := TokenClaims{
			Issuer:    "pulse-auth",
			ExpiresAt: time.Now().Unix() + 3600,
		}
		token, _ := SignHS256(claims, []byte("wrong-secret-key-that-does-not-match"))
		_, err := v.Validate(token)
		if err != ErrInvalidSignature {
			t.Errorf("expected ErrInvalidSignature, got %v", err)
		}
	})

	t.Run("Ed25519 signing and verification", func(t *testing.T) {
		pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		edValidator := NewEd25519Validator(pubKey, "pulse-auth", "pulse-chronodb")

		claims := TokenClaims{
			Issuer:    "pulse-auth",
			Subject:   "admin@pulse.internal",
			Audience:  "pulse-chronodb",
			Role:      RoleAdmin,
			ExpiresAt: time.Now().Unix() + 3600,
		}
		token, err := SignEd25519(claims, privKey)
		if err != nil {
			t.Fatalf("ed25519 sign error: %v", err)
		}

		parsed, err := edValidator.Validate(token)
		if err != nil {
			t.Fatalf("ed25519 validate error: %v", err)
		}
		if parsed.Subject != "admin@pulse.internal" {
			t.Errorf("expected subject admin@pulse.internal, got %s", parsed.Subject)
		}
	})
}

func TestWebhookSigningAndVerification(t *testing.T) {
	secret := []byte("webhook-signing-secret-key-1234")
	signer := NewWebhookSigner(secret)
	verifier := NewWebhookVerifier(secret)

	body := []byte(`{"incident_id":"inc-001","device_id":"turbine-01","metric":"temp","status":"CRITICAL"}`)
	now := time.Now()

	sig, tsStr := signer.Sign(now, body)

	t.Run("Valid signature and timestamp", func(t *testing.T) {
		ok, err := verifier.Verify(sig, tsStr, body, 300*time.Second)
		if err != nil || !ok {
			t.Fatalf("expected valid verification, got ok=%v, err=%v", ok, err)
		}
	})

	t.Run("Payload tampering detected", func(t *testing.T) {
		tampered := []byte(`{"incident_id":"inc-001","device_id":"turbine-01","metric":"temp","status":"RESOLVED"}`)
		ok, err := verifier.Verify(sig, tsStr, tampered, 300*time.Second)
		if ok || err != ErrInvalidSignature {
			t.Fatalf("expected ErrInvalidSignature, got ok=%v, err=%v", ok, err)
		}
	})

	t.Run("Replay attack outside clock skew tolerance", func(t *testing.T) {
		oldTime := time.Now().Add(-10 * time.Minute)
		oldSig, oldTsStr := signer.Sign(oldTime, body)
		ok, err := verifier.Verify(oldSig, oldTsStr, body, 300*time.Second)
		if ok || err == nil || !strings.Contains(err.Error(), "clock skew") {
			t.Fatalf("expected replay rejection, got ok=%v, err=%v", ok, err)
		}
	})
}

func TestTokenBucketRateLimiter(t *testing.T) {
	rl := NewTokenBucketRateLimiter(10.0, 3.0) // 10 tokens/sec, burst of 3

	// Burst of 3 should all be allowed
	for i := 0; i < 3; i++ {
		allowed, _ := rl.Allow("turbine-01")
		if !allowed {
			t.Fatalf("expected request %d in burst to be allowed", i)
		}
	}

	// 4th request without delay should be disallowed
	allowed, retryAfter := rl.Allow("turbine-01")
	if allowed {
		t.Fatal("expected 4th immediate request to be rate limited")
	}
	if retryAfter <= 0 {
		t.Errorf("expected positive retryAfter duration, got %v", retryAfter)
	}

	// Different device should have its own separate bucket
	allowed2, _ := rl.Allow("turbine-02")
	if !allowed2 {
		t.Fatal("expected separate device turbine-02 to be allowed")
	}
}

func TestAuditLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONAuditLogger(&buf, "pulse-test")

	logger.LogSpoofingAttempt("127.0.0.1:1234", "mTLS", "turbine-02", "turbine-01", 403, 7)

	var record SecurityAuditRecord
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("failed to decode audit log JSON: %v", err)
	}

	if record.EventType != EventSpoofingAttempt {
		t.Errorf("expected %s, got %s", EventSpoofingAttempt, record.EventType)
	}
	if record.Outcome != "DENIED" {
		t.Errorf("expected DENIED, got %s", record.Outcome)
	}
	if record.ClaimedDeviceID != "turbine-02" || record.CertDeviceID != "turbine-01" {
		t.Errorf("mismatched audit IDs: claimed=%s, cert=%s", record.ClaimedDeviceID, record.CertDeviceID)
	}
}

func TestHTTPMiddleware(t *testing.T) {
	keyVal := NewAPIKeyValidator(APIKeyEntry{Key: "valid-key", DeviceID: "turbine-01"})
	authenticator := NewGatewayAuthenticator(keyVal)
	rateLimiter := NewTokenBucketRateLimiter(10, 2)
	audit := &NopAuditLogger{}

	mw := GatewayHTTPMiddleware(authenticator, rateLimiter, audit)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := FromDeviceIdentity(r.Context())
		if !ok {
			t.Error("missing identity in context")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(id.DeviceID))
	}))

	t.Run("Missing credentials returns 401", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/telemetry", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("Valid API key returns 200", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/telemetry", nil)
		req.Header.Set("Authorization", "Bearer valid-key")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", rec.Code)
		}
		if rec.Body.String() != "turbine-01" {
			t.Errorf("expected body turbine-01, got %s", rec.Body.String())
		}
	})
}

