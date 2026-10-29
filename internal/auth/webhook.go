package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Standard Pulse Webhook HTTP Headers.
const (
	HeaderPulseSignature = "X-Pulse-Signature"
	HeaderPulseTimestamp = "X-Pulse-Timestamp"
	DefaultMaxClockSkew  = 300 * time.Second // 5 minutes
)

// WebhookSigner creates HMAC-SHA256 signatures for outgoing webhook payloads.
type WebhookSigner interface {
	Sign(timestamp time.Time, body []byte) (signature string, timestampStr string)
}

// WebhookVerifier validates incoming HMAC-SHA256 signatures on webhook payloads.
type WebhookVerifier interface {
	Verify(signatureHeader, timestampHeader string, body []byte, maxClockSkew time.Duration) (bool, error)
}

// DefaultWebhookSigner implements WebhookSigner using HMAC-SHA256.
type DefaultWebhookSigner struct {
	secret []byte
}

// NewWebhookSigner creates a new DefaultWebhookSigner with the given shared secret.
func NewWebhookSigner(secret []byte) *DefaultWebhookSigner {
	return &DefaultWebhookSigner{secret: secret}
}

// Sign computes the HMAC-SHA256 signature for timestamp + "." + body.
// Returns "sha256=" + hex(hmac) and the timestamp as a decimal Unix epoch string.
func (s *DefaultWebhookSigner) Sign(t time.Time, body []byte) (string, string) {
	tsStr := strconv.FormatInt(t.Unix(), 10)
	canonical := tsStr + "." + string(body)

	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(canonical))
	sigHex := hex.EncodeToString(mac.Sum(nil))

	return "sha256=" + sigHex, tsStr
}

// DefaultWebhookVerifier implements WebhookVerifier using constant-time HMAC-SHA256 comparison.
type DefaultWebhookVerifier struct {
	secret []byte
}

// NewWebhookVerifier creates a new DefaultWebhookVerifier with the given shared secret.
func NewWebhookVerifier(secret []byte) *DefaultWebhookVerifier {
	return &DefaultWebhookVerifier{secret: secret}
}

// Verify checks the signature and timestamp headers against the body.
// Enforces replay prevention: |Now - Timestamp| <= maxClockSkew.
func (v *DefaultWebhookVerifier) Verify(signatureHeader, timestampHeader string, body []byte, maxClockSkew time.Duration) (bool, error) {
	if signatureHeader == "" || timestampHeader == "" {
		return false, ErrInvalidWebhookHeaders
	}

	if maxClockSkew <= 0 {
		maxClockSkew = DefaultMaxClockSkew
	}

	// Validate timestamp
	tsUnix, err := strconv.ParseInt(timestampHeader, 10, 64)
	if err != nil {
		return false, fmt.Errorf("%w: invalid timestamp header %q", ErrInvalidWebhookHeaders, timestampHeader)
	}

	msgTime := time.Unix(tsUnix, 0)
	now := time.Now()
	diff := now.Sub(msgTime)
	if diff < 0 {
		diff = -diff
	}
	if diff > maxClockSkew {
		return false, fmt.Errorf("%w: clock skew %v exceeds maximum allowed %v", ErrWebhookReplay, diff, maxClockSkew)
	}

	// Parse signature header: "sha256=<hex>"
	expectedPrefix := "sha256="
	sigHex := signatureHeader
	if strings.HasPrefix(strings.ToLower(sigHex), expectedPrefix) {
		sigHex = sigHex[len(expectedPrefix):]
	}

	// Calculate expected HMAC
	canonical := timestampHeader + "." + string(body)
	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(canonical))
	expectedBytes := mac.Sum(nil)
	expectedHex := hex.EncodeToString(expectedBytes)

	// Constant-time compare
	if subtle.ConstantTimeCompare([]byte(sigHex), []byte(expectedHex)) != 1 {
		return false, ErrInvalidSignature
	}

	return true, nil
}

