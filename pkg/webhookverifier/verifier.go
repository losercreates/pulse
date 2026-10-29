package webhookverifier

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Pulse HTTP headers for alert webhook authenticity.
const (
	HeaderSignature     = "X-Pulse-Signature"
	HeaderTimestamp     = "X-Pulse-Timestamp"
	DefaultMaxClockSkew = 300 * time.Second // 5 minutes
)

// Security error sentinels.
var (
	ErrMissingHeaders   = errors.New("missing X-Pulse-Signature or X-Pulse-Timestamp headers")
	ErrInvalidSignature = errors.New("invalid HMAC-SHA256 signature")
	ErrReplayDetected   = errors.New("webhook timestamp outside allowed clock skew window")
)

// Verifier validates incoming webhook alerts from Pulse.
type Verifier struct {
	secret []byte
}

// New creates a new Verifier with the specified shared secret.
func New(secret string) *Verifier {
	return &Verifier{secret: []byte(secret)}
}

// NewWithBytes creates a new Verifier with raw secret bytes.
func NewWithBytes(secret []byte) *Verifier {
	return &Verifier{secret: secret}
}

// Verify validates the signature and timestamp headers against the body bytes.
// Enforces constant-time HMAC comparison and replay protection.
func (v *Verifier) Verify(signatureHeader, timestampHeader string, body []byte, maxClockSkew ...time.Duration) error {
	if signatureHeader == "" || timestampHeader == "" {
		return ErrMissingHeaders
	}

	skew := DefaultMaxClockSkew
	if len(maxClockSkew) > 0 && maxClockSkew[0] > 0 {
		skew = maxClockSkew[0]
	}

	// 1. Validate timestamp and defend against replay attacks
	tsUnix, err := strconv.ParseInt(timestampHeader, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid timestamp header %q", ErrMissingHeaders, timestampHeader)
	}

	msgTime := time.Unix(tsUnix, 0)
	now := time.Now()
	diff := now.Sub(msgTime)
	if diff < 0 {
		diff = -diff
	}
	if diff > skew {
		return fmt.Errorf("%w: clock drift %v exceeds maximum permitted %v", ErrReplayDetected, diff, skew)
	}

	// 2. Parse signature header (e.g. "sha256=1a2b3c...")
	sigHex := signatureHeader
	if strings.HasPrefix(strings.ToLower(sigHex), "sha256=") {
		sigHex = sigHex[7:]
	}

	// 3. Compute expected HMAC-SHA256 signature
	canonical := timestampHeader + "." + string(body)
	mac := hmac.New(sha256.New, v.secret)
	mac.Write([]byte(canonical))
	expectedBytes := mac.Sum(nil)
	expectedHex := hex.EncodeToString(expectedBytes)

	// 4. Constant-time comparison to protect against timing attacks
	if subtle.ConstantTimeCompare([]byte(sigHex), []byte(expectedHex)) != 1 {
		return ErrInvalidSignature
	}

	return nil
}

// VerifyRequest extracts headers and body from an *http.Request, validates authenticity,
// and resets r.Body so subsequent handlers can read it.
func (v *Verifier) VerifyRequest(r *http.Request, maxClockSkew ...time.Duration) ([]byte, error) {
	sigHeader := r.Header.Get(HeaderSignature)
	tsHeader := r.Header.Get(HeaderTimestamp)

	if r.Body == nil {
		return nil, errors.New("empty request body")
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	if err := v.Verify(sigHeader, tsHeader, body, maxClockSkew...); err != nil {
		return body, err
	}

	return body, nil
}

// Middleware creates standard HTTP middleware verifying webhook authenticity on incoming requests.
func (v *Verifier) Middleware(errorHandler ...http.HandlerFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := v.VerifyRequest(r)
			if err != nil {
				if len(errorHandler) > 0 && errorHandler[0] != nil {
					errorHandler[0](w, r)
					return
				}
				http.Error(w, `{"error": "Unauthorized: `+err.Error()+`"}`, http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

