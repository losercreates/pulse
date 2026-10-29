package webhookverifier_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/auth"
	"github.com/losercreates/pulse/pkg/webhookverifier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookVerifierSDK(t *testing.T) {
	secret := "pulse-sdk-test-secret"
	v := webhookverifier.New(secret)
	signer := auth.NewWebhookSigner([]byte(secret))

	body := []byte(`{"event":"incident.created","incident_id":"inc-999"}`)
	now := time.Now()
	sig, ts := signer.Sign(now, body)

	t.Run("Verify valid payload", func(t *testing.T) {
		err := v.Verify(sig, ts, body)
		assert.NoError(t, err)
	})

	t.Run("VerifyRequest with valid request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set(webhookverifier.HeaderSignature, sig)
		req.Header.Set(webhookverifier.HeaderTimestamp, ts)

		payload, err := v.VerifyRequest(req)
		require.NoError(t, err)
		assert.Equal(t, body, payload)
	})

	t.Run("Verify missing headers", func(t *testing.T) {
		err := v.Verify("", "", body)
		assert.ErrorIs(t, err, webhookverifier.ErrMissingHeaders)
	})

	t.Run("Verify invalid signature", func(t *testing.T) {
		err := v.Verify("sha256=badbadbad", ts, body)
		assert.ErrorIs(t, err, webhookverifier.ErrInvalidSignature)
	})
}

