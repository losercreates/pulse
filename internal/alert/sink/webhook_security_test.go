package sink_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/alert/sink"
	"github.com/losercreates/pulse/internal/auth"
	"github.com/losercreates/pulse/pkg/webhookverifier"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookSinkHMACSecurity(t *testing.T) {
	secret := "alert-dispatcher-hmac-secret-xyz987"
	signer := auth.NewWebhookSigner([]byte(secret))
	verifier := webhookverifier.New(secret)

	var mu sync.Mutex
	var receivedHeaders http.Header
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		receivedHeaders = r.Header.Clone()
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	webhookSink := sink.NewWebhookSink(server.URL, nil).WithSigner(signer)

	alert := &telemetryv1.AlertEvent{
		AlertId:           "alert-001",
		DeviceId:          "turbine-01",
		MetricName:        "bearing_temp",
		TriggerValue:      98.6,
		ExpectedMean:      50.0,
		StandardDeviation: 5.0,
		ZScore:            9.72,
		Severity:          telemetryv1.AlertSeverity_SEVERITY_CRITICAL,
		Status:            telemetryv1.AlertStatus_STATUS_FIRING,
		TimestampNs:       time.Now().UnixNano(),
		PartitionId:       0,
	}

	t.Run("SC-004: Delivers alert with valid HMAC-SHA256 signature and timestamp headers", func(t *testing.T) {
		err := webhookSink.Deliver(context.Background(), alert)
		require.NoError(t, err)

		mu.Lock()
		sig := receivedHeaders.Get("X-Pulse-Signature")
		ts := receivedHeaders.Get("X-Pulse-Timestamp")
		body := append([]byte(nil), receivedBody...)
		mu.Unlock()

		assert.NotEmpty(t, sig)
		assert.True(t, strings.HasPrefix(sig, "sha256="))
		assert.NotEmpty(t, ts)

		// Verify with WebhookVerifier SDK
		err = verifier.Verify(sig, ts, body)
		assert.NoError(t, err)
	})

	t.Run("SC-004: Tampered payload bytes detected and rejected", func(t *testing.T) {
		mu.Lock()
		sig := receivedHeaders.Get("X-Pulse-Signature")
		ts := receivedHeaders.Get("X-Pulse-Timestamp")
		body := append([]byte(nil), receivedBody...)
		mu.Unlock()

		tamperedBody := append(body, []byte("tamper")...)
		err := verifier.Verify(sig, ts, tamperedBody)
		assert.ErrorIs(t, err, webhookverifier.ErrInvalidSignature)
	})

	t.Run("SC-004: Replay attack rejected when timestamp > 300s old", func(t *testing.T) {
		oldTime := time.Now().Add(-10 * time.Minute)
		oldSig, oldTs := signer.Sign(oldTime, []byte(`{"test":"payload"}`))

		err := verifier.Verify(oldSig, oldTs, []byte(`{"test":"payload"}`))
		assert.ErrorIs(t, err, webhookverifier.ErrReplayDetected)
	})

	t.Run("SC-004: Mismatched secret rejected", func(t *testing.T) {
		wrongVerifier := webhookverifier.New("completely-different-secret")
		mu.Lock()
		sig := receivedHeaders.Get("X-Pulse-Signature")
		ts := receivedHeaders.Get("X-Pulse-Timestamp")
		body := append([]byte(nil), receivedBody...)
		mu.Unlock()

		err := wrongVerifier.Verify(sig, ts, body)
		assert.ErrorIs(t, err, webhookverifier.ErrInvalidSignature)
	})

	t.Run("WebhookVerifier HTTP Middleware integration", func(t *testing.T) {
		middleware := verifier.Middleware()
		handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		mu.Lock()
		sig := receivedHeaders.Get("X-Pulse-Signature")
		ts := receivedHeaders.Get("X-Pulse-Timestamp")
		body := append([]byte(nil), receivedBody...)
		mu.Unlock()

		// Valid request passes middleware
		req := httptest.NewRequest("POST", "/webhook", strings.NewReader(string(body)))
		req.Header.Set("X-Pulse-Signature", sig)
		req.Header.Set("X-Pulse-Timestamp", ts)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Missing headers fails middleware
		reqNoHeaders := httptest.NewRequest("POST", "/webhook", strings.NewReader(string(body)))
		recNoHeaders := httptest.NewRecorder()
		handler.ServeHTTP(recNoHeaders, reqNoHeaders)
		assert.Equal(t, http.StatusUnauthorized, recNoHeaders.Code)
	})
}
