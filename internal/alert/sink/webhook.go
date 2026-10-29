package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/losercreates/pulse/internal/auth"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// WebhookPayload represents the JSON payload dispatched to HTTP webhooks.
type WebhookPayload struct {
	AlertID           string  `json:"alert_id"`
	DeviceID          string  `json:"device_id"`
	MetricName        string  `json:"metric_name"`
	TriggerValue      float64 `json:"trigger_value"`
	ExpectedMean      float64 `json:"expected_mean"`
	StandardDeviation float64 `json:"standard_deviation"`
	ZScore            float64 `json:"z_score"`
	Severity          string  `json:"severity"`
	Status            string  `json:"status"`
	TimestampNs       int64   `json:"timestamp_ns"`
	PartitionID       int32   `json:"partition_id"`
}

// WebhookSink delivers alert notifications to external HTTP endpoints.
type WebhookSink struct {
	url    string
	client *http.Client
	signer auth.WebhookSigner
}

// NewWebhookSink creates a new WebhookSink targeting targetURL.
func NewWebhookSink(targetURL string, client *http.Client) *WebhookSink {
	if client == nil {
		client = &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}
	return &WebhookSink{
		url:    targetURL,
		client: client,
	}
}

// WithSigner attaches an HMAC-SHA256 signer to the WebhookSink for cryptographic integrity.
func (s *WebhookSink) WithSigner(signer auth.WebhookSigner) *WebhookSink {
	s.signer = signer
	return s
}

// Name returns the identifier for this sink.
func (s *WebhookSink) Name() string {
	return "webhook"
}

// Deliver formats and sends the alert payload via HTTP POST.
func (s *WebhookSink) Deliver(ctx context.Context, alert *telemetryv1.AlertEvent) error {
	payload := WebhookPayload{
		AlertID:           alert.AlertId,
		DeviceID:          alert.DeviceId,
		MetricName:        alert.MetricName,
		TriggerValue:      alert.TriggerValue,
		ExpectedMean:      alert.ExpectedMean,
		StandardDeviation: alert.StandardDeviation,
		ZScore:            alert.ZScore,
		Severity:          alert.Severity.String(),
		Status:            alert.Status.String(),
		TimestampNs:       alert.TimestampNs,
		PartitionID:       alert.PartitionId,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal alert webhook payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Pulse-AlertDispatcher/1.0")

	// Milestone 009: Attach HMAC-SHA256 signature and timestamp headers
	if s.signer != nil {
		sig, tsStr := s.signer.Sign(time.Now().UTC(), body)
		req.Header.Set(auth.HeaderPulseSignature, sig)
		req.Header.Set(auth.HeaderPulseTimestamp, tsStr)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook POST failed: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook endpoint returned non-2xx status code: %d", resp.StatusCode)
	}

	return nil
}
