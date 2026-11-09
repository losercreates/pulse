package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/losercreates/pulse/internal/auth"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

var (
	// ErrRestrictedDestination is returned when target URL points to a private, loopback, or metadata address.
	ErrRestrictedDestination = errors.New("webhook destination targets restricted private, loopback, or metadata network")
	// ErrInvalidWebhookScheme is returned when target URL does not use http or https.
	ErrInvalidWebhookScheme = errors.New("webhook destination must use http or https scheme")
)

// IsPrivateOrLoopbackIP reports whether ip belongs to a private, loopback, or cloud metadata network.
func IsPrivateOrLoopbackIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	if ip4 := ip.To4(); ip4 != nil {
		// 10.0.0.0/8
		if ip4[0] == 10 {
			return true
		}
		// 172.16.0.0/12
		if ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31 {
			return true
		}
		// 192.168.0.0/16
		if ip4[0] == 192 && ip4[1] == 168 {
			return true
		}
		// 100.64.0.0/10 (Carrier-Grade NAT)
		if ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
			return true
		}
	}
	return false
}

// ValidateWebhookURL verifies the scheme and IP safety of a target destination URL.
func ValidateWebhookURL(rawURL string, allowPrivate bool) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid webhook url: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return ErrInvalidWebhookScheme
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("webhook url missing host")
	}

	if allowPrivate {
		return nil
	}

	ip := net.ParseIP(host)
	if ip != nil {
		if IsPrivateOrLoopbackIP(ip) {
			return fmt.Errorf("%w: %s", ErrRestrictedDestination, host)
		}
	} else if strings.EqualFold(host, "localhost") {
		return fmt.Errorf("%w: localhost", ErrRestrictedDestination)
	}

	return nil
}

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
	url                  string
	client               *http.Client
	signer               auth.WebhookSigner
	allowPrivateNetworks bool
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

// WithAllowPrivateNetworks configures whether loopback or private network destinations are allowed.
func (s *WebhookSink) WithAllowPrivateNetworks(allow bool) *WebhookSink {
	s.allowPrivateNetworks = allow
	return s
}

// Name returns the identifier for this sink.
func (s *WebhookSink) Name() string {
	return "webhook"
}

// Deliver formats and sends the alert payload via HTTP POST.
func (s *WebhookSink) Deliver(ctx context.Context, alert *telemetryv1.AlertEvent) error {
	if err := ValidateWebhookURL(s.url, s.allowPrivateNetworks); err != nil {
		return fmt.Errorf("webhook validation rejected target: %w", err)
	}

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
