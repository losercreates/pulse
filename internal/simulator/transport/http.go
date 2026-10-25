package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// HTTPSender dispatches batches of telemetry readings to the Ingestion Gateway REST endpoint.
type HTTPSender struct {
	client    *http.Client
	targetURL string
}

// NewHTTPSender initializes an HTTP batch sender with tuned connection pooling.
func NewHTTPSender(targetURL string) *HTTPSender {
	endpoint := targetURL
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}
	if !strings.Contains(endpoint, "/api/v1/telemetry/batch") {
		endpoint = strings.TrimRight(endpoint, "/") + "/api/v1/telemetry/batch"
	}

	return &HTTPSender{
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        500,
				MaxIdleConnsPerHost: 200,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression: false,
			},
		},
		targetURL: endpoint,
	}
}

// SendBatch marshals and sends a batch of telemetry readings over HTTP/JSON.
func (s *HTTPSender) SendBatch(ctx context.Context, readings []*telemetryv1.TelemetryReading) (int, error) {
	if len(readings) == 0 {
		return 0, nil
	}

	batch := telemetryv1.TelemetryBatch{
		Readings: readings,
	}

	body, err := json.Marshal(&batch)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal telemetry batch: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.targetURL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http batch request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return 0, fmt.Errorf("unexpected http status %d: %s", resp.StatusCode, string(respBody))
	}

	return len(readings), nil
}

// Close closes any idle HTTP transport connections.
func (s *HTTPSender) Close() error {
	s.client.CloseIdleConnections()
	return nil
}
