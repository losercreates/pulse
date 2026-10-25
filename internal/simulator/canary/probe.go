package canary

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/losercreates/pulse/internal/simulator/metrics"
	"github.com/losercreates/pulse/internal/simulator/transport"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// Probe periodically injects synthetic canary telemetry readings and measures
// the end-to-end pipeline latency by polling ChronoDB storage.
type Probe struct {
	sender         transport.Sender
	chronoQueryURL string
	tracker        *metrics.Tracker
	interval       time.Duration
	httpClient     *http.Client
	probesSent     atomic.Int64
	probesResolved atomic.Int64
}

// NewProbe creates a canary latency verification probe.
func NewProbe(sender transport.Sender, chronoQueryURL string, tracker *metrics.Tracker, interval time.Duration) *Probe {
	if interval <= 0 {
		interval = 10 * time.Second
	}

	endpoint := chronoQueryURL
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "http://" + endpoint
	}
	endpoint = strings.TrimRight(endpoint, "/")

	return &Probe{
		sender:         sender,
		chronoQueryURL: endpoint,
		tracker:        tracker,
		interval:       interval,
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

// SendProbe transmits a single canary reading and polls ChronoDB until stored.
func (p *Probe) SendProbe(ctx context.Context) error {
	t0 := time.Now()
	timestampNs := t0.UnixNano()
	deviceID := "canary-probe-01"
	metricName := "canary_latency"

	reading := &telemetryv1.TelemetryReading{
		DeviceId:    deviceID,
		TimestampNs: timestampNs,
		MetricName:  metricName,
		Value:       1.0,
		Unit:        "ms",
		Metadata: map[string]string{
			"probe": "canary",
		},
	}

	p.probesSent.Add(1)
	if _, err := p.sender.SendBatch(ctx, []*telemetryv1.TelemetryReading{reading}); err != nil {
		return fmt.Errorf("failed to send canary reading: %w", err)
	}

	// Poll ChronoDB for the record to appear
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-pollCtx.Done():
			return pollCtx.Err()
		case <-ticker.C:
			resolved, err := p.checkChronoRecord(pollCtx, deviceID, metricName, timestampNs)
			if err == nil && resolved {
				latency := time.Since(t0)
				p.probesResolved.Add(1)
				if p.tracker != nil {
					p.tracker.RecordCanaryLatency(latency)
				}
				return nil
			}
		}
	}
}

func (p *Probe) checkChronoRecord(ctx context.Context, deviceID, metricName string, minTsNs int64) (bool, error) {
	queryURL := fmt.Sprintf("%s/api/v1/query?device_id=%s&metric_name=%s&start_time=%d&end_time=%d",
		p.chronoQueryURL,
		url.QueryEscape(deviceID),
		url.QueryEscape(metricName),
		minTsNs-int64(10*time.Second),
		minTsNs+int64(30*time.Second),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if err != nil {
		return false, err
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("query returned status %d", resp.StatusCode)
	}

	var result struct {
		TotalRecords int `json:"total_records"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, err
	}

	return result.TotalRecords > 0, nil
}

// Run executes periodic canary probes until context cancellation.
func (p *Probe) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Initial probe on startup
	_ = p.SendProbe(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = p.SendProbe(ctx)
		}
	}
}

// ProbesSent returns total canary probes emitted.
func (p *Probe) ProbesSent() int {
	return int(p.probesSent.Load())
}

// ProbesResolved returns total canary probes verified stored in ChronoDB.
func (p *Probe) ProbesResolved() int {
	return int(p.probesResolved.Load())
}
