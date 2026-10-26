package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type grafanaDashboard struct {
	UID         string         `json:"uid"`
	Title       string         `json:"title"`
	Tags        []string       `json:"tags"`
	Panels      []grafanaPanel `json:"panels"`
	Refresh     string         `json:"refresh"`
	SchemaVer   int            `json:"schemaVersion"`
}

type grafanaPanel struct {
	ID        int            `json:"id"`
	Title     string         `json:"title"`
	Type      string         `json:"type"`
	Targets   []panelTarget  `json:"targets"`
}

type panelTarget struct {
	Expr         string `json:"expr"`
	LegendFormat string `json:"legendFormat,omitempty"`
	RefID        string `json:"refId"`
}

func TestGrafanaDashboards_SchemaAndPanels(t *testing.T) {
	expectedDashboards := map[string]struct {
		expectedUID       string
		expectedTitle     string
		minPanels         int
		requiredSubstrings []string
	}{
		"executive_overview.json": {
			expectedUID:   "pulse-executive-overview",
			expectedTitle: "Pulse Executive Pipeline Overview",
			minPanels:     4,
			requiredSubstrings: []string{
				"pulse_ingest_records_total",
				"pulse_stream_events_processed_total",
			},
		},
		"ingest_broker_lag.json": {
			expectedUID:   "pulse-ingest-broker-lag",
			expectedTitle: "Ingestion Gateway & Redpanda Partition Lag",
			minPanels:     4,
			requiredSubstrings: []string{
				"pulse_ingest_records_total",
				"pulse_stream_consumer_lag",
			},
		},
		"stream_processor.json": {
			expectedUID:   "pulse-stream-processor",
			expectedTitle: "Stream Processor & Watermarking Health",
			minPanels:     4,
			requiredSubstrings: []string{
				"pulse_stream_watermark_lag_seconds",
				"pulse_stream_open_windows_count",
			},
		},
		"anomaly_alerts.json": {
			expectedUID:   "pulse-anomaly-alerts",
			expectedTitle: "Anomaly Detection & Decoupled Alerting",
			minPanels:     4,
			requiredSubstrings: []string{
				"pulse_anomaly_detections_total",
				"pulse_alert_circuit_breaker_state",
			},
		},
		"chronodb_storage.json": {
			expectedUID:   "pulse-chronodb-storage",
			expectedTitle: "ChronoDB Storage & Analytical Query Engine",
			minPanels:     4,
			requiredSubstrings: []string{
				"pulse_storage_writes_total",
				"pulse_storage_query_latency_seconds",
			},
		},
	}

	dashboardsDir := "../../deployments/grafana/dashboards"

	for file, expected := range expectedDashboards {
		t.Run(file, func(t *testing.T) {
			path := filepath.Join(dashboardsDir, file)
			data, err := os.ReadFile(path)
			require.NoError(t, err, "dashboard file %s must exist", file)

			var d grafanaDashboard
			err = json.Unmarshal(data, &d)
			require.NoError(t, err, "dashboard %s must be valid JSON", file)

			assert.Equal(t, expected.expectedUID, d.UID, "UID must match specification")
			assert.Equal(t, expected.expectedTitle, d.Title, "Title must match specification")
			assert.GreaterOrEqual(t, len(d.Panels), expected.minPanels, "Must contain required number of panels")

			rawContent := string(data)
			for _, sub := range expected.requiredSubstrings {
				assert.Contains(t, rawContent, sub, "Dashboard must reference metric %s", sub)
			}

			// Validate each panel has an ID, Type, and non-empty Targets
			for _, p := range d.Panels {
				assert.NotEmpty(t, p.Title, "Panel in %s must have a title", file)
				assert.NotEmpty(t, p.Type, "Panel %s must have a type", p.Title)
				if p.Type != "row" {
					assert.NotEmpty(t, p.Targets, "Panel %s must have query targets", p.Title)
					for _, target := range p.Targets {
						assert.NotEmpty(t, target.Expr, "Panel %s target must have an expression", p.Title)
					}
				}
			}
		})
	}
}
