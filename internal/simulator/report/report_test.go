package report_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/config"
	"github.com/losercreates/pulse/internal/simulator/metrics"
	"github.com/losercreates/pulse/internal/simulator/report"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReport_JSONSchemaConformity(t *testing.T) {
	cfg := &config.Config{
		TargetRate: 50000,
		NumDevices: 1000,
		Duration:   60 * time.Second,
		Transport:  "grpc",
		Profile:    "steady",
	}

	snap := metrics.Snapshot{
		TotalSent:         3000000,
		TotalFailed:       0,
		ElapsedSeconds:    60.0,
		InstantaneousRate: 50000.0,
		AverageRate:       50000.0,
		PeakRate:          52000.0,
		ErrorRatePct:      0.0,
		CanaryCount:       6,
		CanaryP50Ms:       1200.0,
		CanaryP90Ms:       1500.0,
		CanaryP95Ms:       1650.0,
	}

	rep := report.BuildReport(cfg, snap, 6, 6)
	assert.True(t, rep.SLAVerdict.Compliant)
	assert.True(t, rep.SLAVerdict.ThroughputSLAMet)
	assert.True(t, rep.SLAVerdict.LatencySLAMet)
	assert.True(t, rep.SLAVerdict.ErrorSLAMet)
	assert.Empty(t, rep.SLAVerdict.Violations)

	data, err := json.Marshal(rep)
	require.NoError(t, err)

	var parsed map[string]any
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)

	assert.Contains(t, parsed, "timestamp")
	assert.Contains(t, parsed, "config")
	assert.Contains(t, parsed, "results")
	assert.Contains(t, parsed, "sla_verdict")

	res := parsed["results"].(map[string]any)
	assert.Equal(t, float64(3000000), res["total_sent"])
	assert.Equal(t, float64(0), res["total_failed"])
}

func TestReport_SLAViolations(t *testing.T) {
	cfg := &config.Config{
		TargetRate: 50000,
		NumDevices: 1000,
		Duration:   60 * time.Second,
		Transport:  "grpc",
		Profile:    "steady",
	}

	// Degraded snapshot: low throughput, high latency, high errors
	snap := metrics.Snapshot{
		TotalSent:         1000000,
		TotalFailed:       50000,
		ElapsedSeconds:    60.0,
		InstantaneousRate: 15000.0,
		AverageRate:       16666.0,
		PeakRate:          20000.0,
		ErrorRatePct:      4.76,
		CanaryCount:       5,
		CanaryP95Ms:       2500.0, // > 2000ms SLA
	}

	rep := report.BuildReport(cfg, snap, 5, 5)
	assert.False(t, rep.SLAVerdict.Compliant)
	assert.False(t, rep.SLAVerdict.ThroughputSLAMet)
	assert.False(t, rep.SLAVerdict.LatencySLAMet)
	assert.False(t, rep.SLAVerdict.ErrorSLAMet)
	assert.Len(t, rep.SLAVerdict.Violations, 3)
}
