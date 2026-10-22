package detector

import (
	"testing"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestStatisticalBaseline_WarmupAndConvergence(t *testing.T) {
	alpha := 0.1
	varianceEpsilon := 0.0001
	warmup := 30
	detector := NewAnomalyDetector(alpha, 3.0, varianceEpsilon, warmup)

	devID := "turbine-01"
	metric := "temp"
	baseTime := time.Now().UnixNano()

	// 1. Send 29 samples around 70.0 with small noise - should remain in warmup
	for i := 0; i < 29; i++ {
		val := 70.0
		if i%2 == 0 {
			val = 71.0
		} else {
			val = 69.0
		}
		ts := baseTime + int64(i)*int64(time.Second)
		score, isAnomaly := detector.Evaluate(devID, metric, val, ts)
		assert.False(t, isAnomaly, "Sample %d should be suppressed in warmup", i+1)
		assert.False(t, score.IsAnomaly)
	}

	// 2. 30th sample - warmup complete, natural variance established (stddev ~ 1.0)
	ts := baseTime + 29*int64(time.Second)
	_, isAnomaly := detector.Evaluate(devID, metric, 70.0, ts)
	assert.False(t, isAnomaly)

	// 3. Inject nominal values with small noise around 70.0 (z ~ 1.0 < 3.0)
	for i := 30; i < 60; i++ {
		val := 70.0
		if i%2 == 0 {
			val = 70.8
		} else {
			val = 69.2
		}
		ts := baseTime + int64(i)*int64(time.Second)
		score, isAnomaly := detector.Evaluate(devID, metric, val, ts)
		assert.False(t, isAnomaly)
		assert.True(t, score.StdDev > 0)
	}

	// 4. Inject a significant outlier: 95.0 (expected mean ~70, stddev ~1.0)
	outlierTs := baseTime + 61*int64(time.Second)
	score, isAnomaly := detector.Evaluate(devID, metric, 95.0, outlierTs)
	assert.True(t, isAnomaly, "Outlier value 95.0 should trigger anomaly")
	assert.True(t, score.IsAnomaly)
	assert.Greater(t, score.ZScore, 5.0)
	assert.Equal(t, telemetryv1.AlertSeverity_SEVERITY_CRITICAL, score.Severity)
}

func TestStatisticalBaseline_FlatlineSensorVarianceFloor(t *testing.T) {
	// A broken sensor sending constant 50.0000
	alpha := 0.05
	varianceEpsilon := 0.0001 // sigma_min = 0.01
	warmup := 30
	detector := NewAnomalyDetector(alpha, 3.0, varianceEpsilon, warmup)

	devID := "sensor-flatline"
	metric := "pressure"
	baseTime := time.Now().UnixNano()

	for i := 0; i < 100; i++ {
		ts := baseTime + int64(i)*int64(time.Second)
		detector.Evaluate(devID, metric, 50.0, ts)
	}

	// After 100 identical samples, variance reaches the floor epsilon
	// A small fluctuation of 50.02 (diff = 0.02)
	// z = 0.02 / 0.01 = 2.0 < 3.0 -> should NOT trigger an anomaly!
	ts := baseTime + 101*int64(time.Second)
	score, isAnomaly := detector.Evaluate(devID, metric, 50.02, ts)
	assert.False(t, isAnomaly, "Minor 0.02 delta on flatline must be protected by variance floor")
	assert.InDelta(t, 2.0, score.ZScore, 0.2)

	// An actual significant spike to 50.5 (diff = 0.5, z = 50) -> should trigger CRITICAL
	tsSpike := baseTime + 102*int64(time.Second)
	scoreSpike, isAnomalySpike := detector.Evaluate(devID, metric, 50.5, tsSpike)
	assert.True(t, isAnomalySpike)
	assert.Equal(t, telemetryv1.AlertSeverity_SEVERITY_CRITICAL, scoreSpike.Severity)
}
