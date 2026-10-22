package detector

import (
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/alert/incident"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAnomalyDetector_WarmupAndEvaluation(t *testing.T) {
	// alpha=0.1, k=3.0, eps=0.0001, warmup=30
	detector := NewAnomalyDetector(0.1, 3.0, 0.0001, 30)

	deviceID := "pump-01"
	metric := "temp"
	baseTime := int64(1000 * time.Millisecond)

	// Feed 29 nominal readings (during warmup)
	for i := 0; i < 29; i++ {
		score, isAnomaly := detector.Evaluate(deviceID, metric, 50.0, baseTime+int64(i)*1000000)
		assert.False(t, isAnomaly, "warmup phase must not flag anomaly")
		assert.False(t, score.IsAnomaly)
	}

	// 30th reading completes warmup
	score, isAnomaly := detector.Evaluate(deviceID, metric, 50.0, baseTime+int64(30)*1000000)
	assert.False(t, isAnomaly)
	assert.False(t, score.IsAnomaly)

	// Feed nominal reading -> no anomaly
	score, isAnomaly = detector.Evaluate(deviceID, metric, 50.0, baseTime+int64(31)*1000000)
	assert.False(t, isAnomaly)
	assert.False(t, score.IsAnomaly)

	// Inject massive spike (150.0 when mean is ~50.0, stddev is ~sqrt(eps) = 0.01)
	score, isAnomaly = detector.Evaluate(deviceID, metric, 150.0, baseTime+int64(32)*1000000)
	assert.True(t, isAnomaly)
	assert.True(t, score.IsAnomaly)
	assert.True(t, score.ZScore >= 3.0)
	assert.Equal(t, telemetryv1.AlertSeverity_SEVERITY_CRITICAL, score.Severity)
	assert.Equal(t, 150.0, score.TriggerValue)
}

func TestAnomalyDetector_ProcessReading_Lifecycle(t *testing.T) {
	detector := NewAnomalyDetector(0.1, 3.0, 0.0001, 30)
	tracker := incident.NewIncidentTracker(5*time.Minute, 10)
	detector.WithIncidentTracker(tracker)

	deviceID := "turbine-01"
	metric := "vibration"
	ts := int64(1_000_000_000)

	// Warmup
	for i := 0; i < 30; i++ {
		evt := detector.ProcessReading(deviceID, metric, 10.0, ts+int64(i)*100_000_000, 1)
		assert.Nil(t, evt, "warmup readings should not emit alert")
	}

	// Inject anomaly reading -> should emit STATUS_FIRING
	evt := detector.ProcessReading(deviceID, metric, 50.0, ts+31*100_000_000, 1)
	require.NotNil(t, evt, "first anomaly must emit FIRING alert")
	assert.Equal(t, telemetryv1.AlertStatus_STATUS_FIRING, evt.Status)
	assert.Equal(t, deviceID, evt.DeviceId)
	assert.Equal(t, metric, evt.MetricName)
	assert.Equal(t, int32(1), evt.PartitionId)
	assert.NotEmpty(t, evt.AlertId)

	// Immediate repeat anomaly within 5-min cooldown -> suppressed, returns nil
	evt2 := detector.ProcessReading(deviceID, metric, 52.0, ts+32*100_000_000, 1)
	assert.Nil(t, evt2, "repeat anomaly within cooldown window must be suppressed")

	// 9 nominal readings (value = 10.0) -> hysteresis not satisfied yet
	for i := 0; i < 9; i++ {
		resolveEvt := detector.ProcessReading(deviceID, metric, 10.0, ts+int64(40+i)*100_000_000, 1)
		assert.Nil(t, resolveEvt, "9 nominal readings must not resolve")
	}

	// 10th nominal reading -> triggers STATUS_RESOLVED
	resolveEvt := detector.ProcessReading(deviceID, metric, 10.0, ts+50*100_000_000, 1)
	require.NotNil(t, resolveEvt, "10th nominal reading must emit RESOLVED alert")
	assert.Equal(t, telemetryv1.AlertStatus_STATUS_RESOLVED, resolveEvt.Status)
	assert.Equal(t, deviceID, resolveEvt.DeviceId)
	assert.Equal(t, metric, resolveEvt.MetricName)
}
