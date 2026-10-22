package incident

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIncidentTracker_CooldownSuppressionAndAutoResolution(t *testing.T) {
	cooldown := 5 * time.Minute
	nominalThreshold := 10
	tracker := NewIncidentTracker(cooldown, nominalThreshold)

	devID := "turbine-01"
	metric := "vibration"
	baseTime := int64(100 * time.Second)

	// 1. First anomaly: should fire
	shouldFire, isEscalation := tracker.OnAnomaly(devID, metric, baseTime)
	assert.True(t, shouldFire)
	assert.False(t, isEscalation)
	assert.Equal(t, 1, tracker.ActiveIncidentCount())

	// 2. Anomalies arriving within cooldown (10s, 60s, 200s later) -> must be suppressed
	for _, delay := range []time.Duration{10 * time.Second, 60 * time.Second, 200 * time.Second} {
		fire, _ := tracker.OnAnomaly(devID, metric, baseTime+int64(delay))
		assert.False(t, fire, "Anomaly within cooldown window must be suppressed")
	}

	// 3. Anomaly arriving after cooldown expires (5min 1s later) -> must fire reminder/escalation
	fire, escalation := tracker.OnAnomaly(devID, metric, baseTime+int64(5*time.Minute)+int64(time.Second))
	assert.True(t, fire, "Anomaly after cooldown expiration must fire")
	assert.True(t, escalation)

	// 4. Recovery: 9 nominal readings (threshold is 10) -> not resolved yet
	for i := 0; i < 9; i++ {
		resolved := tracker.OnNominal(devID, metric)
		assert.False(t, resolved, "9 nominal readings should not resolve incident")
	}
	assert.Equal(t, 1, tracker.ActiveIncidentCount())

	// 5. 10th nominal reading -> auto-resolved!
	resolved := tracker.OnNominal(devID, metric)
	assert.True(t, resolved, "10th nominal reading must trigger auto-resolution")
	assert.Equal(t, 0, tracker.ActiveIncidentCount())

	// 6. Subsequent nominal readings do nothing
	assert.False(t, tracker.OnNominal(devID, metric))
}
