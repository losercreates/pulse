package incident

import (
	"fmt"
	"time"
)

// IncidentState tracks active incident lifecycle per series.
type IncidentState struct {
	DeviceID           string
	MetricName         string
	IsFiring           bool
	LastAlertTimeNs    int64
	ConsecutiveNominal int
}

// IncidentTracker enforces cooldown suppression and auto-resolution hysteresis.
// Verbatim Constraints:
// - cooldown suppression (default: 5 minutes)
// - Hysteresis counter for auto-resolution (threshold N = 10)
type IncidentTracker struct {
	cooldownNs                  int64
	consecutiveNominalThreshold int
	incidents                   map[string]*IncidentState
}

// NewIncidentTracker creates a new IncidentTracker.
func NewIncidentTracker(cooldown time.Duration, consecutiveNominalThreshold int) *IncidentTracker {
	if cooldown <= 0 {
		cooldown = 5 * time.Minute
	}
	if consecutiveNominalThreshold <= 0 {
		consecutiveNominalThreshold = 10
	}

	return &IncidentTracker{
		cooldownNs:                  cooldown.Nanoseconds(),
		consecutiveNominalThreshold: consecutiveNominalThreshold,
		incidents:                   make(map[string]*IncidentState),
	}
}

// OnAnomaly evaluates whether an anomalous reading should dispatch a FIRING alert or be suppressed by cooldown.
// Returns (shouldDispatch, isEscalation).
func (t *IncidentTracker) OnAnomaly(deviceID, metricName string, nowNs int64) (bool, bool) {
	key := fmt.Sprintf("%s:%s", deviceID, metricName)
	state, exists := t.incidents[key]
	if !exists {
		state = &IncidentState{
			DeviceID:   deviceID,
			MetricName: metricName,
		}
		t.incidents[key] = state
	}

	state.ConsecutiveNominal = 0

	// 1. Initial onset of incident
	if !state.IsFiring {
		state.IsFiring = true
		state.LastAlertTimeNs = nowNs
		return true, false
	}

	// 2. Already firing: check if cooldown window has expired
	if nowNs-state.LastAlertTimeNs >= t.cooldownNs {
		state.LastAlertTimeNs = nowNs
		return true, true // Escalation / reminder alert after cooldown
	}

	// 3. Suppressed within cooldown window
	return false, false
}

// OnNominal records a nominal reading and evaluates if the incident should auto-resolve.
// Returns (shouldResolve).
func (t *IncidentTracker) OnNominal(deviceID, metricName string) bool {
	key := fmt.Sprintf("%s:%s", deviceID, metricName)
	state, exists := t.incidents[key]
	if !exists || !state.IsFiring {
		return false
	}

	state.ConsecutiveNominal++
	if state.ConsecutiveNominal >= t.consecutiveNominalThreshold {
		state.IsFiring = false
		state.ConsecutiveNominal = 0
		return true
	}

	return false
}

// ActiveIncidentCount returns the number of currently firing incidents.
func (t *IncidentTracker) ActiveIncidentCount() int {
	count := 0
	for _, inc := range t.incidents {
		if inc.IsFiring {
			count++
		}
	}
	return count
}
