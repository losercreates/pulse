package chaos

import (
	"math/rand/v2"
	"sync/atomic"

	"github.com/losercreates/pulse/internal/simulator/fleet"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// AnomalyInjector injects deliberate physical sensor anomalies into readings.
type AnomalyInjector struct {
	anomalyPct    float64
	spikeCount    atomic.Int64
	flatlineCount atomic.Int64
	driftCount    atomic.Int64
}

// NewAnomalyInjector creates a sensor failure chaos injector.
func NewAnomalyInjector(anomalyPct float64) *AnomalyInjector {
	if anomalyPct < 0 {
		anomalyPct = 0
	}
	return &AnomalyInjector{
		anomalyPct: anomalyPct,
	}
}

// MaybeInject checks whether a reading should be mutated with an anomaly.
// Returns true if a step spike was injected.
func (a *AnomalyInjector) MaybeInject(r *telemetryv1.TelemetryReading, baseVal, noiseSigma float64) bool {
	if a.anomalyPct <= 0 || r == nil {
		return false
	}

	if rand.Float64() < a.anomalyPct {
		// Inject step spike (z >= 5.0)
		zScore := 6.0
		spikeDelta := zScore * noiseSigma
		if spikeDelta < 2.0 {
			spikeDelta = baseVal*0.5 + 2.5
		}

		// Randomly spike positive or negative
		if rand.Float64() < 0.5 {
			r.Value += spikeDelta
		} else {
			r.Value -= spikeDelta
		}

		a.spikeCount.Add(1)
		return true
	}

	return false
}

// InjectFlatline freezes the sensor output of a target device to a constant value.
func (a *AnomalyInjector) InjectFlatline(dev *fleet.Device, val float64) {
	if dev != nil {
		dev.Freeze(val)
		a.flatlineCount.Add(1)
	}
}

// InjectDrift imposes a continuous baseline drift rate on a target device.
func (a *AnomalyInjector) InjectDrift(dev *fleet.Device, driftRate float64) {
	if dev != nil {
		dev.SetDrift(driftRate)
		a.driftCount.Add(1)
	}
}

// SpikeCount returns total step spikes injected.
func (a *AnomalyInjector) SpikeCount() int64 {
	return a.spikeCount.Load()
}

// FlatlineCount returns total devices frozen in flatline mode.
func (a *AnomalyInjector) FlatlineCount() int64 {
	return a.flatlineCount.Load()
}

// DriftCount returns total devices undergoing calibration drift.
func (a *AnomalyInjector) DriftCount() int64 {
	return a.driftCount.Load()
}
