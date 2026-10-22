package detector

import (
	"math"
)

// StatisticalBaseline maintains the partition-local dynamic statistical state per series.
// Verbatim Constraints:
// - device_id: Machine or sensor identifier (1 to 128 characters).
// - Minimum N_warmup = 30 samples before alert evaluation.
// - guarded by sigma^2 >= epsilon.
type StatisticalBaseline struct {
	DeviceID        string  `json:"device_id"`
	MetricName      string  `json:"metric_name"`
	Mean            float64 `json:"mean"`
	Variance        float64 `json:"variance"`
	SampleCount     uint64  `json:"sample_count"`
	LastUpdateNs    int64   `json:"last_update_ns"`
	alpha           float64
	varianceEpsilon float64
	warmupSamples   uint64
}

// NewStatisticalBaseline creates a new baseline tracker for a (device_id, metric_name) series.
func NewStatisticalBaseline(deviceID, metricName string, alpha, varianceEpsilon float64, warmupSamples int) *StatisticalBaseline {
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.05
	}
	if varianceEpsilon <= 0 {
		varianceEpsilon = 0.0001
	}
	if warmupSamples <= 0 {
		warmupSamples = 30
	}

	return &StatisticalBaseline{
		DeviceID:        deviceID,
		MetricName:      metricName,
		Variance:        varianceEpsilon,
		alpha:           alpha,
		varianceEpsilon: varianceEpsilon,
		warmupSamples:   uint64(warmupSamples),
	}
}

// Update incorporates an observed measurement into the running EWMA mean and rolling variance.
// Returns (mean, stdDev, isWarm).
func (b *StatisticalBaseline) Update(val float64, tsNs int64) (float64, float64, bool) {
	b.LastUpdateNs = tsNs
	b.SampleCount++

	if b.SampleCount == 1 {
		b.Mean = val
		b.Variance = b.varianceEpsilon
		return b.Mean, math.Sqrt(b.Variance), false
	}

	diff := val - b.Mean
	b.Mean += b.alpha * diff

	// Rolling variance update: sigma^2_t = (1 - alpha) * (sigma^2_{t-1} + alpha * diff^2)
	b.Variance = (1.0 - b.alpha) * (b.Variance + b.alpha*diff*diff)

	// Defensive variance floor guardrail to prevent flatline false-positive storms
	if b.Variance < b.varianceEpsilon {
		b.Variance = b.varianceEpsilon
	}

	stdDev := math.Sqrt(b.Variance)
	isWarm := b.SampleCount >= b.warmupSamples

	return b.Mean, stdDev, isWarm
}

// StdDev returns the current standard deviation guarded by the variance floor.
func (b *StatisticalBaseline) StdDev() float64 {
	if b.Variance < b.varianceEpsilon {
		return math.Sqrt(b.varianceEpsilon)
	}
	return math.Sqrt(b.Variance)
}

// IsWarm returns true if the series has accumulated enough observations to evaluate anomalies.
func (b *StatisticalBaseline) IsWarm() bool {
	return b.SampleCount >= b.warmupSamples
}
