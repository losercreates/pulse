package detector

import (
	"fmt"
	"math"

	"github.com/losercreates/pulse/internal/alert/incident"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// AnomalyScore encapsulates the deviation analysis of an observed measurement.
type AnomalyScore struct {
	IsAnomaly    bool
	Severity     telemetryv1.AlertSeverity
	ZScore       float64
	ExpectedMean float64
	StdDev       float64
	TriggerValue float64
}

// AnomalyDetector evaluates stream readings partition-locally with zero locks.
type AnomalyDetector struct {
	alpha           float64
	kSigma          float64
	varianceEpsilon float64
	warmupSamples   int
	baselines       map[string]*StatisticalBaseline
	tracker         *incident.IncidentTracker
}

// NewAnomalyDetector creates a new partition-local anomaly detector.
func NewAnomalyDetector(alpha, kSigma, varianceEpsilon float64, warmupSamples int) *AnomalyDetector {
	if kSigma <= 0 {
		kSigma = 3.0
	}
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.05
	}
	if varianceEpsilon <= 0 {
		varianceEpsilon = 0.0001
	}
	if warmupSamples <= 0 {
		warmupSamples = 30
	}

	return &AnomalyDetector{
		alpha:           alpha,
		kSigma:          kSigma,
		varianceEpsilon: varianceEpsilon,
		warmupSamples:   warmupSamples,
		baselines:       make(map[string]*StatisticalBaseline),
	}
}

// WithIncidentTracker attaches an incident tracker to manage alert cooldown and auto-resolution.
func (d *AnomalyDetector) WithIncidentTracker(tracker *incident.IncidentTracker) *AnomalyDetector {
	d.tracker = tracker
	return d
}

// Evaluate updates the series baseline and checks if the reading violates the dynamic k-sigma band.
func (d *AnomalyDetector) Evaluate(deviceID, metricName string, value float64, tsNs int64) (AnomalyScore, bool) {
	key := fmt.Sprintf("%s:%s", deviceID, metricName)
	base, exists := d.baselines[key]
	if !exists {
		base = NewStatisticalBaseline(deviceID, metricName, d.alpha, d.varianceEpsilon, d.warmupSamples)
		d.baselines[key] = base
	}

	priorMean := base.Mean
	priorStdDev := base.StdDev()
	wasWarm := base.IsWarm()

	// Update baseline with current observation
	_, _, isWarm := base.Update(value, tsNs)

	// Suppress anomaly detection during warmup phase (N < 30)
	if !wasWarm || !isWarm {
		return AnomalyScore{
			IsAnomaly:    false,
			Severity:     telemetryv1.AlertSeverity_SEVERITY_UNSPECIFIED,
			ExpectedMean: priorMean,
			StdDev:       priorStdDev,
			TriggerValue: value,
		}, false
	}

	deviation := math.Abs(value - priorMean)
	zScore := deviation / priorStdDev

	isAnomaly := zScore >= d.kSigma
	severity := classifySeverity(zScore)

	score := AnomalyScore{
		IsAnomaly:    isAnomaly,
		Severity:     severity,
		ZScore:       zScore,
		ExpectedMean: priorMean,
		StdDev:       priorStdDev,
		TriggerValue: value,
	}

	return score, isAnomaly
}

func classifySeverity(z float64) telemetryv1.AlertSeverity {
	switch {
	case z >= 5.0:
		return telemetryv1.AlertSeverity_SEVERITY_CRITICAL
	case z >= 3.0:
		return telemetryv1.AlertSeverity_SEVERITY_WARNING
	case z >= 2.5:
		return telemetryv1.AlertSeverity_SEVERITY_INFO
	default:
		return telemetryv1.AlertSeverity_SEVERITY_UNSPECIFIED
	}
}

// BaselineCount returns the number of active series baselines tracked.
func (d *AnomalyDetector) BaselineCount() int {
	return len(d.baselines)
}

// ProcessReading evaluates the reading against the statistical baseline, tracks incident lifecycle,
// and returns an AlertEvent to publish if an alert should fire or resolve.
func (d *AnomalyDetector) ProcessReading(deviceID, metricName string, value float64, tsNs int64, partitionID int32) *telemetryv1.AlertEvent {
	key := fmt.Sprintf("%s:%s", deviceID, metricName)
	base, exists := d.baselines[key]
	if !exists {
		base = NewStatisticalBaseline(deviceID, metricName, d.alpha, d.varianceEpsilon, d.warmupSamples)
		d.baselines[key] = base
	}

	priorMean := base.Mean
	priorStdDev := base.StdDev()
	wasWarm := base.IsWarm()

	// Update baseline with current observation
	_, _, isWarm := base.Update(value, tsNs)
	if !wasWarm || !isWarm {
		return nil
	}

	deviation := math.Abs(value - priorMean)
	zScore := deviation / priorStdDev
	isAnomaly := zScore >= d.kSigma

	if isAnomaly {
		severity := classifySeverity(zScore)
		shouldDispatch := true
		if d.tracker != nil {
			shouldDispatch, _ = d.tracker.OnAnomaly(deviceID, metricName, tsNs)
		}
		if shouldDispatch {
			return &telemetryv1.AlertEvent{
				AlertId:           fmt.Sprintf("alert-%s-%s-%d", deviceID, metricName, tsNs),
				DeviceId:          deviceID,
				MetricName:        metricName,
				TriggerValue:      value,
				ExpectedMean:      priorMean,
				StandardDeviation: priorStdDev,
				ZScore:            zScore,
				Severity:          severity,
				Status:            telemetryv1.AlertStatus_STATUS_FIRING,
				TimestampNs:       tsNs,
				PartitionId:       partitionID,
			}
		}
		return nil
	}

	// Nominal reading: check for auto-resolution hysteresis
	if d.tracker != nil {
		if deviation <= 1.5*priorStdDev {
			if d.tracker.OnNominal(deviceID, metricName) {
				return &telemetryv1.AlertEvent{
					AlertId:           fmt.Sprintf("resolve-%s-%s-%d", deviceID, metricName, tsNs),
					DeviceId:          deviceID,
					MetricName:        metricName,
					TriggerValue:      value,
					ExpectedMean:      priorMean,
					StandardDeviation: priorStdDev,
					ZScore:            zScore,
					Severity:          telemetryv1.AlertSeverity_SEVERITY_INFO,
					Status:            telemetryv1.AlertStatus_STATUS_RESOLVED,
					TimestampNs:       tsNs,
					PartitionId:       partitionID,
				}
			}
		}
	}

	return nil
}
