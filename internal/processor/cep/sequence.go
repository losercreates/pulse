package cep

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// Engine implements partition-local CEPEngine conforming to Constitution Principle I (zero-lock hot-path).
type Engine struct {
	partitionID   int32
	rules         atomic.Pointer[[]CEPRule]
	candidates    *CandidateTable
	deviceMetrics map[string]map[string]metricEntry // device_id -> metric_name -> latest value & timestamp
	lastAlertTime map[string]int64                  // ruleID:deviceID -> last alert timestampNs
	exprEval      ExpressionEvaluator
}

type metricEntry struct {
	value       float64
	timestampNs int64
}

// NewEngine creates an isolated, partition-local CEP engine for a worker goroutine.
func NewEngine(partitionID int32) *Engine {
	e := &Engine{
		partitionID:   partitionID,
		candidates:    NewCandidateTable(),
		deviceMetrics: make(map[string]map[string]metricEntry),
		lastAlertTime: make(map[string]int64),
		exprEval:      NewExpressionEvaluator(),
	}
	emptyRules := make([]CEPRule, 0)
	e.rules.Store(&emptyRules)
	return e
}

// ReloadRules atomically swaps active compiled rule definitions.
func (e *Engine) ReloadRules(rules []CEPRule) error {
	copied := make([]CEPRule, len(rules))
	copy(copied, rules)
	e.rules.Store(&copied)
	return nil
}

// ActiveCandidatesCount returns current in-flight partial sequence matches.
func (e *Engine) ActiveCandidatesCount() int {
	return e.candidates.Count()
}

// AdvanceWatermark purges expired sequence candidates older than watermarkNs (Constitution Principle II).
func (e *Engine) AdvanceWatermark(watermarkNs int64) {
	e.candidates.Expire(watermarkNs)
}

// ProcessReading evaluates a single incoming telemetry reading against active rules.
func (e *Engine) ProcessReading(reading *telemetryv1.TelemetryReading, zScore float64) []*telemetryv1.AlertEvent {
	if reading == nil {
		return nil
	}

	rulesPtr := e.rules.Load()
	if rulesPtr == nil || len(*rulesPtr) == 0 {
		return nil
	}
	rules := *rulesPtr

	// Update partition-local cache of latest metric values for the device
	devMap := e.deviceMetrics[reading.DeviceId]
	if devMap == nil {
		devMap = make(map[string]metricEntry)
		e.deviceMetrics[reading.DeviceId] = devMap
	}
	devMap[reading.MetricName] = metricEntry{
		value:       reading.Value,
		timestampNs: reading.TimestampNs,
	}

	var alerts []*telemetryv1.AlertEvent

	for _, rule := range rules {
		if !matchDevice(rule.DevicePattern, reading.DeviceId) {
			continue
		}

		if rule.PatternType == PatternTypeSequence {
			if alert := e.evaluateSequence(rule, reading, zScore); alert != nil {
				alerts = append(alerts, alert)
			}
		} else if rule.PatternType == PatternTypeExpression {
			if alert := e.evaluateExpression(rule, reading, zScore, devMap); alert != nil {
				alerts = append(alerts, alert)
			}
		}
	}

	return alerts
}

func (e *Engine) evaluateSequence(rule CEPRule, reading *telemetryv1.TelemetryReading, zScore float64) *telemetryv1.AlertEvent {
	steps := rule.SequenceSteps
	if len(steps) < 2 {
		return nil
	}

	candidate := e.candidates.Get(rule.RuleID, reading.DeviceId)

	if candidate == nil {
		// Test if this reading initiates Step 0
		if matchStep(steps[0], reading, zScore) {
			windowDurationNs := int64(rule.WindowDuration)
			if windowDurationNs <= 0 {
				windowDurationNs = int64(30 * time.Second)
			}
			newCandidate := &SequenceCandidate{
				RuleID:      rule.RuleID,
				DeviceID:    reading.DeviceId,
				CurrentStep: 1, // Expect step 1 next
				StartTimeNs: reading.TimestampNs,
				DeadlineNs:  reading.TimestampNs + windowDurationNs,
				MatchedEvents: []CandidateEvent{
					{
						StepIndex:   0,
						MetricName:  reading.MetricName,
						Value:       reading.Value,
						ZScore:      zScore,
						TimestampNs: reading.TimestampNs,
					},
				},
			}
			e.candidates.Put(newCandidate)
		}
		return nil
	}

	// Candidate already in progress: check deadline
	if reading.TimestampNs > candidate.DeadlineNs {
		// Candidate expired by event time
		e.candidates.Remove(rule.RuleID, reading.DeviceId)
		return nil
	}

	// Check if reading matches next expected step
	nextStepIdx := candidate.CurrentStep
	if nextStepIdx < len(steps) && matchStep(steps[nextStepIdx], reading, zScore) {
		candidate.MatchedEvents = append(candidate.MatchedEvents, CandidateEvent{
			StepIndex:   nextStepIdx,
			MetricName:  reading.MetricName,
			Value:       reading.Value,
			ZScore:      zScore,
			TimestampNs: reading.TimestampNs,
		})
		candidate.CurrentStep++

		// If all sequence steps matched, emit composite alert!
		if candidate.CurrentStep >= len(steps) {
			e.candidates.Remove(rule.RuleID, reading.DeviceId)

			// Cooldown check
			cooldownKey := rule.RuleID + ":" + reading.DeviceId
			if last, ok := e.lastAlertTime[cooldownKey]; ok {
				if reading.TimestampNs-last < int64(rule.Cooldown) {
					return nil // In cooldown
				}
			}
			e.lastAlertTime[cooldownKey] = reading.TimestampNs

			return e.createAlert(rule, reading, zScore, candidate.MatchedEvents)
		}
	}

	return nil
}

func (e *Engine) createAlert(rule CEPRule, triggerReading *telemetryv1.TelemetryReading, zScore float64, matchedEvents []CandidateEvent) *telemetryv1.AlertEvent {
	severity := telemetryv1.AlertSeverity_SEVERITY_WARNING
	if rule.Severity == "CRITICAL" {
		severity = telemetryv1.AlertSeverity_SEVERITY_CRITICAL
	} else if rule.Severity == "INFO" {
		severity = telemetryv1.AlertSeverity_SEVERITY_INFO
	}

	alertID := fmt.Sprintf("cep-%s-%s-%d", rule.RuleID, triggerReading.DeviceId, triggerReading.TimestampNs)

	return &telemetryv1.AlertEvent{
		AlertId:           alertID,
		DeviceId:          triggerReading.DeviceId,
		MetricName:        triggerReading.MetricName,
		TriggerValue:      triggerReading.Value,
		ExpectedMean:      0,
		StandardDeviation: 0,
		ZScore:            zScore,
		Severity:          severity,
		Status:            telemetryv1.AlertStatus_STATUS_FIRING,
		TimestampNs:       triggerReading.TimestampNs,
		PartitionId:       e.partitionID,
	}
}

func matchStep(step PatternStep, reading *telemetryv1.TelemetryReading, zScore float64) bool {
	if step.MetricName != "" && step.MetricName != reading.MetricName {
		return false
	}

	cond := strings.TrimSpace(step.Condition)
	if cond == "" {
		return true
	}

	if strings.HasPrefix(cond, "z_score") {
		thresh := step.ZScoreThreshold
		if thresh == 0 {
			thresh = step.Threshold
		}
		if strings.Contains(cond, ">=") {
			return zScore >= thresh
		}
		if strings.Contains(cond, ">") {
			return zScore > thresh
		}
		if strings.Contains(cond, "<=") {
			return zScore <= thresh
		}
		if strings.Contains(cond, "<") {
			return zScore < thresh
		}
		return zScore >= thresh
	}

	switch cond {
	case ">=":
		return reading.Value >= step.Threshold
	case ">":
		return reading.Value > step.Threshold
	case "<=":
		return reading.Value <= step.Threshold
	case "<":
		return reading.Value < step.Threshold
	case "==":
		return reading.Value == step.Threshold
	case "!=":
		return reading.Value != step.Threshold
	default:
		return true
	}
}

func matchDevice(pattern, deviceID string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}
	matched, err := filepath.Match(pattern, deviceID)
	if err != nil {
		return pattern == deviceID
	}
	return matched
}

// Ensure interface adherence
var _ CEPEngine = (*Engine)(nil)

