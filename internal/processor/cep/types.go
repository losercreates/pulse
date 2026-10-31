package cep

import (
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
)

// PatternType denotes the rule evaluation strategy.
type PatternType string

const (
	PatternTypeSequence   PatternType = "SEQUENCE"
	PatternTypeExpression PatternType = "EXPRESSION"
)

// PatternStep defines an individual stage in a multi-step temporal sequence rule.
type PatternStep struct {
	StepIndex       int     `json:"step_index" yaml:"step_index"`
	MetricName      string  `json:"metric_name" yaml:"metric_name"`
	Condition       string  `json:"condition" yaml:"condition"` // ">", ">=", "<", "<=", "==", "!=", "z_score>="
	Threshold       float64 `json:"threshold,omitempty" yaml:"threshold,omitempty"`
	ZScoreThreshold float64 `json:"z_score_threshold,omitempty" yaml:"z_score_threshold,omitempty"`
}

// CEPRule is the declarative definition for a complex event processing rule.
type CEPRule struct {
	RuleID         string        `json:"rule_id" yaml:"rule_id"`
	Name           string        `json:"name" yaml:"name"`
	Description    string        `json:"description" yaml:"description"`
	DevicePattern  string        `json:"device_pattern" yaml:"device_pattern"` // glob pattern e.g. "turbine-*"
	WindowDuration time.Duration `json:"window_duration" yaml:"window_duration"`
	PatternType    PatternType   `json:"pattern_type" yaml:"pattern_type"` // "SEQUENCE" or "EXPRESSION"
	SequenceSteps  []PatternStep `json:"sequence_steps,omitempty" yaml:"sequence_steps,omitempty"`
	Expression     string        `json:"expression,omitempty" yaml:"expression,omitempty"` // e.g. "power_mw / fuel_flow < 0.70"
	Severity       string        `json:"severity" yaml:"severity"`                         // "WARNING" or "CRITICAL"
	Cooldown       time.Duration `json:"cooldown" yaml:"cooldown"`
}

// CandidateEvent records a single matched event step within an in-flight sequence candidate.
type CandidateEvent struct {
	StepIndex   int     `json:"step_index"`
	MetricName  string  `json:"metric_name"`
	Value       float64 `json:"value"`
	ZScore      float64 `json:"z_score"`
	TimestampNs int64   `json:"timestamp_ns"`
}

// SequenceCandidate represents an in-flight sequence pattern being matched for a specific device.
// Held partition-locally without cross-goroutine locks (Constitution Principle I).
type SequenceCandidate struct {
	RuleID        string           `json:"rule_id"`
	DeviceID      string           `json:"device_id"`
	CurrentStep   int              `json:"current_step"` // Next required step index
	StartTimeNs   int64            `json:"start_time_ns"`
	DeadlineNs    int64            `json:"deadline_ns"` // StartTimeNs + WindowDuration
	MatchedEvents []CandidateEvent `json:"matched_events"`
}

// IncidentMetric details the initiating anomaly trigger in a synthesized composite incident.
type IncidentMetric struct {
	MetricName  string  `json:"metric_name"`
	Value       float64 `json:"value"`
	ZScore      float64 `json:"z_score"`
	TimestampNs int64   `json:"timestamp_ns"`
}

// IncidentSymptom captures correlated secondary metrics accompanying the primary incident trigger.
type IncidentSymptom struct {
	MetricName       string  `json:"metric_name"`
	Value            float64 `json:"value"`
	ZScore           float64 `json:"z_score"`
	DeltaTimeSeconds float64 `json:"delta_time_seconds"`
	TimestampNs      int64   `json:"timestamp_ns"`
}

// CompositeIncidentEvent represents a synthesized root-cause incident aggregating cascading symptoms.
type CompositeIncidentEvent struct {
	IncidentID         string            `json:"incident_id"`
	RuleID             string            `json:"rule_id"`
	DeviceID           string            `json:"device_id"`
	Severity           string            `json:"severity"`
	Status             string            `json:"status"`
	Summary            string            `json:"summary"`
	DetectedTimeNs     int64             `json:"detected_time_ns"`
	InitiatingMetric   IncidentMetric    `json:"initiating_metric"`
	CorrelatedSymptoms []IncidentSymptom `json:"correlated_symptoms"`
}

// CEPEngine manages stateful pattern matching and cross-metric evaluations inside a single
// partition worker goroutine. Zero-lock hot-path compliance with Constitution Principle I.
type CEPEngine interface {
	// ProcessReading evaluates a single incoming telemetry reading against active rules.
	// Returns any completed composite incident alerts triggered by this reading.
	ProcessReading(reading *telemetryv1.TelemetryReading, zScore float64) []*telemetryv1.AlertEvent

	// AdvanceWatermark purges expired sequence candidates older than watermarkNs.
	AdvanceWatermark(watermarkNs int64)

	// ReloadRules atomically swaps the active compiled rule registry.
	ReloadRules(rules []CEPRule) error

	// ActiveCandidatesCount returns current in-flight partial sequence matches.
	ActiveCandidatesCount() int
}

// RuleCompiler parses declarative YAML/JSON rule files into validated execution trees.
type RuleCompiler interface {
	// Compile validates syntax, compiles math expressions, and builds NFA transition tables.
	Compile(rawConfig []byte) ([]CEPRule, error)

	// CompileFile loads and validates rule definitions from a file path.
	CompileFile(filePath string) ([]CEPRule, error)
}

// ExpressionEvaluator parses and executes arithmetic formulas across concurrent device metrics.
type ExpressionEvaluator interface {
	// Evaluate executes the formula using the provided metric values map.
	// Example: Evaluate("power_mw / fuel_flow < 0.70", map[string]float64{"power_mw": 50, "fuel_flow": 100})
	Evaluate(expression string, values map[string]float64) (float64, error)

	// EvaluateCondition evaluates a boolean expression condition (e.g. "a / b < 0.70").
	EvaluateCondition(expression string, values map[string]float64) (bool, error)
}

