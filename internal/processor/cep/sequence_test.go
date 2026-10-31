package cep_test

import (
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/processor/cep"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCEPEngine_TemporalSequenceMatch(t *testing.T) {
	rule := cep.CEPRule{
		RuleID:         "RULE-CASCADE-01",
		Name:           "Vibration to Temp Runaway",
		DevicePattern:  "turbine-*",
		WindowDuration: 30 * time.Second,
		PatternType:    cep.PatternTypeSequence,
		SequenceSteps: []cep.PatternStep{
			{
				StepIndex:       0,
				MetricName:      "vibration",
				Condition:       "z_score>=",
				ZScoreThreshold: 3.5,
			},
			{
				StepIndex:  1,
				MetricName: "bearing_temp",
				Condition:  ">=",
				Threshold:  90.0,
			},
		},
		Severity: "CRITICAL",
		Cooldown: 1 * time.Minute,
	}

	engine := cep.NewEngine(0) // Partition 0
	require.NoError(t, engine.ReloadRules([]cep.CEPRule{rule}))

	baseTime := time.Date(2026, 10, 31, 10, 0, 0, 0, time.UTC).UnixNano()

	// Step 1: Ingest Step 0 match (vibration spike with z_score = 4.0)
	r1 := &telemetryv1.TelemetryReading{
		DeviceId:    "turbine-01",
		MetricName:  "vibration",
		Value:       8.5,
		TimestampNs: baseTime,
	}
	alerts1 := engine.ProcessReading(r1, 4.0)
	assert.Empty(t, alerts1) // Not yet complete
	assert.Equal(t, 1, engine.ActiveCandidatesCount())

	// Step 2: Ingest non-matching intermediate metric (e.g. rpm)
	rIntermediate := &telemetryv1.TelemetryReading{
		DeviceId:    "turbine-01",
		MetricName:  "rpm",
		Value:       3000,
		TimestampNs: baseTime + int64(5*time.Second),
	}
	alertsIntermediate := engine.ProcessReading(rIntermediate, 0.5)
	assert.Empty(t, alertsIntermediate)
	assert.Equal(t, 1, engine.ActiveCandidatesCount())

	// Step 3: Ingest Step 1 match within window (15s after Step 0)
	r2 := &telemetryv1.TelemetryReading{
		DeviceId:    "turbine-01",
		MetricName:  "bearing_temp",
		Value:       94.5,
		TimestampNs: baseTime + int64(15*time.Second),
	}
	alerts2 := engine.ProcessReading(r2, 2.8)
	require.Len(t, alerts2, 1)

	alert := alerts2[0]
	assert.Equal(t, "turbine-01", alert.DeviceId)
	assert.Equal(t, "bearing_temp", alert.MetricName)
	assert.Equal(t, 94.5, alert.TriggerValue)
	assert.Equal(t, telemetryv1.AlertSeverity_SEVERITY_CRITICAL, alert.Severity)
	assert.Equal(t, telemetryv1.AlertStatus_STATUS_FIRING, alert.Status)
	assert.Equal(t, int32(0), alert.PartitionId)

	// Candidate state should be cleared after completion
	assert.Equal(t, 0, engine.ActiveCandidatesCount())
}

func TestCEPEngine_WatermarkExpiration(t *testing.T) {
	rule := cep.CEPRule{
		RuleID:         "RULE-EXPIRE-01",
		Name:           "Vibration to Temp Window Expiration",
		DevicePattern:  "*",
		WindowDuration: 20 * time.Second,
		PatternType:    cep.PatternTypeSequence,
		SequenceSteps: []cep.PatternStep{
			{StepIndex: 0, MetricName: "vibration", Condition: ">=", Threshold: 5.0},
			{StepIndex: 1, MetricName: "temp", Condition: ">=", Threshold: 80.0},
		},
		Severity: "WARNING",
	}

	engine := cep.NewEngine(1)
	require.NoError(t, engine.ReloadRules([]cep.CEPRule{rule}))

	baseTime := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC).UnixNano()

	// Step 0 arrives at baseTime
	r1 := &telemetryv1.TelemetryReading{
		DeviceId:    "motor-01",
		MetricName:  "vibration",
		Value:       6.2,
		TimestampNs: baseTime,
	}
	_ = engine.ProcessReading(r1, 0.0)
	assert.Equal(t, 1, engine.ActiveCandidatesCount())

	// Watermark advances to baseTime + 10s (before deadline of baseTime + 20s)
	engine.AdvanceWatermark(baseTime + int64(10*time.Second))
	assert.Equal(t, 1, engine.ActiveCandidatesCount())

	// Watermark advances to baseTime + 25s (past deadline)
	engine.AdvanceWatermark(baseTime + int64(25*time.Second))
	assert.Equal(t, 0, engine.ActiveCandidatesCount())

	// If step 1 arrives now at baseTime + 26s, it should NOT trigger alert because candidate expired
	r2 := &telemetryv1.TelemetryReading{
		DeviceId:    "motor-01",
		MetricName:  "temp",
		Value:       85.0,
		TimestampNs: baseTime + int64(26*time.Second),
	}
	alerts := engine.ProcessReading(r2, 0.0)
	assert.Empty(t, alerts)
}

