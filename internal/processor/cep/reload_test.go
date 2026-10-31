package cep_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/losercreates/pulse/internal/processor/cep"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockReloadTarget struct {
	lastRules []cep.CEPRule
}

func (m *mockReloadTarget) ReloadCEPRules(rules []cep.CEPRule) {
	m.lastRules = rules
}

func TestRuleReloader_LiveAtomicSwap(t *testing.T) {
	tmpDir := t.TempDir()
	rulesFile := filepath.Join(tmpDir, "cep_rules.yaml")

	initialYAML := `
rules:
  - rule_id: RULE-VIBE-01
    name: Initial Vibration Rule
    device_pattern: "turbine-*"
    window_duration: 30s
    pattern_type: SEQUENCE
    sequence_steps:
      - step_index: 0
        metric_name: vibration
        condition: ">="
        threshold: 3.5
      - step_index: 1
        metric_name: temp
        condition: ">="
        threshold: 90.0
    severity: WARNING
`
	require.NoError(t, os.WriteFile(rulesFile, []byte(initialYAML), 0600))

	target := &mockReloadTarget{}
	reloader := cep.NewRuleReloader(rulesFile, target)

	// First reload
	rules, err := reloader.Reload()
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "RULE-VIBE-01", rules[0].RuleID)
	assert.Equal(t, 3.5, rules[0].SequenceSteps[0].Threshold)

	// Update rules file with a new threshold and a second rule
	updatedYAML := `
rules:
  - rule_id: RULE-VIBE-01
    name: Initial Vibration Rule
    device_pattern: "turbine-*"
    window_duration: 30s
    pattern_type: SEQUENCE
    sequence_steps:
      - step_index: 0
        metric_name: vibration
        condition: ">="
        threshold: 4.5
      - step_index: 1
        metric_name: temp
        condition: ">="
        threshold: 95.0
    severity: CRITICAL
  - rule_id: RULE-EXPR-02
    name: Efficiency Drop
    device_pattern: "*"
    window_duration: 1m
    pattern_type: EXPRESSION
    expression: "power_mw / fuel_flow < 0.70"
    severity: WARNING
`
	require.NoError(t, os.WriteFile(rulesFile, []byte(updatedYAML), 0600))

	// Live reload
	rules2, err := reloader.Reload()
	require.NoError(t, err)
	require.Len(t, rules2, 2)
	assert.Equal(t, 4.5, rules2[0].SequenceSteps[0].Threshold)
	assert.Equal(t, "RULE-EXPR-02", rules2[1].RuleID)
	assert.Equal(t, 2, len(target.lastRules))

	// Test invalid rule syntax: existing rules should not be overwritten
	badYAML := `
rules:
  - rule_id: ""
`
	require.NoError(t, os.WriteFile(rulesFile, []byte(badYAML), 0600))
	_, errBad := reloader.Reload()
	assert.Error(t, errBad)
	// target should still preserve rules2
	assert.Equal(t, 2, len(target.lastRules))
}

func TestIncidentSynthesizer(t *testing.T) {
	rule := cep.CEPRule{
		RuleID:   "RULE-SYNTH-01",
		Name:     "Bearing Failure Cascade",
		Severity: "CRITICAL",
	}

	synth := cep.NewIncidentSynthesizer()

	matched := []cep.CandidateEvent{
		{
			StepIndex:   0,
			MetricName:  "vibration",
			Value:       5.5,
			ZScore:      4.2,
			TimestampNs: 1730000000000000000,
		},
		{
			StepIndex:   1,
			MetricName:  "temperature",
			Value:       98.2,
			ZScore:      3.9,
			TimestampNs: 1730000025000000000, // +25 seconds
		},
	}

	inc := synth.SynthesizeCompositeIncident(rule, nil, 0, matched)
	assert.Equal(t, "RULE-SYNTH-01", inc.RuleID)
	assert.Equal(t, "CRITICAL", inc.Severity)
	assert.Equal(t, "vibration", inc.InitiatingMetric.MetricName)
	assert.Equal(t, 5.5, inc.InitiatingMetric.Value)
	require.Len(t, inc.CorrelatedSymptoms, 1)
	assert.Equal(t, "temperature", inc.CorrelatedSymptoms[0].MetricName)
	assert.InDelta(t, 25.0, inc.CorrelatedSymptoms[0].DeltaTimeSeconds, 1e-4)

	alert := synth.ToAlertEvent(inc, 0)
	assert.Equal(t, inc.IncidentID, alert.AlertId)
	assert.Equal(t, "vibration", alert.MetricName)
}
