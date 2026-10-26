package integration_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

type alertRulesConfig struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []struct {
			Alert       string            `yaml:"alert"`
			Expr        string            `yaml:"expr"`
			For         string            `yaml:"for"`
			Labels      map[string]string `yaml:"labels"`
			Annotations map[string]string `yaml:"annotations"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

func TestAlertRules_SyntaxAndThresholds(t *testing.T) {
	data, err := os.ReadFile("../../deployments/prometheus/alert_rules.yml")
	require.NoError(t, err, "deployments/prometheus/alert_rules.yml must exist")

	var cfg alertRulesConfig
	err = yaml.Unmarshal(data, &cfg)
	require.NoError(t, err, "alert_rules.yml must be valid YAML")

	require.NotEmpty(t, cfg.Groups, "At least one rule group must be defined")
	assert.Equal(t, "pulse_sla_alerts", cfg.Groups[0].Name, "Group name must be pulse_sla_alerts")

	expectedAlerts := map[string]struct {
		expectedSeverity string
		requiredSubExpr  string
	}{
		"CriticalIngestionErrorRate": {
			expectedSeverity: "critical",
			requiredSubExpr:  "0.10",
		},
		"HighConsumerLag": {
			expectedSeverity: "critical",
			requiredSubExpr:  "250000",
		},
		"WatermarkStalled": {
			expectedSeverity: "critical",
			requiredSubExpr:  "15",
		},
		"AlertCircuitBreakerOpen": {
			expectedSeverity: "warning",
			requiredSubExpr:  "pulse_alert_circuit_breaker_state",
		},
		"CanaryLatencySLABreached": {
			expectedSeverity: "critical",
			requiredSubExpr:  "2000",
		},
		"StorageQueryLatencyHigh": {
			expectedSeverity: "warning",
			requiredSubExpr:  "0.005",
		},
	}

	foundAlerts := make(map[string]bool)
	for _, rule := range cfg.Groups[0].Rules {
		foundAlerts[rule.Alert] = true
		expected, ok := expectedAlerts[rule.Alert]
		if ok {
			assert.Equal(t, expected.expectedSeverity, rule.Labels["severity"], "Alert %s severity must match", rule.Alert)
			assert.Contains(t, rule.Expr, expected.requiredSubExpr, "Alert %s expression must contain threshold", rule.Alert)
			assert.NotEmpty(t, rule.For, "Alert %s must have a 'for' duration", rule.Alert)
			assert.NotEmpty(t, rule.Annotations["summary"], "Alert %s must have summary annotation", rule.Alert)
			assert.NotEmpty(t, rule.Annotations["description"], "Alert %s must have description annotation", rule.Alert)
		}
	}

	for alert := range expectedAlerts {
		assert.True(t, foundAlerts[alert], "Alert %s must be defined in alert_rules.yml", alert)
	}
}
