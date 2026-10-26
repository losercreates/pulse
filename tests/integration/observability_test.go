package integration_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

type prometheusConfig struct {
	Global struct {
		ScrapeInterval string `yaml:"scrape_interval"`
	} `yaml:"global"`
	RuleFiles     []string `yaml:"rule_files"`
	ScrapeConfigs []struct {
		JobName       string `yaml:"job_name"`
		MetricsPath   string `yaml:"metrics_path,omitempty"`
		StaticConfigs []struct {
			Targets []string          `yaml:"targets"`
			Labels  map[string]string `yaml:"labels,omitempty"`
		} `yaml:"static_configs"`
	} `yaml:"scrape_configs"`
}

func TestPrometheusConfig_SyntaxAndTargets(t *testing.T) {
	data, err := os.ReadFile("../../deployments/prometheus/prometheus.yml")
	require.NoError(t, err, "deployments/prometheus/prometheus.yml must exist")

	var cfg prometheusConfig
	err = yaml.Unmarshal(data, &cfg)
	require.NoError(t, err, "prometheus.yml must be valid YAML")

	assert.Equal(t, "5s", cfg.Global.ScrapeInterval, "Scrape interval must be 5s")
	assert.Contains(t, cfg.RuleFiles, "/etc/prometheus/alert_rules.yml", "Rule files must include alert_rules.yml")

	expectedJobs := map[string]bool{
		"pulse-gateway":   false,
		"pulse-processor": false,
		"pulse-alertd":    false,
		"pulse-chronod":   false,
		"pulse-redpanda":  false,
	}

	for _, sc := range cfg.ScrapeConfigs {
		if _, ok := expectedJobs[sc.JobName]; ok {
			expectedJobs[sc.JobName] = true
			assert.NotEmpty(t, sc.StaticConfigs, "Job %s must have static_configs", sc.JobName)
			assert.NotEmpty(t, sc.StaticConfigs[0].Targets, "Job %s must define targets", sc.JobName)
		}
	}

	for job, found := range expectedJobs {
		assert.True(t, found, "Scrape config must include job: %s", job)
	}
}

func TestAlertmanagerConfig_SyntaxAndRoutes(t *testing.T) {
	data, err := os.ReadFile("../../deployments/alertmanager/alertmanager.yml")
	require.NoError(t, err, "deployments/alertmanager/alertmanager.yml must exist")

	var cfg struct {
		Route struct {
			Receiver string   `yaml:"receiver"`
			GroupBy  []string `yaml:"group_by"`
		} `yaml:"route"`
		Receivers []struct {
			Name string `yaml:"name"`
		} `yaml:"receivers"`
	}

	err = yaml.Unmarshal(data, &cfg)
	require.NoError(t, err, "alertmanager.yml must be valid YAML")

	assert.NotEmpty(t, cfg.Route.Receiver, "Default receiver must be configured")
	assert.Contains(t, cfg.Route.GroupBy, "alertname")
	assert.NotEmpty(t, cfg.Receivers, "Receivers list must not be empty")
}

func TestDockerComposeConfig_ObservabilityServices(t *testing.T) {
	data, err := os.ReadFile("../../deployments/docker-compose.yml")
	require.NoError(t, err, "deployments/docker-compose.yml must exist")

	var compose struct {
		Services map[string]struct {
			Image      string   `yaml:"image"`
			Ports      []string `yaml:"ports"`
			Volumes    []string `yaml:"volumes"`
			ExtraHosts []string `yaml:"extra_hosts"`
		} `yaml:"services"`
		Volumes map[string]interface{} `yaml:"volumes"`
	}

	err = yaml.Unmarshal(data, &compose)
	require.NoError(t, err, "docker-compose.yml must be valid YAML")

	// Verify required services exist
	requiredServices := []string{"prometheus", "grafana", "alertmanager", "redpanda"}
	for _, svc := range requiredServices {
		assert.Contains(t, compose.Services, svc, "Service %s must exist in docker-compose.yml", svc)
	}

	// Verify Prometheus
	prom := compose.Services["prometheus"]
	assert.Contains(t, prom.Ports, "9090:9090")
	assert.Contains(t, prom.ExtraHosts, "host.docker.internal:host-gateway")

	// Verify Grafana
	graf := compose.Services["grafana"]
	assert.Contains(t, graf.Ports, "3000:3000")

	// Verify Alertmanager
	alertm := compose.Services["alertmanager"]
	assert.Contains(t, alertm.Ports, "9093:9093")
	assert.Contains(t, alertm.ExtraHosts, "host.docker.internal:host-gateway")

	// Verify persistent volume declarations
	assert.Contains(t, compose.Volumes, "prometheus-data")
	assert.Contains(t, compose.Volumes, "grafana-data")
	assert.Contains(t, compose.Volumes, "alertmanager-data")
}
