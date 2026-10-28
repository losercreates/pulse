package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestDockerfiles_SyntaxAndSecurity(t *testing.T) {
	dockerDir := "../../deployments/docker"

	expectedDockerfiles := map[string]struct {
		expectedPorts []string
		expectedCmd   string
	}{
		"Dockerfile.gateway": {
			expectedPorts: []string{"8080", "50051"},
			expectedCmd:   "cmd/gateway",
		},
		"Dockerfile.processor": {
			expectedPorts: []string{"8081"},
			expectedCmd:   "cmd/processor",
		},
		"Dockerfile.alertd": {
			expectedPorts: []string{"8083"},
			expectedCmd:   "cmd/alertd",
		},
		"Dockerfile.chronod": {
			expectedPorts: []string{"8084", "50054"},
			expectedCmd:   "cmd/chronod",
		},
		"Dockerfile.simulator": {
			expectedPorts: nil,
			expectedCmd:   "cmd/simulator",
		},
	}

	for file, expected := range expectedDockerfiles {
		t.Run(file, func(t *testing.T) {
			path := filepath.Join(dockerDir, file)
			data, err := os.ReadFile(path)
			require.NoError(t, err, "Dockerfile %s must exist", file)

			content := string(data)

			// 1. Multi-stage build checks
			assert.True(t, strings.Contains(content, "FROM golang:1.26") || strings.Contains(content, "FROM golang:1.23"), "Must use Go builder stage")
			assert.Contains(t, content, "AS builder", "Must name the builder stage")
			assert.Contains(t, content, "CGO_ENABLED=0", "Must disable CGO for static compilation")
			assert.Contains(t, content, expected.expectedCmd, "Must compile target command %s", expected.expectedCmd)

			// 2. Distroless non-root runtime checks
			assert.Contains(t, content, "gcr.io/distroless/static-debian12:nonroot", "Must use distroless nonroot runtime")
			assert.Contains(t, content, "USER 65532:65532", "Must enforce non-root user UID 65532")
			assert.Contains(t, content, "ENTRYPOINT", "Must specify container ENTRYPOINT")

			// 3. Port exposures
			for _, port := range expected.expectedPorts {
				assert.Contains(t, content, "EXPOSE "+port, "Must expose port %s in %s", port, file)
			}
		})
	}
}

func TestHelmChart_StructureAndValues(t *testing.T) {
	chartPath := "../../deployments/helm/pulse/Chart.yaml"
	data, err := os.ReadFile(chartPath)
	require.NoError(t, err, "Chart.yaml must exist")

	var chart struct {
		APIVersion string `yaml:"apiVersion"`
		Name       string `yaml:"name"`
		Version    string `yaml:"version"`
		AppVersion string `yaml:"appVersion"`
	}
	err = yaml.Unmarshal(data, &chart)
	require.NoError(t, err, "Chart.yaml must be valid YAML")
	assert.Equal(t, "v2", chart.APIVersion)
	assert.Equal(t, "pulse", chart.Name)

	valuesPath := "../../deployments/helm/pulse/values.yaml"
	valData, err := os.ReadFile(valuesPath)
	require.NoError(t, err, "values.yaml must exist")

	var values struct {
		Global struct {
			Redpanda struct {
				Brokers string `yaml:"brokers"`
			} `yaml:"redpanda"`
		} `yaml:"global"`
		Gateway struct {
			ReplicaCount int `yaml:"replicaCount"`
			Autoscaling  struct {
				Enabled     bool `yaml:"enabled"`
				MinReplicas int  `yaml:"minReplicas"`
				MaxReplicas int  `yaml:"maxReplicas"`
			} `yaml:"autoscaling"`
		} `yaml:"gateway"`
		Processor struct {
			ReplicaCount int `yaml:"replicaCount"`
			Autoscaling  struct {
				Enabled     bool `yaml:"enabled"`
				MinReplicas int  `yaml:"minReplicas"`
				MaxReplicas int  `yaml:"maxReplicas"`
			} `yaml:"autoscaling"`
		} `yaml:"processor"`
	}

	err = yaml.Unmarshal(valData, &values)
	require.NoError(t, err, "values.yaml must be valid YAML")

	assert.NotEmpty(t, values.Global.Redpanda.Brokers, "Brokers must be configured")
	assert.True(t, values.Gateway.Autoscaling.Enabled, "Gateway autoscaling must be enabled")
	assert.Equal(t, 2, values.Gateway.Autoscaling.MinReplicas)
	assert.Equal(t, 10, values.Gateway.Autoscaling.MaxReplicas)

	// Constitution Principle I check: Processor replicas capped at 12
	assert.True(t, values.Processor.Autoscaling.Enabled, "Processor autoscaling must be enabled")
	assert.Equal(t, 1, values.Processor.Autoscaling.MinReplicas)
	assert.Equal(t, 12, values.Processor.Autoscaling.MaxReplicas, "Stream processor maxReplicas must be capped at 12 to match partition count")
}

func TestHelmChart_TemplatesContent(t *testing.T) {
	templatesDir := "../../deployments/helm/pulse/templates"

	requiredTemplates := []string{
		"_helpers.tpl",
		"configmap.yaml",
		"secret.yaml",
		"gateway-deployment.yaml",
		"gateway-service.yaml",
		"gateway-hpa.yaml",
		"processor-deployment.yaml",
		"processor-service.yaml",
		"processor-hpa.yaml",
		"alertd-deployment.yaml",
		"alertd-service.yaml",
		"chronod-deployment.yaml",
		"chronod-statefulset.yaml",
		"chronod-headless-service.yaml",
		"chronod-service.yaml",
		"pdb.yaml",
		"simulator-job.yaml",
	}

	for _, tmpl := range requiredTemplates {
		t.Run(tmpl, func(t *testing.T) {
			path := filepath.Join(templatesDir, tmpl)
			data, err := os.ReadFile(path)
			require.NoError(t, err, "Template %s must exist", tmpl)

			content := string(data)
			assert.NotEmpty(t, strings.TrimSpace(content), "Template %s must not be empty", tmpl)

			// If it's a deployment or statefulset, assert probes and grace period
			if strings.HasSuffix(tmpl, "-deployment.yaml") || strings.HasSuffix(tmpl, "-statefulset.yaml") {
				assert.Contains(t, content, "terminationGracePeriodSeconds", "%s must specify terminationGracePeriodSeconds", tmpl)
				assert.Contains(t, content, "livenessProbe", "%s must define livenessProbe", tmpl)
				assert.Contains(t, content, "readinessProbe", "%s must define readinessProbe", tmpl)
				assert.Contains(t, content, "/healthz", "%s must probe /healthz", tmpl)
			}
		})
	}
}

func TestHelmChart_ChronoDBStatefulSetAndPVC(t *testing.T) {
	templatesDir := "../../deployments/helm/pulse/templates"

	// 1. Verify StatefulSet template
	statefulSetPath := filepath.Join(templatesDir, "chronod-statefulset.yaml")
	data, err := os.ReadFile(statefulSetPath)
	require.NoError(t, err)

	content := string(data)
	assert.Contains(t, content, "kind: StatefulSet")
	assert.Contains(t, content, "chronod-headless")
	assert.Contains(t, content, "volumeClaimTemplates")
	assert.Contains(t, content, "ReadWriteOnce")
	assert.Contains(t, content, "STORAGE_DATA_DIR")
	assert.Contains(t, content, "fsGroup: 65532")
	assert.Contains(t, content, "runAsNonRoot: true")

	// 2. Verify Headless Service template
	headlessPath := filepath.Join(templatesDir, "chronod-headless-service.yaml")
	headlessData, err := os.ReadFile(headlessPath)
	require.NoError(t, err)

	headlessContent := string(headlessData)
	assert.Contains(t, headlessContent, "clusterIP: None")
	assert.Contains(t, headlessContent, "chronod-headless")

	// 3. Verify values.yaml persistence configuration
	valuesPath := "../../deployments/helm/pulse/values.yaml"
	valData, err := os.ReadFile(valuesPath)
	require.NoError(t, err)

	var values struct {
		Chronod struct {
			Persistence struct {
				Enabled      bool   `yaml:"enabled"`
				Size         string `yaml:"size"`
				AccessMode   string `yaml:"accessMode"`
				MountPath    string `yaml:"mountPath"`
				StorageClass string `yaml:"storageClass"`
			} `yaml:"persistence"`
		} `yaml:"chronod"`
	}

	err = yaml.Unmarshal(valData, &values)
	require.NoError(t, err)
	assert.True(t, values.Chronod.Persistence.Enabled)
	assert.Equal(t, "10Gi", values.Chronod.Persistence.Size)
	assert.Equal(t, "ReadWriteOnce", values.Chronod.Persistence.AccessMode)
	assert.Equal(t, "/var/lib/pulse/chronodb", values.Chronod.Persistence.MountPath)
}

