package integration_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestHelmSecurity_NetworkPolicies(t *testing.T) {
	netpolPath := "../../deployments/helm/pulse/templates/networkpolicy.yaml"
	data, err := os.ReadFile(netpolPath)
	require.NoError(t, err, "networkpolicy.yaml must exist")

	content := string(data)

	t.Run("SC-005: Enforces default-deny ingress policy", func(t *testing.T) {
		assert.Contains(t, content, "default-deny-ingress")
		assert.Contains(t, content, "podSelector: {}")
		assert.Contains(t, content, "- Ingress")
	})

	t.Run("Gateway NetworkPolicy restricts perimeter ingress and egress", func(t *testing.T) {
		assert.Contains(t, content, "gateway-netpol")
		assert.Contains(t, content, "app.kubernetes.io/component: gateway")
		assert.Contains(t, content, "port: {{ .Values.gateway.service.httpPort | default 8080 }}")
		assert.Contains(t, content, "port: {{ .Values.gateway.service.grpcPort | default 50051 }}")
		assert.Contains(t, content, "app.kubernetes.io/component: redpanda")
		assert.Contains(t, content, "port: 9092")
		assert.Contains(t, content, "port: 53") // CoreDNS
	})

	t.Run("Processor NetworkPolicy isolates internal stream processing", func(t *testing.T) {
		assert.Contains(t, content, "processor-netpol")
		assert.Contains(t, content, "app.kubernetes.io/component: processor")
		assert.Contains(t, content, "port: {{ .Values.processor.service.httpPort | default 8081 }}")
	})

	t.Run("ChronoDB NetworkPolicy isolates historical queries", func(t *testing.T) {
		assert.Contains(t, content, "chronod-netpol")
		assert.Contains(t, content, "app.kubernetes.io/component: chronod")
		assert.Contains(t, content, "port: {{ .Values.chronod.service.httpPort | default 8084 }}")
		assert.Contains(t, content, "port: {{ .Values.chronod.service.grpcPort | default 50054 }}")
	})

	t.Run("Alert Dispatcher NetworkPolicy allows external webhook egress", func(t *testing.T) {
		assert.Contains(t, content, "alertd-netpol")
		assert.Contains(t, content, "app.kubernetes.io/component: alertd")
		assert.Contains(t, content, "port: {{ .Values.alertd.service.httpPort | default 8083 }}")
		assert.Contains(t, content, "port: 443")
		assert.Contains(t, content, "port: 80")
	})
}

func TestHelmSecurity_SecretsTemplates(t *testing.T) {
	secretsPath := "../../deployments/helm/pulse/templates/secrets.yaml"
	data, err := os.ReadFile(secretsPath)
	require.NoError(t, err, "secrets.yaml must exist")

	content := string(data)

	t.Run("SC-006: Defines native Kubernetes secrets for all microservices", func(t *testing.T) {
		assert.Contains(t, content, "kind: Secret")
		assert.Contains(t, content, "security-secrets")
		assert.Contains(t, content, "ca.crt")
		assert.Contains(t, content, "server.crt")
		assert.Contains(t, content, "server.key")
		assert.Contains(t, content, "device_keys.json")
		assert.Contains(t, content, "jwt_secret.key")
		assert.Contains(t, content, "hmac_signing_secret")
	})
}

func TestHelmSecurity_ValuesConfig(t *testing.T) {
	valuesPath := "../../deployments/helm/pulse/values.yaml"
	data, err := os.ReadFile(valuesPath)
	require.NoError(t, err, "values.yaml must exist")

	var vals struct {
		Security struct {
			NetworkPolicies struct {
				Enabled bool `yaml:"enabled"`
			} `yaml:"networkPolicies"`
			Secrets struct {
				TLS struct {
					CACert     string `yaml:"caCert"`
					ServerCert string `yaml:"serverCert"`
					ServerKey  string `yaml:"serverKey"`
				} `yaml:"tls"`
				APIKeys struct {
					DeviceKeys string `yaml:"deviceKeys"`
				} `yaml:"apiKeys"`
				JWT struct {
					Secret   string `yaml:"secret"`
					Issuer   string `yaml:"issuer"`
					Audience string `yaml:"audience"`
				} `yaml:"jwt"`
				Webhook struct {
					HMACSecret string `yaml:"hmacSecret"`
				} `yaml:"webhook"`
			} `yaml:"secrets"`
		} `yaml:"security"`
	}

	err = yaml.Unmarshal(data, &vals)
	require.NoError(t, err, "values.yaml must be valid YAML")

	assert.True(t, vals.Security.NetworkPolicies.Enabled, "NetworkPolicies must be enabled by default")
	assert.Equal(t, "pulse-auth", vals.Security.Secrets.JWT.Issuer)
	assert.Equal(t, "pulse-chronodb", vals.Security.Secrets.JWT.Audience)
}
