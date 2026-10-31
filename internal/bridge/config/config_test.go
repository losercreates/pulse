package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/bridge/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	assert.Equal(t, "pulse-bridge-default", cfg.BridgeID)
	assert.Equal(t, "localhost:50051", cfg.GatewayEndpoint)
	assert.Equal(t, ":9095", cfg.MetricsPort)
	assert.Equal(t, int64(5*1024*1024*1024), cfg.MaxSpoolBytes)
	assert.Equal(t, 10*time.Millisecond, cfg.BatchLinger)
	assert.Equal(t, 1000, cfg.BatchMaxRecords)
}

func TestLoad_YAML(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "bridge.yaml")
	yamlContent := `
bridge_id: plant-alpha-bridge
gateway_endpoint: 10.0.0.5:50051
metrics_port: 9099
spool_dir: /tmp/test-spool
max_spool_bytes: 104857600
batch_linger: 20ms
batch_max_records: 500
mqtt:
  - name: factory-mqtt
    broker_url: tcp://localhost:1883
    client_id: test-client
    topics:
      - "factory/+/+/telemetry"
    format: json
modbus:
  - name: generator-plc
    endpoint: 192.168.1.100:502
    slave_id: 1
    poll_interval: 100ms
    timeout: 1s
    registers:
      - device_id: gen-01
        metric_name: oil_temp
        unit: celsius
        function_code: 3
        start_address: 40001
        data_type: float32_be
        multiplier: 0.1
        offset: 0.0
`
	require.NoError(t, os.WriteFile(configFile, []byte(yamlContent), 0600))

	cfg, err := config.Load(configFile)
	require.NoError(t, err)
	assert.Equal(t, "plant-alpha-bridge", cfg.BridgeID)
	assert.Equal(t, "10.0.0.5:50051", cfg.GatewayEndpoint)
	assert.Equal(t, ":9099", cfg.MetricsPort)
	assert.Equal(t, "/tmp/test-spool", cfg.SpoolDir)
	assert.Equal(t, int64(104857600), cfg.MaxSpoolBytes)
	assert.Equal(t, 20*time.Millisecond, cfg.BatchLinger)
	assert.Equal(t, 500, cfg.BatchMaxRecords)

	require.Len(t, cfg.MQTTAdapters, 1)
	assert.Equal(t, "factory-mqtt", cfg.MQTTAdapters[0].Name)
	assert.Equal(t, "tcp://localhost:1883", cfg.MQTTAdapters[0].BrokerURL)

	require.Len(t, cfg.ModbusAdapters, 1)
	assert.Equal(t, "generator-plc", cfg.ModbusAdapters[0].Name)
	assert.Equal(t, uint8(1), cfg.ModbusAdapters[0].SlaveID)
	require.Len(t, cfg.ModbusAdapters[0].Registers, 1)
	assert.Equal(t, "oil_temp", cfg.ModbusAdapters[0].Registers[0].MetricName)
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("BRIDGE_ID", "env-bridge")
	t.Setenv("GATEWAY_ENDPOINT", "env-gateway:50051")
	t.Setenv("BATCH_LINGER_MS", "50")
	t.Setenv("BATCH_MAX_RECORDS", "250")

	cfg, err := config.Load("")
	require.NoError(t, err)
	assert.Equal(t, "env-bridge", cfg.BridgeID)
	assert.Equal(t, "env-gateway:50051", cfg.GatewayEndpoint)
	assert.Equal(t, 50*time.Millisecond, cfg.BatchLinger)
	assert.Equal(t, 250, cfg.BatchMaxRecords)
}

