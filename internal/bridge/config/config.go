package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// EdgeBridgeConfig defines the master configuration for the pulse-bridge daemon.
type EdgeBridgeConfig struct {
	BridgeID        string                `json:"bridge_id" yaml:"bridge_id"`
	GatewayEndpoint string                `json:"gateway_endpoint" yaml:"gateway_endpoint"`
	MetricsPort     string                `json:"metrics_port" yaml:"metrics_port"`
	TLSClientCert   string                `json:"tls_client_cert" yaml:"tls_client_cert"`
	TLSClientKey    string                `json:"tls_client_key" yaml:"tls_client_key"`
	TLSCACert       string                `json:"tls_ca_cert" yaml:"tls_ca_cert"`
	APIKey          string                `json:"api_key" yaml:"api_key"`
	SpoolDir        string                `json:"spool_dir" yaml:"spool_dir"`
	MaxSpoolBytes   int64                 `json:"max_spool_bytes" yaml:"max_spool_bytes"`
	BatchLinger     time.Duration         `json:"batch_linger" yaml:"batch_linger"`
	BatchMaxRecords int                   `json:"batch_max_records" yaml:"batch_max_records"`
	MQTTAdapters    []MQTTAdapterConfig   `json:"mqtt" yaml:"mqtt"`
	ModbusAdapters  []ModbusAdapterConfig `json:"modbus" yaml:"modbus"`
}

// MQTTAdapterConfig defines MQTT broker connection and topic translation mappings.
type MQTTAdapterConfig struct {
	Name          string   `json:"name" yaml:"name"`
	BrokerURL     string   `json:"broker_url" yaml:"broker_url"`
	ClientID      string   `json:"client_id" yaml:"client_id"`
	Username      string   `json:"username,omitempty" yaml:"username,omitempty"`
	Password      string   `json:"password,omitempty" yaml:"password,omitempty"`
	Topics        []string `json:"topics" yaml:"topics"`
	Format        string   `json:"format" yaml:"format"` // "json" or "sparkplug_b"
	TopicPattern  string   `json:"topic_pattern,omitempty" yaml:"topic_pattern,omitempty"`
	DeviceIDField string   `json:"device_id_field,omitempty" yaml:"device_id_field,omitempty"`
}

// ModbusAdapterConfig defines cyclic polling parameters for a Modbus TCP slave.
type ModbusAdapterConfig struct {
	Name         string               `json:"name" yaml:"name"`
	Endpoint     string               `json:"endpoint" yaml:"endpoint"`
	SlaveID      uint8                `json:"slave_id" yaml:"slave_id"`
	PollInterval time.Duration        `json:"poll_interval" yaml:"poll_interval"`
	Timeout      time.Duration        `json:"timeout" yaml:"timeout"`
	Registers    []RegisterDefinition `json:"registers" yaml:"registers"`
}

// RegisterDefinition defines mapping from Modbus registers to canonical TelemetryReading.
type RegisterDefinition struct {
	DeviceID     string  `json:"device_id" yaml:"device_id"`
	MetricName   string  `json:"metric_name" yaml:"metric_name"`
	Unit         string  `json:"unit" yaml:"unit"`
	FunctionCode uint8   `json:"function_code" yaml:"function_code"` // 3 (Holding) or 4 (Input)
	StartAddress uint16  `json:"start_address" yaml:"start_address"`
	DataType     string  `json:"data_type" yaml:"data_type"` // "int16", "uint16", "float32_be", "float32_le"
	Multiplier   float64 `json:"multiplier" yaml:"multiplier"`
	Offset       float64 `json:"offset" yaml:"offset"`
}

// DefaultConfig returns an EdgeBridgeConfig with robust production defaults.
func DefaultConfig() *EdgeBridgeConfig {
	return &EdgeBridgeConfig{
		BridgeID:        "pulse-bridge-default",
		GatewayEndpoint: "localhost:50051",
		MetricsPort:     ":9095",
		SpoolDir:        "/tmp/pulse-bridge-spool",
		MaxSpoolBytes:   5 * 1024 * 1024 * 1024, // 5GB
		BatchLinger:     10 * time.Millisecond,
		BatchMaxRecords: 1000,
		MQTTAdapters:    nil,
		ModbusAdapters:  nil,
	}
}

// Load loads bridge configuration from file (if BRIDGE_CONFIG is set or filePath provided),
// overriding with any specified environment variables.
func Load(filePath string) (*EdgeBridgeConfig, error) {
	cfg := DefaultConfig()

	targetFile := filePath
	if targetFile == "" {
		targetFile = os.Getenv("BRIDGE_CONFIG")
	}

	if targetFile != "" {
		cleanPath := filepath.Clean(targetFile)
		data, err := os.ReadFile(cleanPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read bridge config file %s: %w", cleanPath, err)
		}

		if strings.HasSuffix(cleanPath, ".json") {
			if err := json.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("failed to parse JSON bridge config: %w", err)
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("failed to parse YAML bridge config: %w", err)
			}
		}
	}

	// Environment variable overrides
	if cfg.MetricsPort != "" && !strings.HasPrefix(cfg.MetricsPort, ":") {
		cfg.MetricsPort = ":" + cfg.MetricsPort
	}
	if val := os.Getenv("BRIDGE_ID"); val != "" {
		cfg.BridgeID = val
	}
	if val := os.Getenv("GATEWAY_ENDPOINT"); val != "" {
		cfg.GatewayEndpoint = val
	}
	if val := os.Getenv("METRICS_PORT"); val != "" {
		if !strings.HasPrefix(val, ":") {
			val = ":" + val
		}
		cfg.MetricsPort = val
	}
	if val := os.Getenv("TLS_CLIENT_CERT"); val != "" {
		cfg.TLSClientCert = val
	}
	if val := os.Getenv("TLS_CLIENT_KEY"); val != "" {
		cfg.TLSClientKey = val
	}
	if val := os.Getenv("TLS_CA_CERT"); val != "" {
		cfg.TLSCACert = val
	}
	if val := os.Getenv("API_KEY"); val != "" {
		cfg.APIKey = val
	}
	if val := os.Getenv("SPOOL_DIR"); val != "" {
		cfg.SpoolDir = filepath.Clean(val)
	}
	if val := os.Getenv("MAX_SPOOL_BYTES"); val != "" {
		if bytes, err := strconv.ParseInt(val, 10, 64); err == nil && bytes > 0 {
			cfg.MaxSpoolBytes = bytes
		}
	}
	if val := os.Getenv("BATCH_LINGER_MS"); val != "" {
		if ms, err := strconv.Atoi(val); err == nil && ms > 0 {
			cfg.BatchLinger = time.Duration(ms) * time.Millisecond
		}
	}
	if val := os.Getenv("BATCH_MAX_RECORDS"); val != "" {
		if recs, err := strconv.Atoi(val); err == nil && recs > 0 {
			cfg.BatchMaxRecords = recs
		}
	}

	return cfg, nil
}
