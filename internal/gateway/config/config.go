package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the runtime configuration parameters for the Ingestion Gateway.
type Config struct {
	HTTPPort              string
	GRPCPort              string
	Brokers               []string
	TopicRaw              string
	MaxFutureSkewDuration time.Duration
	MaxPastSkewDuration   time.Duration
	ProducerBatchLinger   time.Duration

	// Security & Access Control
	AuthEnabled     bool
	TLSEnabled      bool
	TLSCertFile     string
	TLSKeyFile      string
	ClientCAFile    string
	APIKeysFile     string
	RateLimitRate   float64
	RateLimitBurst  float64
}

// Load loads configuration from environment variables with safe defaults.
func Load() *Config {
	httpPort := getEnv("HTTP_PORT", "8080")
	if !strings.HasPrefix(httpPort, ":") {
		httpPort = ":" + httpPort
	}

	grpcPort := getEnv("GRPC_PORT", "50051")
	if !strings.HasPrefix(grpcPort, ":") {
		grpcPort = ":" + grpcPort
	}

	brokersRaw := getEnv("REDPANDA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	topicRaw := getEnv("TOPIC_RAW", "telemetry.raw")

	authEnabled := getEnvBool("AUTH_ENABLED", false)
	tlsEnabled := getEnvBool("TLS_ENABLED", false)
	tlsCertFile := getEnv("TLS_CERT_FILE", "")
	tlsKeyFile := getEnv("TLS_KEY_FILE", "")
	clientCAFile := getEnv("CLIENT_CA_FILE", "")
	apiKeysFile := getEnv("API_KEYS_FILE", "")

	// If certificates or key files are provided, enable auth automatically
	if clientCAFile != "" || apiKeysFile != "" || tlsCertFile != "" {
		authEnabled = true
	}
	if tlsCertFile != "" && tlsKeyFile != "" {
		tlsEnabled = true
	}

	rateLimitRate := getEnvFloat("RATE_LIMIT_RATE", 1000.0)
	rateLimitBurst := getEnvFloat("RATE_LIMIT_BURST", 2000.0)

	return &Config{
		HTTPPort:              httpPort,
		GRPCPort:              grpcPort,
		Brokers:               brokers,
		TopicRaw:              topicRaw,
		MaxFutureSkewDuration: 5 * time.Minute,
		MaxPastSkewDuration:   24 * time.Hour,
		ProducerBatchLinger:   5 * time.Millisecond,

		AuthEnabled:    authEnabled,
		TLSEnabled:     tlsEnabled,
		TLSCertFile:    tlsCertFile,
		TLSKeyFile:     tlsKeyFile,
		ClientCAFile:   clientCAFile,
		APIKeysFile:    apiKeysFile,
		RateLimitRate:  rateLimitRate,
		RateLimitBurst: rateLimitBurst,
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		parsed, err := strconv.ParseBool(val)
		if err == nil {
			return parsed
		}
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		parsed, err := strconv.ParseFloat(val, 64)
		if err == nil {
			return parsed
		}
	}
	return defaultVal
}
