package config

import (
	"os"
	"strings"
	"time"
)

// Config holds the runtime configuration parameters for the Ingestion Gateway.
type Config struct {
	HTTPPort             string
	GRPCPort             string
	Brokers              []string
	TopicRaw             string
	MaxFutureSkewDuration time.Duration
	MaxPastSkewDuration   time.Duration
	ProducerBatchLinger  time.Duration
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

	return &Config{
		HTTPPort:             httpPort,
		GRPCPort:             grpcPort,
		Brokers:              brokers,
		TopicRaw:             topicRaw,
		MaxFutureSkewDuration: 5 * time.Minute,
		MaxPastSkewDuration:   24 * time.Hour,
		ProducerBatchLinger:  5 * time.Millisecond,
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}
