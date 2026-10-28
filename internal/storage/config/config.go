package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration parameters for the ChronoDB Storage Sink & Query Service.
type Config struct {
	Brokers         []string
	GroupID         string
	TopicAggregates string
	HTTPPort        string
	GRPCPort        string
	NumShards          int
	BatchSize          int
	FlushInterval      time.Duration
	DataDir            string
	RetentionDays      int
	CompactionInterval time.Duration
}

// Load loads configuration from environment variables with safe defaults.
func Load() *Config {
	brokersRaw := getEnv("STORAGE_BROKERS", getEnv("REDPANDA_BROKERS", "localhost:9092"))
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	httpPort := getEnv("STORAGE_HTTP_PORT", ":8084")
	if !strings.HasPrefix(httpPort, ":") {
		httpPort = ":" + httpPort
	}

	grpcPort := getEnv("STORAGE_GRPC_PORT", ":50054")
	if !strings.HasPrefix(grpcPort, ":") {
		grpcPort = ":" + grpcPort
	}

	return &Config{
		Brokers:         brokers,
		GroupID:         getEnv("STORAGE_GROUP_ID", "pulse-chronodb-consumer"),
		TopicAggregates: getEnv("STORAGE_TOPIC_AGGREGATES", "telemetry.aggregates"),
		HTTPPort:        httpPort,
		GRPCPort:        grpcPort,
		NumShards:          getIntEnv("STORAGE_SHARDS", 16),
		BatchSize:          getIntEnv("STORAGE_BATCH_SIZE", 1000),
		FlushInterval:      getDurationEnv("STORAGE_FLUSH_INTERVAL", 100*time.Millisecond),
		DataDir:            getEnv("STORAGE_DATA_DIR", ""),
		RetentionDays:      getIntEnv("STORAGE_RETENTION_DAYS", 14),
		CompactionInterval: getDurationEnv("STORAGE_COMPACTION_INTERVAL", 5*time.Minute),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getIntEnv(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getDurationEnv(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
