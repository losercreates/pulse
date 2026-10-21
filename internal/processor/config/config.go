package config

import (
	"os"
	"strings"
	"time"
)

// Config holds runtime configuration parameters for the Pulse Stream Processor.
type Config struct {
	Brokers            []string
	GroupID            string
	TopicRaw           string
	TopicAggregates    string
	TopicLate          string
	HTTPPort           string
	AllowedLateness    time.Duration
	IdleTimeout        time.Duration
	TumblingWindowSize time.Duration
	SlidingWindowSize  time.Duration
	SlidingSlideSize   time.Duration
}

// Load loads stream processor configuration from environment variables with safe defaults.
func Load() *Config {
	brokersRaw := getEnv("REDPANDA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	httpPort := getEnv("HTTP_PORT", "8081")
	if !strings.HasPrefix(httpPort, ":") {
		httpPort = ":" + httpPort
	}

	return &Config{
		Brokers:            brokers,
		GroupID:            getEnv("STREAM_GROUP_ID", "pulse-stream-processor"),
		TopicRaw:           getEnv("TOPIC_RAW", "telemetry.raw"),
		TopicAggregates:    getEnv("TOPIC_AGGREGATES", "telemetry.aggregates"),
		TopicLate:          getEnv("TOPIC_LATE", "telemetry.late"),
		HTTPPort:           httpPort,
		AllowedLateness:    getDurationEnv("STREAM_ALLOWED_LATENESS", 5*time.Second),
		IdleTimeout:        getDurationEnv("STREAM_IDLE_TIMEOUT", 10*time.Second),
		TumblingWindowSize: getDurationEnv("TUMBLING_WINDOW_SIZE", 10*time.Second),
		SlidingWindowSize:  getDurationEnv("SLIDING_WINDOW_SIZE", 60*time.Second),
		SlidingSlideSize:   getDurationEnv("SLIDING_SLIDE_SIZE", 10*time.Second),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
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
