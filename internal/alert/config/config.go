package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds runtime configuration parameters for the Anomaly Detection & Alert Dispatcher.
type Config struct {
	Brokers                     []string
	GroupID                     string
	TopicAlerts                 string
	TopicDeadLetter             string
	HTTPPort                    string
	EWMAAlpha                   float64
	KSigma                      float64
	VarianceEpsilon             float64
	WarmupSamples               int
	CooldownDuration            time.Duration
	ConsecutiveNominalThreshold int
	WebhookURL                  string
	WorkerPoolSize              int
	MaxRetries                  int
	CircuitBreakerThreshold     int
	CircuitBreakerCoolOff       time.Duration
}

// Load loads configuration from environment variables with safe defaults.
func Load() *Config {
	brokersRaw := getEnv("REDPANDA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	httpPort := getEnv("HTTP_PORT", "8083")
	if !strings.HasPrefix(httpPort, ":") {
		httpPort = ":" + httpPort
	}

	return &Config{
		Brokers:                     brokers,
		GroupID:                     getEnv("ALERT_GROUP_ID", "pulse-alert-dispatcher"),
		TopicAlerts:                 getEnv("TOPIC_ALERTS", "telemetry.alerts"),
		TopicDeadLetter:             getEnv("TOPIC_ALERTS_DEADLETTER", "telemetry.alerts.deadletter"),
		HTTPPort:                    httpPort,
		EWMAAlpha:                   getFloatEnv("ALERT_EWMA_ALPHA", 0.05),
		KSigma:                      getFloatEnv("ALERT_K_SIGMA", 3.0),
		VarianceEpsilon:             getFloatEnv("ALERT_VARIANCE_EPSILON", 0.0001),
		WarmupSamples:               getIntEnv("ALERT_WARMUP_SAMPLES", 30),
		CooldownDuration:            getDurationEnv("ALERT_COOLDOWN_DURATION", 5*time.Minute),
		ConsecutiveNominalThreshold: getIntEnv("ALERT_CONSECUTIVE_NOMINAL", 10),
		WebhookURL:                  getEnv("ALERT_WEBHOOK_URL", "http://localhost:8089/webhook"),
		WorkerPoolSize:              getIntEnv("ALERT_WORKER_POOL_SIZE", 10),
		MaxRetries:                  getIntEnv("ALERT_MAX_RETRIES", 3),
		CircuitBreakerThreshold:     getIntEnv("ALERT_CB_THRESHOLD", 5),
		CircuitBreakerCoolOff:       getDurationEnv("ALERT_CB_COOLOFF", 30*time.Second),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getFloatEnv(key string, defaultVal float64) float64 {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
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
