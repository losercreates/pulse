package config

import (
	"flag"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config encapsulates runtime operational parameters for the load simulator.
type Config struct {
	TargetRate       int           // Target aggregate readings/sec
	NumDevices       int           // Total distinct virtual devices
	Duration         time.Duration // Total execution duration
	Transport        string        // "grpc" or "http"
	GatewayGRPCAddr  string        // Target gRPC address
	GatewayHTTPAddr  string        // Target HTTP endpoint
	ChronoQueryAddr  string        // ChronoDB query endpoint for canary checks
	Profile          string        // "steady", "burst", "offline", "anomaly", "mixed"
	BurstMultiplier  float64       // Surge rate multiplier
	BurstDuration    time.Duration // Duration of burst window
	OfflinePct       float64       // Fraction of fleet to disconnect
	OfflineDuration  time.Duration // Duration devices buffer locally
	AnomalyPct       float64       // Fraction of readings with anomalies
	CanaryInterval   time.Duration // Period between canary latency probes
	JSONOutput       bool          // Output machine-readable JSON summary
	Workers          int           // Concurrency worker goroutines
}

// Parse loads configuration from CLI flags and environment variables with safe defaults.
func Parse(args []string) (*Config, error) {
	fs := flag.NewFlagSet("simulator", flag.ContinueOnError)

	rate := fs.Int("rate", getIntEnv("SIM_TARGET_RATE", 50000), "Target aggregate events per second")
	fs.IntVar(rate, "r", *rate, "Shorthand for --rate")

	devices := fs.Int("devices", getIntEnv("SIM_NUM_DEVICES", 1000), "Number of distinct virtual devices")
	fs.IntVar(devices, "d", *devices, "Shorthand for --devices")

	duration := fs.Duration("duration", getDurationEnv("SIM_DURATION", 60*time.Second), "Test execution duration")
	fs.DurationVar(duration, "t", *duration, "Shorthand for --duration")

	transport := fs.String("transport", getEnv("SIM_TRANSPORT", "grpc"), "Transport protocol: grpc or http")
	gatewayGRPC := fs.String("gateway-grpc", getEnv("GATEWAY_GRPC_ADDR", "localhost:50051"), "Gateway gRPC target address")
	gatewayHTTP := fs.String("gateway-http", getEnv("GATEWAY_HTTP_ADDR", "http://localhost:8080"), "Gateway HTTP target address")
	chronoQuery := fs.String("chrono-query", getEnv("CHRONODB_QUERY_ADDR", "http://localhost:8084"), "ChronoDB query endpoint")

	profile := fs.String("profile", getEnv("SIM_PROFILE", "steady"), "Traffic profile: steady, burst, offline, anomaly, mixed")
	fs.StringVar(profile, "p", *profile, "Shorthand for --profile")

	burstMultiplier := fs.Float64("burst-multiplier", getFloatEnv("SIM_BURST_MULTIPLIER", 3.0), "Burst throughput multiplier")
	burstDuration := fs.Duration("burst-duration", getDurationEnv("SIM_BURST_DURATION", 15*time.Second), "Burst duration")

	offlinePct := fs.Float64("offline-pct", getFloatEnv("SIM_OFFLINE_PCT", 0.05), "Fraction of fleet undergoing disconnect")
	offlineDuration := fs.Duration("offline-duration", getDurationEnv("SIM_OFFLINE_DURATION", 30*time.Second), "Offline duration")

	anomalyPct := fs.Float64("anomaly-pct", getFloatEnv("SIM_ANOMALY_PCT", 0.02), "Percentage of readings with anomalies")
	canaryInterval := fs.Duration("canary-interval", getDurationEnv("SIM_CANARY_INTERVAL", 10*time.Second), "Canary latency interval")

	jsonOut := fs.Bool("json", getBoolEnv("SIM_JSON", false), "Output machine-readable JSON report")
	workers := fs.Int("workers", getIntEnv("SIM_WORKERS", 16), "Number of sender worker goroutines")
	fs.IntVar(workers, "w", *workers, "Shorthand for --workers")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	cfg := &Config{
		TargetRate:       *rate,
		NumDevices:       *devices,
		Duration:         *duration,
		Transport:        strings.ToLower(strings.TrimSpace(*transport)),
		GatewayGRPCAddr:  *gatewayGRPC,
		GatewayHTTPAddr:  *gatewayHTTP,
		ChronoQueryAddr:  *chronoQuery,
		Profile:          strings.ToLower(strings.TrimSpace(*profile)),
		BurstMultiplier:  *burstMultiplier,
		BurstDuration:    *burstDuration,
		OfflinePct:       *offlinePct,
		OfflineDuration:  *offlineDuration,
		AnomalyPct:       *anomalyPct,
		CanaryInterval:   *canaryInterval,
		JSONOutput:       *jsonOut,
		Workers:          *workers,
	}

	if cfg.Workers <= 0 {
		cfg.Workers = 16
	}
	if cfg.NumDevices <= 0 {
		cfg.NumDevices = 1000
	}
	if cfg.TargetRate <= 0 {
		cfg.TargetRate = 50000
	}

	return cfg, nil
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

func getFloatEnv(key string, defaultVal float64) float64 {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
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

func getBoolEnv(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return defaultVal
}
