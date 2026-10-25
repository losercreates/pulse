package report

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/losercreates/pulse/internal/simulator/config"
	"github.com/losercreates/pulse/internal/simulator/metrics"
)

// ConfigMeta describes runtime benchmark configuration.
type ConfigMeta struct {
	TargetRate      int     `json:"target_rate"`
	NumDevices      int     `json:"num_devices"`
	DurationSeconds float64 `json:"duration_seconds"`
	Transport       string  `json:"transport"`
	Profile         string  `json:"profile"`
}

// ResultsMeta describes aggregate empirical performance measurements.
type ResultsMeta struct {
	TotalSent            int64   `json:"total_sent"`
	TotalFailed          int64   `json:"total_failed"`
	ErrorRatePct         float64 `json:"error_rate_pct"`
	AchievedRate         float64 `json:"achieved_rate"`
	PeakRate             float64 `json:"peak_rate"`
	CanaryProbesSent     int     `json:"canary_probes_sent"`
	CanaryProbesResolved int     `json:"canary_probes_resolved"`
	CanaryP50LatencyMs   float64 `json:"canary_p50_latency_ms"`
	CanaryP95LatencyMs   float64 `json:"canary_p95_latency_ms"`
}

// VerdictMeta describes SLA compliance outcomes.
type VerdictMeta struct {
	Compliant        bool     `json:"compliant"`
	ThroughputSLAMet bool     `json:"throughput_sla_met"`
	LatencySLAMet    bool     `json:"latency_sla_met"`
	ErrorSLAMet      bool     `json:"error_sla_met"`
	Violations       []string `json:"violations"`
}

// Report represents the final machine-readable simulation report.
type Report struct {
	Timestamp  string      `json:"timestamp"`
	Config     ConfigMeta  `json:"config"`
	Results    ResultsMeta `json:"results"`
	SLAVerdict VerdictMeta `json:"sla_verdict"`
}

// BuildReport constructs an SLA-evaluated Report from current run metrics.
func BuildReport(cfg *config.Config, snap metrics.Snapshot, canarySent, canaryResolved int) *Report {
	throughputSLAMet := snap.AverageRate >= 0.90*float64(cfg.TargetRate)
	errorSLAMet := snap.ErrorRatePct <= 0.10

	latencySLAMet := true
	if canaryResolved > 0 {
		latencySLAMet = snap.CanaryP95Ms <= 2000.0 // SLA: p95 latency < 2.0s
	}

	var violations []string
	if !throughputSLAMet {
		violations = append(violations, fmt.Sprintf("Achieved throughput %.1f ev/s fell below 90%% of target (%d ev/s)", snap.AverageRate, cfg.TargetRate))
	}
	if !errorSLAMet {
		violations = append(violations, fmt.Sprintf("Error rate %.3f%% exceeded maximum allowed threshold (0.10%%)", snap.ErrorRatePct))
	}
	if !latencySLAMet {
		violations = append(violations, fmt.Sprintf("Canary p95 end-to-end latency %.1fms exceeded 2000ms SLA threshold", snap.CanaryP95Ms))
	}

	compliant := throughputSLAMet && latencySLAMet && errorSLAMet

	return &Report{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Config: ConfigMeta{
			TargetRate:      cfg.TargetRate,
			NumDevices:      cfg.NumDevices,
			DurationSeconds: cfg.Duration.Seconds(),
			Transport:       cfg.Transport,
			Profile:         cfg.Profile,
		},
		Results: ResultsMeta{
			TotalSent:            snap.TotalSent,
			TotalFailed:          snap.TotalFailed,
			ErrorRatePct:         snap.ErrorRatePct,
			AchievedRate:         snap.AverageRate,
			PeakRate:             snap.PeakRate,
			CanaryProbesSent:     canarySent,
			CanaryProbesResolved: canaryResolved,
			CanaryP50LatencyMs:   snap.CanaryP50Ms,
			CanaryP95LatencyMs:   snap.CanaryP95Ms,
		},
		SLAVerdict: VerdictMeta{
			Compliant:        compliant,
			ThroughputSLAMet: throughputSLAMet,
			LatencySLAMet:    latencySLAMet,
			ErrorSLAMet:      errorSLAMet,
			Violations:       violations,
		},
	}
}

// RenderDashboard prints an interactive ANSI status dashboard to w.
func RenderDashboard(w io.Writer, cfg *config.Config, snap metrics.Snapshot, elapsed time.Duration, activeWorkers int, isBursting bool, offlineCount int) {
	// ANSI escape codes: \033[H (cursor home), \033[2J (clear screen) or simple progress output
	targetPct := 0.0
	if cfg.TargetRate > 0 {
		targetPct = (snap.InstantaneousRate / float64(cfg.TargetRate)) * 100.0
	}

	barWidth := 30
	filled := int((targetPct / 100.0) * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	} else if filled < 0 {
		filled = 0
	}
	bar := strings.Repeat("=", filled)
	if filled < barWidth {
		bar += ">" + strings.Repeat(" ", barWidth-filled-1)
	}

	profileStatus := strings.ToUpper(cfg.Profile)
	if isBursting {
		profileStatus += " [BURST SURGE ACTIVE]"
	}
	if offlineCount > 0 {
		profileStatus += fmt.Sprintf(" [%d NODES OFFLINE]", offlineCount)
	}

	latencyStatus := "N/A"
	if snap.CanaryCount > 0 {
		verdict := "PASS"
		if snap.CanaryP95Ms > 2000.0 {
			verdict = "FAIL"
		}
		latencyStatus = fmt.Sprintf("p50: %.2fs | p90: %.2fs | p95: %.2fs (SLA: < 2.0s [%s])",
			snap.CanaryP50Ms/1000.0, snap.CanaryP90Ms/1000.0, snap.CanaryP95Ms/1000.0, verdict)
	}

	fmt.Fprintf(w, "\r\033[K[PULSE SIM] Elapsed: %v / %v | Rate: %8.0f ev/s [%s] %5.1f%% | Sent: %10d | Err: %d (%.3f%%) | Canary: %s",
		elapsed.Round(time.Second),
		cfg.Duration.Round(time.Second),
		snap.InstantaneousRate,
		bar,
		targetPct,
		snap.TotalSent,
		snap.TotalFailed,
		snap.ErrorRatePct,
		latencyStatus,
	)
}

// PrintJSON outputs the formatted JSON report to w.
func PrintJSON(w io.Writer, rep *Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}
