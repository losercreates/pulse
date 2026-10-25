package metrics

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Tracker provides lock-free atomic tracking for high-throughput load generation.
type Tracker struct {
	totalSent       atomic.Int64
	totalFailed     atomic.Int64
	startTime       time.Time
	mu              sync.RWMutex
	lastCheckTime   time.Time
	lastCheckSent   int64
	currentRate     float64
	peakRate        float64
	canaryLatencies []float64 // in milliseconds
}

// Snapshot contains a point-in-time metrics summary.
type Snapshot struct {
	TotalSent       int64
	TotalFailed     int64
	ElapsedSeconds  float64
	InstantaneousRate float64
	AverageRate     float64
	PeakRate        float64
	ErrorRatePct    float64
	CanaryCount     int
	CanaryP50Ms     float64
	CanaryP90Ms     float64
	CanaryP95Ms     float64
}

// NewTracker creates a new performance and latency metrics tracker.
func NewTracker() *Tracker {
	now := time.Now()
	return &Tracker{
		startTime:     now,
		lastCheckTime: now,
	}
}

// RecordSent increments the count of successfully transmitted readings.
func (t *Tracker) RecordSent(count int) {
	t.totalSent.Add(int64(count))
}

// RecordFailed increments the count of rejected or errored readings.
func (t *Tracker) RecordFailed(count int) {
	t.totalFailed.Add(int64(count))
}

// RecordCanaryLatency records an end-to-end latency measurement.
func (t *Tracker) RecordCanaryLatency(latency time.Duration) {
	ms := float64(latency.Nanoseconds()) / 1e6
	t.mu.Lock()
	defer t.mu.Unlock()
	t.canaryLatencies = append(t.canaryLatencies, ms)
}

// UpdateRate recalculates instantaneous throughput over the elapsed delta interval.
func (t *Tracker) UpdateRate() {
	now := time.Now()
	total := t.totalSent.Load()

	t.mu.Lock()
	defer t.mu.Unlock()

	deltaSec := now.Sub(t.lastCheckTime).Seconds()
	if deltaSec >= 0.2 { // update every >= 200ms
		deltaSent := total - t.lastCheckSent
		t.currentRate = float64(deltaSent) / deltaSec
		if t.currentRate > t.peakRate {
			t.peakRate = t.currentRate
		}
		t.lastCheckTime = now
		t.lastCheckSent = total
	}
}

// Snapshot computes and returns current aggregate metrics.
func (t *Tracker) Snapshot() Snapshot {
	t.UpdateRate()

	sent := t.totalSent.Load()
	failed := t.totalFailed.Load()
	elapsed := time.Since(t.startTime).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}

	totalOps := sent + failed
	errPct := 0.0
	if totalOps > 0 {
		errPct = (float64(failed) / float64(totalOps)) * 100.0
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	p50, p90, p95 := calculatePercentiles(t.canaryLatencies)

	return Snapshot{
		TotalSent:         sent,
		TotalFailed:       failed,
		ElapsedSeconds:    elapsed,
		InstantaneousRate: t.currentRate,
		AverageRate:       float64(sent) / elapsed,
		PeakRate:          t.peakRate,
		ErrorRatePct:      errPct,
		CanaryCount:       len(t.canaryLatencies),
		CanaryP50Ms:       p50,
		CanaryP90Ms:       p90,
		CanaryP95Ms:       p95,
	}
}

func calculatePercentiles(latencies []float64) (p50, p90, p95 float64) {
	n := len(latencies)
	if n == 0 {
		return 0, 0, 0
	}

	sorted := make([]float64, n)
	copy(sorted, latencies)
	sort.Float64s(sorted)

	p50Idx := int(math.Round(float64(n-1) * 0.50))
	p90Idx := int(math.Round(float64(n-1) * 0.90))
	p95Idx := int(math.Round(float64(n-1) * 0.95))

	return sorted[p50Idx], sorted[p90Idx], sorted[p95Idx]
}
