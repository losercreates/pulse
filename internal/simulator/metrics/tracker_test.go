package metrics_test

import (
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/metrics"
	"github.com/stretchr/testify/assert"
)

func TestTracker_AtomicCountersAndRates(t *testing.T) {
	tracker := metrics.NewTracker()

	tracker.RecordSent(100)
	tracker.RecordFailed(5)

	snap := tracker.Snapshot()
	assert.Equal(t, int64(100), snap.TotalSent)
	assert.Equal(t, int64(5), snap.TotalFailed)
	assert.InDelta(t, 4.76, snap.ErrorRatePct, 0.1)

	tracker.RecordCanaryLatency(50 * time.Millisecond)
	tracker.RecordCanaryLatency(100 * time.Millisecond)
	tracker.RecordCanaryLatency(150 * time.Millisecond)

	snap = tracker.Snapshot()
	assert.Equal(t, 3, snap.CanaryCount)
	assert.Equal(t, 100.0, snap.CanaryP50Ms)
	assert.Equal(t, 150.0, snap.CanaryP95Ms)
}
