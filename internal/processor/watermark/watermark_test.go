package watermark

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWatermarkProgression(t *testing.T) {
	allowedLateness := 5 * time.Second
	idleTimeout := 10 * time.Second
	tracker := NewTracker(0, allowedLateness, idleTimeout)

	// Base time: 10:00:00.000 (100s in nanoseconds)
	baseTime := int64(100 * time.Second)

	// First event at 100s
	advanced, wm := tracker.ProcessEvent(baseTime)
	assert.True(t, advanced)
	assert.Equal(t, int64(95*time.Second), wm)
	assert.Equal(t, baseTime, tracker.MaxEventTime())
	assert.Equal(t, int64(95*time.Second), tracker.CurrentWatermark())

	// Out-of-order event within allowed lateness (98s arrives after 100s)
	advanced, wm = tracker.ProcessEvent(int64(98 * time.Second))
	assert.False(t, advanced)
	assert.Equal(t, int64(95*time.Second), wm) // Watermark does not regress
	assert.False(t, tracker.IsLate(int64(98*time.Second)))
	assert.Equal(t, int64(0), tracker.LatenessDuration(int64(98*time.Second)))

	// Later event at 110s
	advanced, wm = tracker.ProcessEvent(int64(110 * time.Second))
	assert.True(t, advanced)
	assert.Equal(t, int64(105*time.Second), wm) // 110s - 5s = 105s
	assert.Equal(t, int64(110*time.Second), tracker.MaxEventTime())

	// Excessively late event at 104s (older than 105s watermark)
	lateTimestamp := int64(104 * time.Second)
	assert.True(t, tracker.IsLate(lateTimestamp))
	assert.Equal(t, int64(1*time.Second), tracker.LatenessDuration(lateTimestamp))

	advanced, wm = tracker.ProcessEvent(lateTimestamp)
	assert.False(t, advanced)
	assert.Equal(t, int64(105*time.Second), wm)
}

func TestWatermarkIdleTimeout(t *testing.T) {
	allowedLateness := 5 * time.Second
	idleTimeout := 2 * time.Second
	tracker := NewTracker(1, allowedLateness, idleTimeout)

	baseTime := int64(100 * time.Second)
	now := time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC)
	tracker.ProcessEventWithTime(baseTime, now)
	initialWM := tracker.CurrentWatermark()
	assert.Equal(t, int64(95*time.Second), initialWM)

	// Check timeout immediately - should not advance
	advanced, wm := tracker.CheckIdleTimeout(now)
	assert.False(t, advanced)
	assert.Equal(t, initialWM, wm)

	// Simulate idle timeout by passing now + 3s
	futureNow := now.Add(3 * time.Second)
	advanced, wm = tracker.CheckIdleTimeout(futureNow)
	assert.True(t, advanced)
	assert.Greater(t, wm, initialWM)
	assert.Equal(t, initialWM+int64(3*time.Second), wm)
}

func TestWatermarkMonotonicity(t *testing.T) {
	tracker := NewTracker(0, 5*time.Second, 10*time.Second)

	// Shuffled sequence of timestamps in nanoseconds
	timestamps := []int64{
		int64(10 * time.Second),
		int64(14 * time.Second),
		int64(12 * time.Second), // out of order
		int64(11 * time.Second), // out of order
		int64(20 * time.Second),
		int64(15 * time.Second), // out of order
		int64(25 * time.Second),
	}

	var prevWM int64
	for _, ts := range timestamps {
		tracker.ProcessEvent(ts)
		currWM := tracker.CurrentWatermark()
		assert.GreaterOrEqual(t, currWM, prevWM, "Watermark must never regress!")
		prevWM = currWM
	}
}
