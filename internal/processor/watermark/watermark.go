package watermark

import (
	"sync/atomic"
	"time"
)

// WatermarkTracker tracks monotonic event-time progression per partition.
// In accordance with Constitution Principle I, a tracker is accessed exclusively
// by its partition worker goroutine without cross-goroutine locks.
type WatermarkTracker struct {
	partitionID         int32
	allowedLatenessNs   int64
	idleTimeout         time.Duration
	maxEventTimeNs      int64
	currentWatermarkNs  int64
	lastAdvanceWallTime time.Time
	hasSeenFirstEvent   bool
}

// NewTracker creates a new WatermarkTracker for a specific partition.
// allowedLatenessNs: Configurable allowed lateness bound (default: 5 seconds).
func NewTracker(partitionID int32, allowedLateness time.Duration, idleTimeout time.Duration) *WatermarkTracker {
	if allowedLateness <= 0 {
		allowedLateness = 5 * time.Second
	}
	if idleTimeout <= 0 {
		idleTimeout = 10 * time.Second
	}

	return &WatermarkTracker{
		partitionID:         partitionID,
		allowedLatenessNs:   allowedLateness.Nanoseconds(),
		idleTimeout:         idleTimeout,
		lastAdvanceWallTime: time.Now(),
	}
}

// ProcessEvent advances the event-time clock upon observing an event timestamp.
// Returns (advanced, newWatermark).
func (t *WatermarkTracker) ProcessEvent(eventTimeNs int64) (bool, int64) {
	return t.ProcessEventWithTime(eventTimeNs, time.Now())
}

// ProcessEventWithTime advances the event-time clock using a specified wall-clock time.
func (t *WatermarkTracker) ProcessEventWithTime(eventTimeNs int64, wallClock time.Time) (bool, int64) {
	t.lastAdvanceWallTime = wallClock

	if !t.hasSeenFirstEvent {
		t.hasSeenFirstEvent = true
		t.maxEventTimeNs = eventTimeNs
		newWM := eventTimeNs - t.allowedLatenessNs
		if newWM < 0 {
			newWM = 0
		}
		t.currentWatermarkNs = newWM
		return true, newWM
	}

	if eventTimeNs > t.maxEventTimeNs {
		t.maxEventTimeNs = eventTimeNs
		newWM := eventTimeNs - t.allowedLatenessNs
		if newWM < 0 {
			newWM = 0
		}
		if newWM > t.currentWatermarkNs {
			t.currentWatermarkNs = newWM
			return true, newWM
		}
	}

	return false, t.currentWatermarkNs
}

// IsLate checks if an event timestamp is strictly older than the active watermark.
// Events older than current watermark cannot be incorporated into closed windows.
func (t *WatermarkTracker) IsLate(eventTimeNs int64) bool {
	if !t.hasSeenFirstEvent {
		return false
	}
	return eventTimeNs < t.currentWatermarkNs
}

// LatenessDuration returns how far behind the watermark an event is (watermark - timestamp_ns).
func (t *WatermarkTracker) LatenessDuration(eventTimeNs int64) int64 {
	if !t.IsLate(eventTimeNs) {
		return 0
	}
	return t.currentWatermarkNs - eventTimeNs
}

// CheckIdleTimeout checks whether wall-clock elapsed time since last observed event
// exceeds the idle partition timeout. If so, advances the watermark proportionally
// to prevent partition starvation and open window stalls.
func (t *WatermarkTracker) CheckIdleTimeout(now time.Time) (bool, int64) {
	if !t.hasSeenFirstEvent {
		return false, t.currentWatermarkNs
	}

	elapsed := now.Sub(t.lastAdvanceWallTime)
	if elapsed >= t.idleTimeout {
		advanceNs := elapsed.Nanoseconds()
		t.currentWatermarkNs += advanceNs
		t.lastAdvanceWallTime = now
		return true, t.currentWatermarkNs
	}

	return false, t.currentWatermarkNs
}

// CurrentWatermark returns the current watermark in epoch nanoseconds.
func (t *WatermarkTracker) CurrentWatermark() int64 {
	return t.currentWatermarkNs
}

// MaxEventTime returns the maximum event timestamp observed so far in epoch nanoseconds.
func (t *WatermarkTracker) MaxEventTime() int64 {
	return t.maxEventTimeNs
}

// AllowedLateness returns the configured allowed lateness duration.
func (t *WatermarkTracker) AllowedLateness() time.Duration {
	return time.Duration(atomic.LoadInt64(&t.allowedLatenessNs))
}
