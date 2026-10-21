package window

import (
	"fmt"
	"time"
)

// WindowType identifies the type of window aggregation.
type WindowType string

const (
	WindowTypeTumbling WindowType = "tumbling"
	WindowTypeSliding  WindowType = "sliding"
)

// WindowKey identifies an immutable window aggregate slice across partition-local state.
// Constraint: device_id is 1 to 128 characters.
// Constraint: window_start_ns is inclusive.
// Constraint: window_end_ns is exclusive.
type WindowKey struct {
	DeviceID      string     `json:"device_id"`
	MetricName    string     `json:"metric_name"`
	WindowStartNs int64      `json:"window_start_ns"`
	WindowEndNs   int64      `json:"window_end_ns"`
	Type          WindowType `json:"type"`
}

func (k WindowKey) String() string {
	return fmt.Sprintf("%s:%s:%s:[%d,%d)", k.Type, k.DeviceID, k.MetricName, k.WindowStartNs, k.WindowEndNs)
}

// WindowBoundaryCalculator computes start and end timestamps for tumbling and sliding windows.
type WindowBoundaryCalculator struct {
	tumblingSizeNs int64
	slidingSizeNs  int64
	slidingSlideNs int64
}

// NewBoundaryCalculator creates a window boundary calculator.
func NewBoundaryCalculator(tumblingSize, slidingSize, slidingSlide time.Duration) *WindowBoundaryCalculator {
	if tumblingSize <= 0 {
		tumblingSize = 10 * time.Second
	}
	if slidingSize <= 0 {
		slidingSize = 60 * time.Second
	}
	if slidingSlide <= 0 || slidingSlide > slidingSize {
		slidingSlide = 10 * time.Second
	}

	return &WindowBoundaryCalculator{
		tumblingSizeNs: tumblingSize.Nanoseconds(),
		slidingSizeNs:  slidingSize.Nanoseconds(),
		slidingSlideNs: slidingSlide.Nanoseconds(),
	}
}

// ComputeTumblingWindow calculates the single non-overlapping tumbling window for event timestamp T.
// Mathematical formula:
// WindowStart = floor(T / D) * D
// WindowEnd = WindowStart + D
func (c *WindowBoundaryCalculator) ComputeTumblingWindow(eventTimeNs int64, deviceID, metricName string) WindowKey {
	start := (eventTimeNs / c.tumblingSizeNs) * c.tumblingSizeNs
	if eventTimeNs < 0 && eventTimeNs%c.tumblingSizeNs != 0 {
		start -= c.tumblingSizeNs
	}
	end := start + c.tumblingSizeNs

	return WindowKey{
		DeviceID:      deviceID,
		MetricName:    metricName,
		WindowStartNs: start,
		WindowEndNs:   end,
		Type:          WindowTypeTumbling,
	}
}

// ComputeSlidingWindows calculates all overlapping sliding windows of duration D and slide S
// that contain event timestamp T.
// Mathematical formula:
// T - D < W_s <= T and W_s = 0 (mod S)
// WindowEnd = W_s + D
func (c *WindowBoundaryCalculator) ComputeSlidingWindows(eventTimeNs int64, deviceID, metricName string) []WindowKey {
	var windows []WindowKey

	// First slide window start <= eventTimeNs
	lastStart := (eventTimeNs / c.slidingSlideNs) * c.slidingSlideNs
	if eventTimeNs < 0 && eventTimeNs%c.slidingSlideNs != 0 {
		lastStart -= c.slidingSlideNs
	}

	// Any window start W_s where W_s > eventTimeNs - D and W_s <= eventTimeNs
	for start := lastStart; start > eventTimeNs-c.slidingSizeNs; start -= c.slidingSlideNs {
		end := start + c.slidingSizeNs
		windows = append(windows, WindowKey{
			DeviceID:      deviceID,
			MetricName:    metricName,
			WindowStartNs: start,
			WindowEndNs:   end,
			Type:          WindowTypeSliding,
		})
	}

	return windows
}

// AssignAllWindows generates both tumbling and sliding window keys for an incoming event.
func (c *WindowBoundaryCalculator) AssignAllWindows(eventTimeNs int64, deviceID, metricName string) []WindowKey {
	tumbling := c.ComputeTumblingWindow(eventTimeNs, deviceID, metricName)
	sliding := c.ComputeSlidingWindows(eventTimeNs, deviceID, metricName)

	keys := make([]WindowKey, 0, 1+len(sliding))
	keys = append(keys, tumbling)
	keys = append(keys, sliding...)
	return keys
}
