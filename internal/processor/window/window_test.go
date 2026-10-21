package window

import (
	"fmt"
	"testing"
	"time"

	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowBoundaryCalculator(t *testing.T) {
	calc := NewBoundaryCalculator(10*time.Second, 60*time.Second, 10*time.Second)

	// Event at 10:00:04 (4 seconds)
	eventNs := int64(4 * time.Second)
	tumbling := calc.ComputeTumblingWindow(eventNs, "dev-1", "temp")
	assert.Equal(t, int64(0), tumbling.WindowStartNs)
	assert.Equal(t, int64(10*time.Second), tumbling.WindowEndNs)
	assert.Equal(t, WindowTypeTumbling, tumbling.Type)

	// Event at 10:00:15 (15 seconds)
	eventNs2 := int64(15 * time.Second)
	tumbling2 := calc.ComputeTumblingWindow(eventNs2, "dev-1", "temp")
	assert.Equal(t, int64(10*time.Second), tumbling2.WindowStartNs)
	assert.Equal(t, int64(20*time.Second), tumbling2.WindowEndNs)

	// Sliding windows for 60s duration with 10s slide at event 65s:
	// Windows starting at 10s, 20s, 30s, 40s, 50s, 60s (all 6 windows)
	sliding := calc.ComputeSlidingWindows(int64(65*time.Second), "dev-1", "temp")
	assert.Len(t, sliding, 6)
	for _, w := range sliding {
		assert.Equal(t, int64(60*time.Second), w.WindowEndNs-w.WindowStartNs)
		assert.True(t, w.WindowStartNs <= int64(65*time.Second))
		assert.True(t, w.WindowEndNs > int64(65*time.Second))
	}
}

func TestWindowAccumulator(t *testing.T) {
	key := WindowKey{
		DeviceID:      "turbine-01",
		MetricName:    "vibration",
		WindowStartNs: 0,
		WindowEndNs:   int64(10 * time.Second),
		Type:          WindowTypeTumbling,
	}
	acc := NewAccumulator(key)

	readings := []float64{10.0, 20.0, 5.0, 15.0, 50.0}
	for i, val := range readings {
		acc.Add(val, fmt.Sprintf("dev-%d", i%3))
	}

	assert.Equal(t, uint64(5), acc.Count)
	assert.Equal(t, 100.0, acc.Sum)
	assert.Equal(t, 5.0, acc.Min)
	assert.Equal(t, 50.0, acc.Max)
	assert.Equal(t, 20.0, acc.Mean())

	// 3 distinct devices: dev-0, dev-1, dev-2
	distinct := acc.ApproxDistinct()
	assert.InDelta(t, 3, distinct, 1)
}

func TestWindowPriorityQueue(t *testing.T) {
	pq := NewWindowPriorityQueue()

	acc1 := NewAccumulator(WindowKey{WindowEndNs: 300})
	acc2 := NewAccumulator(WindowKey{WindowEndNs: 100})
	acc3 := NewAccumulator(WindowKey{WindowEndNs: 200})

	pq.Push(acc1)
	pq.Push(acc2)
	pq.Push(acc3)

	assert.Equal(t, 3, pq.Len())
	assert.Equal(t, int64(100), pq.Peek().Key.WindowEndNs)

	popped1 := pq.Pop()
	assert.Equal(t, int64(100), popped1.Key.WindowEndNs)
	assert.Equal(t, int64(200), pq.Peek().Key.WindowEndNs)

	popped2 := pq.Pop()
	assert.Equal(t, int64(200), popped2.Key.WindowEndNs)

	popped3 := pq.Pop()
	assert.Equal(t, int64(300), popped3.Key.WindowEndNs)

	assert.Equal(t, 0, pq.Len())
	assert.Nil(t, pq.Pop())
}

func TestWindowManagerOutofOrderAndWatermarkClosure(t *testing.T) {
	// 10s tumbling, 60s sliding with 10s slide
	calc := NewBoundaryCalculator(10*time.Second, 60*time.Second, 10*time.Second)
	manager := NewWindowManager(0, calc)

	// Ingest readings out of order for window [0, 10s)
	// Reading 1: at 8s, value 10
	manager.ProcessReading(int64(8*time.Second), "dev-1", "temperature", 10.0)
	// Reading 2: at 2s (out of order!), value 20
	manager.ProcessReading(int64(2*time.Second), "dev-1", "temperature", 20.0)
	// Reading 3: at 5s (out of order!), value 30
	manager.ProcessReading(int64(5*time.Second), "dev-1", "temperature", 30.0)

	// Open windows should include tumbling [0, 10s) plus sliding windows
	assert.Greater(t, manager.OpenWindowsCount(), 0)

	// Watermark is at 9s (window [0, 10s) must NOT close yet!)
	emitted := manager.AdvanceWatermark(int64(9 * time.Second))
	assert.Empty(t, emitted)

	// Watermark advances to 10s (window [0, 10s) must close!)
	emitted = manager.AdvanceWatermark(int64(10 * time.Second))
	require.NotEmpty(t, emitted)

	// Find the tumbling window aggregate
	var tumblingAgg *telemetryv1.WindowAggregate
	for _, agg := range emitted {
		if agg.WindowStartNs == 0 && agg.WindowEndNs == int64(10*time.Second) {
			tumblingAgg = agg
			break
		}
	}

	require.NotNil(t, tumblingAgg)
	assert.Equal(t, "dev-1", tumblingAgg.DeviceId)
	assert.Equal(t, "temperature", tumblingAgg.MetricName)
	assert.Equal(t, uint64(3), tumblingAgg.Count)
	assert.Equal(t, 60.0, tumblingAgg.Sum)
	assert.Equal(t, 10.0, tumblingAgg.Min)
	assert.Equal(t, 30.0, tumblingAgg.Max)
	assert.Equal(t, 20.0, tumblingAgg.Mean)
}
