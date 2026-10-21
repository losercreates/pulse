package property

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/processor/window"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rawEvent struct {
	deviceID    string
	metricName  string
	value       float64
	timestampNs int64
}

func runStream(events []rawEvent, calc *window.WindowBoundaryCalculator, finalWatermarkNs int64) map[string]*telemetryv1.WindowAggregate {
	wm := window.NewWindowManager(0, calc)
	for _, e := range events {
		wm.ProcessReading(e.timestampNs, e.deviceID, e.metricName, e.value)
	}

	emitted := wm.AdvanceWatermark(finalWatermarkNs)
	results := make(map[string]*telemetryv1.WindowAggregate)
	for _, agg := range emitted {
		key := fmt.Sprintf("%s:%s:[%d,%d)", agg.DeviceId, agg.MetricName, agg.WindowStartNs, agg.WindowEndNs)
		results[key] = agg
	}
	return results
}

func TestProperty_ArrivalOrderInvariance(t *testing.T) {
	// 10s tumbling, 60s sliding with 10s slide
	calc := window.NewBoundaryCalculator(10*time.Second, 60*time.Second, 10*time.Second)

	devices := []string{"sensor-A", "sensor-B", "sensor-C", "sensor-D"}
	metric := "vibration"

	// Generate 1000 events spanning 100 seconds
	numEvents := 1000
	baseTimeNs := int64(100 * time.Second)
	finalWatermarkNs := int64(250 * time.Second)

	r := rand.New(rand.NewSource(42))

	inOrderEvents := make([]rawEvent, numEvents)
	for i := 0; i < numEvents; i++ {
		// Monotonically non-decreasing timestamps
		offsetMs := int64(i * 100) // 100ms apart
		inOrderEvents[i] = rawEvent{
			deviceID:    devices[r.Intn(len(devices))],
			metricName:  metric,
			value:       10.0 + r.Float64()*90.0,
			timestampNs: baseTimeNs + offsetMs*int64(time.Millisecond),
		}
	}

	// 1. Process in strict chronological order
	inOrderAggs := runStream(inOrderEvents, calc, finalWatermarkNs)
	require.NotEmpty(t, inOrderAggs)

	// 2. Perform property test with multiple randomized permutations
	iterations := 10
	for iter := 0; iter < iterations; iter++ {
		shuffledEvents := make([]rawEvent, len(inOrderEvents))
		copy(shuffledEvents, inOrderEvents)

		// Shuffle events locally within sliding buckets to simulate network jitter
		// within allowed lateness (jitter <= 4 seconds)
		r.Shuffle(len(shuffledEvents), func(i, j int) {
			// Only swap if time difference <= 4s to simulate out-of-order within lateness
			diff := shuffledEvents[i].timestampNs - shuffledEvents[j].timestampNs
			if diff < 0 {
				diff = -diff
			}
			if diff <= int64(4*time.Second) {
				shuffledEvents[i], shuffledEvents[j] = shuffledEvents[j], shuffledEvents[i]
			}
		})

		shuffledAggs := runStream(shuffledEvents, calc, finalWatermarkNs)

		// Validate aggregate invariance property
		assert.Equal(t, len(inOrderAggs), len(shuffledAggs), "Number of closed windows must match for iter %d", iter)

		for key, expected := range inOrderAggs {
			actual, exists := shuffledAggs[key]
			require.True(t, exists, "Window %s must exist in shuffled stream", key)

			assert.Equal(t, expected.Count, actual.Count, "Count mismatch for window %s", key)
			assert.InDelta(t, expected.Sum, actual.Sum, 1e-6, "Sum mismatch for window %s", key)
			assert.InDelta(t, expected.Min, actual.Min, 1e-6, "Min mismatch for window %s", key)
			assert.InDelta(t, expected.Max, actual.Max, 1e-6, "Max mismatch for window %s", key)
			assert.InDelta(t, expected.Mean, actual.Mean, 1e-6, "Mean mismatch for window %s", key)
			assert.Equal(t, expected.ApproxDistinctCount, actual.ApproxDistinctCount, "HLL distinct count mismatch for window %s", key)
		}
	}
}
