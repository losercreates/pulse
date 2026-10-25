package signal_test

import (
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/signal"
	"github.com/stretchr/testify/assert"
)

func TestSignalGenerator_Distribution(t *testing.T) {
	gen := signal.NewGenerator(50.0, 5.0, 0.1, 0.5)

	var sum float64
	minVal := 1000.0
	maxVal := -1000.0
	samples := 1000 // 1000 * 10ms = 10s, exactly 1 complete period of 0.1 Hz sine wave

	for i := 0; i < samples; i++ {
		val := gen.Next(time.Duration(i) * 10 * time.Millisecond)
		sum += val
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
	}

	mean := sum / float64(samples)
	// Mean should oscillate around base value (50.0)
	assert.InDelta(t, 50.0, mean, 2.0, "Signal mean should be close to nominal base")
	assert.GreaterOrEqual(t, maxVal, 50.0, "Signal peak should exceed base")
	assert.LessOrEqual(t, minVal, 50.0, "Signal valley should be below base")
}

func TestSignalGenerator_FreezeAndUnfreeze(t *testing.T) {
	gen := signal.NewGenerator(50.0, 5.0, 0.1, 0.5)
	gen.Freeze(99.9)

	for i := 0; i < 20; i++ {
		assert.Equal(t, 99.9, gen.Next(time.Duration(i)*time.Millisecond))
	}

	gen.Unfreeze()
	unfrozen := gen.Next(1 * time.Second)
	assert.NotEqual(t, 99.9, unfrozen)
}

func TestSignalGenerator_Drift(t *testing.T) {
	gen := signal.NewGenerator(50.0, 0.0, 0.0, 0.0) // No sine, no noise
	gen.SetDrift(0.1)

	for i := 1; i <= 10; i++ {
		val := gen.Next(time.Duration(i) * time.Millisecond)
		expected := 50.0 + float64(i)*0.1
		assert.InDelta(t, expected, val, 1e-6)
	}
}
