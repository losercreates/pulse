package chaos_test

import (
	"math"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/chaos"
	"github.com/losercreates/pulse/internal/simulator/fleet"
	telemetryv1 "github.com/losercreates/pulse/proto/v1"
	"github.com/stretchr/testify/assert"
)

func TestAnomalyInjector_StepSpikes(t *testing.T) {
	injector := chaos.NewAnomalyInjector(1.0) // 100% anomaly injection for test

	reading := &telemetryv1.TelemetryReading{
		DeviceId:    "turbine-0001",
		TimestampNs: time.Now().UnixNano(),
		MetricName:  "vibration",
		Value:       2.5,
		Unit:        "mm/s",
	}

	injected := injector.MaybeInject(reading, 2.5, 0.5)
	assert.True(t, injected)
	// With 6*sigma spike (6 * 0.5 = 3.0), magnitude of deviation from nominal base must be >= 3.0
	deviation := math.Abs(reading.Value - 2.5)
	assert.GreaterOrEqual(t, deviation, 2.99)
	assert.Equal(t, int64(1), injector.SpikeCount())
}

func TestAnomalyInjector_FlatlineAndDrift(t *testing.T) {
	f := fleet.NewFleet(1)
	dev := f.Devices()[0]

	injector := chaos.NewAnomalyInjector(0.0)

	// Inject flatline
	injector.InjectFlatline(dev, 999.9)
	assert.Equal(t, int64(1), injector.FlatlineCount())

	ts := time.Now().UnixNano()
	r1 := dev.NextReading(ts, 1*time.Second, 0)
	r2 := dev.NextReading(ts+1000, 2*time.Second, 0)
	assert.Equal(t, 999.9, r1.Value)
	assert.Equal(t, 999.9, r2.Value)

	// Inject drift
	injector.InjectDrift(dev, 0.5)
	assert.Equal(t, int64(1), injector.DriftCount())
}
