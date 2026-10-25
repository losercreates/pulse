package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/chaos"
	"github.com/losercreates/pulse/internal/simulator/fleet"
	"github.com/stretchr/testify/assert"
)

func TestOfflineController_Lifecycle(t *testing.T) {
	f := fleet.NewFleet(100)
	controller := chaos.NewOfflineController(f, 0.10, 50*time.Millisecond, 50*time.Millisecond)

	assert.Equal(t, 0, controller.ActiveOfflineCount())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go controller.Run(ctx)

	// Wait for disconnect phase: 10% of 100 devices = 10 devices
	assert.Eventually(t, func() bool {
		return controller.ActiveOfflineCount() == 10
	}, 200*time.Millisecond, 10*time.Millisecond, "10 devices should be disconnected")

	// Generate readings while offline
	now := time.Now().UnixNano()
	for _, dev := range f.Devices() {
		reading := dev.NextReading(now, 1*time.Second, 0)
		if dev.IsOffline() {
			assert.Nil(t, reading, "Offline device should buffer and return nil")
		} else {
			assert.NotNil(t, reading, "Online device should return reading")
		}
	}

	// Wait for recovery phase
	assert.Eventually(t, func() bool {
		return controller.ActiveOfflineCount() == 0
	}, 300*time.Millisecond, 10*time.Millisecond, "All devices should reconnect")

	// Flushed buffer check
	replayed := controller.DrainAllBuffers()
	// All 10 offline devices should have buffered their 1 reading
	assert.Len(t, replayed, 10)
}
