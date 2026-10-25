package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/chaos"
	"github.com/losercreates/pulse/internal/simulator/pacing"
	"github.com/stretchr/testify/assert"
)

func TestBurstController_Transitions(t *testing.T) {
	limiter := pacing.NewLimiter(1000)
	controller := chaos.NewBurstController(limiter, 1000, 3.0, 50*time.Millisecond, 50*time.Millisecond)

	assert.False(t, controller.IsBursting())
	assert.Equal(t, 1.0, controller.CurrentMultiplier())
	assert.Equal(t, 1000, limiter.GetRate())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go controller.Run(ctx)

	// Wait for burst trigger (after 50ms)
	assert.Eventually(t, func() bool {
		return controller.IsBursting() && limiter.GetRate() == 3000
	}, 200*time.Millisecond, 10*time.Millisecond, "Should enter burst mode with 3x rate")

	// Wait for burst recovery (after another 50ms)
	assert.Eventually(t, func() bool {
		return !controller.IsBursting() && limiter.GetRate() == 1000
	}, 200*time.Millisecond, 10*time.Millisecond, "Should recover back to nominal baseline rate")
}
