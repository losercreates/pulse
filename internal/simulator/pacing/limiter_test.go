package pacing_test

import (
	"context"
	"testing"
	"time"

	"github.com/losercreates/pulse/internal/simulator/pacing"
	"github.com/stretchr/testify/assert"
)

func TestLimiter_RateAndRefill(t *testing.T) {
	limiter := pacing.NewLimiter(1000)
	assert.Equal(t, 1000, limiter.GetRate())

	ctx := context.Background()
	// Initial tokens should allow immediate consumption
	ok := limiter.Wait(ctx, 100)
	assert.True(t, ok)

	// Set dynamic rate
	limiter.SetRate(2000)
	assert.Equal(t, 2000, limiter.GetRate())
}

func TestLimiter_ContextCancellation(t *testing.T) {
	limiter := pacing.NewLimiter(10) // 10 tokens/sec
	ctx, cancel := context.WithCancel(context.Background())

	// Drain initial tokens
	limiter.Wait(ctx, 1000)

	// Cancel context while waiting
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	ok := limiter.Wait(ctx, 500)
	assert.False(t, ok, "Wait should abort on cancelled context")
}
