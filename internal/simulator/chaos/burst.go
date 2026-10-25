package chaos

import (
	"context"
	"sync"
	"time"
)

// RateAdjuster abstracts dynamic rate adjustment for pacing limiters.
type RateAdjuster interface {
	SetRate(rate int)
	GetRate() int
}

// BurstController periodically or reactively spikes target load generation rates.
type BurstController struct {
	limiter      RateAdjuster
	baselineRate int
	multiplier   float64
	interval     time.Duration
	duration     time.Duration
	mu           sync.RWMutex
	isBursting   bool
}

// NewBurstController creates a burst load surge controller.
func NewBurstController(limiter RateAdjuster, baselineRate int, multiplier float64, interval, duration time.Duration) *BurstController {
	if multiplier <= 1.0 {
		multiplier = 2.0
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if duration <= 0 {
		duration = 10 * time.Second
	}

	return &BurstController{
		limiter:      limiter,
		baselineRate: baselineRate,
		multiplier:   multiplier,
		interval:     interval,
		duration:     duration,
	}
}

// IsBursting indicates whether a surge event is currently active.
func (b *BurstController) IsBursting() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.isBursting
}

// CurrentMultiplier returns the effective rate multiplier currently applied.
func (b *BurstController) CurrentMultiplier() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.isBursting {
		return b.multiplier
	}
	return 1.0
}

// Run executes the periodic burst cycle until context cancellation.
func (b *BurstController) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			b.setBurst(false)
			return
		case <-time.After(b.interval):
			b.setBurst(true)

			select {
			case <-ctx.Done():
				b.setBurst(false)
				return
			case <-time.After(b.duration):
				b.setBurst(false)
			}
		}
	}
}

func (b *BurstController) setBurst(active bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.isBursting = active
	if active {
		burstRate := int(float64(b.baselineRate) * b.multiplier)
		b.limiter.SetRate(burstRate)
	} else {
		b.limiter.SetRate(b.baselineRate)
	}
}
