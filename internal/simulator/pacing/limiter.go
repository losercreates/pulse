package pacing

import (
	"context"
	"sync"
	"time"
)

// Limiter coordinates high-precision rate limiting and micro-batch pacing.
type Limiter struct {
	mu           sync.Mutex
	rate         float64 // events per second
	capacity     float64 // bucket capacity
	tokens       float64 // available tokens
	lastRefill   time.Time
}

// NewLimiter creates a token bucket rate limiter initialized to targetRate events/sec.
func NewLimiter(targetRate int) *Limiter {
	r := float64(targetRate)
	// Allow burst capacity of up to 10% of rate or at least 1,000 tokens
	cap := r * 0.1
	if cap < 1000 {
		cap = 1000
	}

	return &Limiter{
		rate:       r,
		capacity:   cap,
		tokens:     cap,
		lastRefill: time.Now(),
	}
}

// SetRate dynamically updates the target throughput rate (e.g. for burst mode).
func (l *Limiter) SetRate(newRate int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()
	l.rate = float64(newRate)
	newCap := l.rate * 0.1
	if newCap < 1000 {
		newCap = 1000
	}
	l.capacity = newCap
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
}

// GetRate returns current configured rate.
func (l *Limiter) GetRate() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return int(l.rate)
}

// refill adds tokens based on elapsed duration since last refill. Caller must hold l.mu.
func (l *Limiter) refill() {
	now := time.Now()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.lastRefill = now

	l.tokens += elapsed * l.rate
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
}

// Wait blocks until the requested number of tokens are available or ctx is cancelled.
func (l *Limiter) Wait(ctx context.Context, count int) bool {
	reqTokens := float64(count)

	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		l.mu.Lock()
		l.refill()

		if l.tokens >= reqTokens {
			l.tokens -= reqTokens
			l.mu.Unlock()
			return true
		}

		// Calculate wait duration until enough tokens accumulate
		missing := reqTokens - l.tokens
		waitSec := missing / l.rate
		l.mu.Unlock()

		waitDur := time.Duration(waitSec * float64(time.Second))
		if waitDur > 10*time.Millisecond {
			waitDur = 10 * time.Millisecond
		} else if waitDur < 100*time.Microsecond {
			waitDur = 100 * time.Microsecond
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(waitDur):
		}
	}
}
