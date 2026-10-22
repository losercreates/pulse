package dispatcher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	threshold := 5
	coolOff := 30 * time.Second

	cb := NewCircuitBreaker(threshold, coolOff)
	currentTime := time.Now()
	cb.nowFunc = func() time.Time {
		return currentTime
	}

	// 1. Initial state is CLOSED
	assert.Equal(t, StateClosed, cb.State())
	assert.True(t, cb.AllowExecution())

	// 2. 4 consecutive failures -> still CLOSED
	for i := 1; i <= 4; i++ {
		cb.RecordFailure()
		assert.Equal(t, StateClosed, cb.State())
		assert.Equal(t, i, cb.ConsecutiveFailures())
	}

	// 3. 5th failure -> trips to OPEN (verbatim constraint: consecutive_failures >= 5)
	cb.RecordFailure()
	assert.Equal(t, StateOpen, cb.State())
	assert.False(t, cb.AllowExecution())

	// 4. Calls while OPEN fail immediately with ErrCircuitBreakerOpen
	err := cb.Execute(context.Background(), func(ctx context.Context) error {
		return nil
	})
	assert.ErrorIs(t, err, ErrCircuitBreakerOpen)

	// 5. Advance time by 29s -> still OPEN (coolOff is 30s)
	currentTime = currentTime.Add(29 * time.Second)
	assert.Equal(t, StateOpen, cb.State())
	assert.False(t, cb.AllowExecution())

	// 6. Advance time by another 2s (total 31s) -> transitions to HALF_OPEN
	currentTime = currentTime.Add(2 * time.Second)
	assert.Equal(t, StateHalfOpen, cb.State())
	assert.True(t, cb.AllowExecution())

	// 7. Successful trial execution in HALF_OPEN -> resets to CLOSED
	err = cb.Execute(context.Background(), func(ctx context.Context) error {
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, StateClosed, cb.State())
	assert.Equal(t, 0, cb.ConsecutiveFailures())
}

func TestCircuitBreaker_HalfOpenFailureTripsToOpen(t *testing.T) {
	cb := NewCircuitBreaker(5, 30*time.Second)
	currentTime := time.Now()
	cb.nowFunc = func() time.Time {
		return currentTime
	}

	// Trip to OPEN
	for i := 0; i < 5; i++ {
		cb.RecordFailure()
	}
	assert.Equal(t, StateOpen, cb.State())

	// Advance beyond cool-off -> HALF_OPEN
	currentTime = currentTime.Add(35 * time.Second)
	assert.Equal(t, StateHalfOpen, cb.State())

	// Failed trial execution -> trips immediately back to OPEN
	err := cb.Execute(context.Background(), func(ctx context.Context) error {
		return errors.New("upstream service unavailable")
	})
	assert.Error(t, err)
	assert.Equal(t, StateOpen, cb.State())
}

func TestRetryWithBackoff_SuccessAfterRetries(t *testing.T) {
	cb := NewCircuitBreaker(5, 30*time.Second)
	var attempts int32

	err := RetryWithBackoff(
		context.Background(),
		3,
		5*time.Millisecond,
		20*time.Millisecond,
		cb,
		func(ctx context.Context) error {
			count := atomic.AddInt32(&attempts, 1)
			if count < 3 {
				return errors.New("transient error")
			}
			return nil
		},
	)

	require.NoError(t, err)
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	assert.Equal(t, StateClosed, cb.State())
}

func TestRetryWithBackoff_FailsFastOnCircuitOpen(t *testing.T) {
	cb := NewCircuitBreaker(2, 30*time.Second)
	var attempts int32

	err := RetryWithBackoff(
		context.Background(),
		5,
		5*time.Millisecond,
		20*time.Millisecond,
		cb,
		func(ctx context.Context) error {
			atomic.AddInt32(&attempts, 1)
			return errors.New("persistent outage")
		},
	)

	assert.Error(t, err)
	// After 2 failures, breaker trips to OPEN, so subsequent attempts are skipped
	assert.Equal(t, int32(2), atomic.LoadInt32(&attempts))
	assert.Equal(t, StateOpen, cb.State())
}
