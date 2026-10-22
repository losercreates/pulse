package dispatcher

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// State represents the operational state of the circuit breaker.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// ErrCircuitBreakerOpen indicates that the circuit breaker is currently open and blocking calls.
var ErrCircuitBreakerOpen = errors.New("circuit breaker is open")

// CircuitBreaker protects downstream services by failing fast when consecutive errors exceed a threshold.
// Verbatim Constraints:
// - Trips to OPEN when consecutive_failures >= 5; hold for 30s
type CircuitBreaker struct {
	mu                  sync.Mutex
	state               State
	consecutiveFailures int
	failureThreshold    int
	coolOffDuration     time.Duration
	lastStateChange     time.Time
	nowFunc             func() time.Time
}

// NewCircuitBreaker creates a new 3-state CircuitBreaker.
func NewCircuitBreaker(threshold int, coolOff time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		threshold = 5
	}
	if coolOff <= 0 {
		coolOff = 30 * time.Second
	}
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: threshold,
		coolOffDuration:  coolOff,
		lastStateChange:  time.Now(),
		nowFunc:          time.Now,
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.updateStateLocked()
	return cb.state
}

// ConsecutiveFailures returns the current count of consecutive failures.
func (cb *CircuitBreaker) ConsecutiveFailures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.consecutiveFailures
}

func (cb *CircuitBreaker) updateStateLocked() {
	if cb.state == StateOpen {
		if cb.nowFunc().Sub(cb.lastStateChange) >= cb.coolOffDuration {
			cb.state = StateHalfOpen
			cb.lastStateChange = cb.nowFunc()
		}
	}
}

// AllowExecution checks whether a request is permitted to proceed.
func (cb *CircuitBreaker) AllowExecution() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.updateStateLocked()
	return cb.state == StateClosed || cb.state == StateHalfOpen
}

// RecordSuccess registers a successful execution, closing the circuit.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures = 0
	if cb.state != StateClosed {
		cb.state = StateClosed
		cb.lastStateChange = cb.nowFunc()
	}
}

// RecordFailure registers a failed execution, potentially tripping the circuit to OPEN.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveFailures++
	if cb.state == StateHalfOpen || cb.consecutiveFailures >= cb.failureThreshold {
		cb.state = StateOpen
		cb.lastStateChange = cb.nowFunc()
	}
}

// Execute wraps an operation with circuit breaker protection.
func (cb *CircuitBreaker) Execute(ctx context.Context, op func(ctx context.Context) error) error {
	if !cb.AllowExecution() {
		return ErrCircuitBreakerOpen
	}

	err := op(ctx)
	if err != nil {
		cb.RecordFailure()
		return err
	}

	cb.RecordSuccess()
	return nil
}

// RetryWithBackoff executes op with exponential backoff and jitter up to maxRetries attempts.
func RetryWithBackoff(
	ctx context.Context,
	maxRetries int,
	initialDelay time.Duration,
	maxDelay time.Duration,
	cb *CircuitBreaker,
	op func(ctx context.Context) error,
) error {
	if maxRetries < 1 {
		maxRetries = 1
	}
	if initialDelay <= 0 {
		initialDelay = 50 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 2 * time.Second
	}

	delay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		var err error
		if cb != nil {
			err = cb.Execute(ctx, op)
		} else {
			err = op(ctx)
		}

		if err == nil {
			return nil
		}

		// If circuit breaker is open, do not wait and retry — fail fast
		if errors.Is(err, ErrCircuitBreakerOpen) {
			return err
		}

		if attempt == maxRetries {
			return err
		}

		// Calculate jittered exponential backoff: delay * (0.8 + 0.4 * rand)
		jitter := 0.8 + 0.4*rand.Float64()
		sleepDuration := time.Duration(float64(delay) * jitter)
		if sleepDuration > maxDelay {
			sleepDuration = maxDelay
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
		}

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return nil
}
