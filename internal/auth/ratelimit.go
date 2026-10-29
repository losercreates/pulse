package auth

import (
	"hash/fnv"
	"sync"
	"time"
)

// RateLimiter defines the perimeter rate limiter contract.
type RateLimiter interface {
	Allow(key string) (allowed bool, retryAfter time.Duration)
}

type bucket struct {
	tokens      float64
	lastUpdated time.Time
	rate        float64 // tokens per second
	burst       float64 // maximum token capacity
}

const numShards = 64

type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

// TokenBucketRateLimiter implements a sharded token-bucket rate limiter.
type TokenBucketRateLimiter struct {
	shards       [numShards]shard
	defaultRate  float64
	defaultBurst float64
}

// NewTokenBucketRateLimiter creates a token bucket rate limiter with default rate and burst capacity.
// defaultRate is tokens added per second; defaultBurst is the bucket maximum capacity.
func NewTokenBucketRateLimiter(defaultRate, defaultBurst float64) *TokenBucketRateLimiter {
	limiter := &TokenBucketRateLimiter{
		defaultRate:  defaultRate,
		defaultBurst: defaultBurst,
	}
	for i := 0; i < numShards; i++ {
		limiter.shards[i].buckets = make(map[string]*bucket)
	}
	return limiter
}

func (l *TokenBucketRateLimiter) getShard(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := h.Sum32() % numShards
	return &l.shards[idx]
}

// SetLimit configures a custom rate and burst for a specific key (e.g., high-throughput device).
func (l *TokenBucketRateLimiter) SetLimit(key string, rate, burst float64) {
	s := l.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.buckets[key]
	if !ok {
		s.buckets[key] = &bucket{
			tokens:      burst,
			lastUpdated: time.Now(),
			rate:        rate,
			burst:       burst,
		}
		return
	}
	b.rate = rate
	b.burst = burst
	if b.tokens > burst {
		b.tokens = burst
	}
}

// Allow evaluates if a request with the given key is permitted.
// If disallowed, returns false and the recommended duration before retry.
func (l *TokenBucketRateLimiter) Allow(key string) (bool, time.Duration) {
	if l == nil || l.defaultRate <= 0 {
		return true, 0
	}

	s := l.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	b, ok := s.buckets[key]
	if !ok {
		// New bucket starts with full burst capacity minus 1 for the current request
		s.buckets[key] = &bucket{
			tokens:      l.defaultBurst - 1.0,
			lastUpdated: now,
			rate:        l.defaultRate,
			burst:       l.defaultBurst,
		}
		return true, 0
	}

	// Refill tokens based on elapsed time
	elapsed := now.Sub(b.lastUpdated).Seconds()
	b.lastUpdated = now
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}

	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		return true, 0
	}

	// Not enough tokens
	missing := 1.0 - b.tokens
	retrySeconds := missing / b.rate
	retryAfter := time.Duration(retrySeconds * float64(time.Second))
	if retryAfter < time.Millisecond {
		retryAfter = time.Millisecond
	}

	return false, retryAfter
}

// Cleanup removes idle buckets that haven't been accessed for older than maxIdle duration.
func (l *TokenBucketRateLimiter) Cleanup(maxIdle time.Duration) {
	if l == nil {
		return
	}
	now := time.Now()
	for i := 0; i < numShards; i++ {
		s := &l.shards[i]
		s.mu.Lock()
		for k, b := range s.buckets {
			if now.Sub(b.lastUpdated) > maxIdle {
				delete(s.buckets, k)
			}
		}
		s.mu.Unlock()
	}
}

// NopRateLimiter allows all requests.
type NopRateLimiter struct{}

func (n *NopRateLimiter) Allow(key string) (bool, time.Duration) {
	return true, 0
}

