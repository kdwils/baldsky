package server

import (
	"context"
	"time"

	"github.com/kdwils/baldsky/cache"
	"golang.org/x/time/rate"
)

type limiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

type RateLimiter struct {
	limiters *cache.Cache[string, *limiterEntry]
	rate     rate.Limit
	burst    int
	maxAge   time.Duration
}

func NewRateLimiter(requestsPerSecond float64, burst int, maxAge time.Duration) *RateLimiter {
	rl := &RateLimiter{
		rate:   rate.Limit(requestsPerSecond),
		burst:  burst,
		maxAge: maxAge,
	}

	rl.limiters = cache.New(
		cache.WithCleanup(1*time.Minute, rl.isStale),
	)

	return rl
}

func (rl *RateLimiter) isStale(_ string, entry *limiterEntry) bool {
	return time.Since(entry.lastUsed) > rl.maxAge
}

func (rl *RateLimiter) getLimiter(ip string) *rate.Limiter {
	if entry, ok := rl.limiters.Get(ip); ok {
		entry.lastUsed = time.Now()
		return entry.limiter
	}

	newLimiter := rate.NewLimiter(rl.rate, rl.burst)
	rl.limiters.Set(ip, &limiterEntry{limiter: newLimiter, lastUsed: time.Now()})
	return newLimiter
}

func (rl *RateLimiter) Allow(ip string) bool {
	return rl.getLimiter(ip).Allow()
}

func (rl *RateLimiter) StartCleanup(ctx context.Context) {
	rl.limiters.StartCleanup(ctx)
}

func (rl *RateLimiter) purgeStale() {
	cutoff := time.Now().Add(-rl.maxAge)
	for _, ip := range rl.limiters.Keys() {
		entry, ok := rl.limiters.Get(ip)
		if ok && entry.lastUsed.Before(cutoff) {
			rl.limiters.Delete(ip)
		}
	}
}
