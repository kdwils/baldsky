package server

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/kdwils/baldsky/cache"
	"golang.org/x/time/rate"
)

type limiterEntry struct {
	limiter  *rate.Limiter
	lastUsed atomic.Int64
}

type RateLimiter struct {
	limiters *cache.Cache[string, *limiterEntry]
	rate     rate.Limit
	burst    int
	maxAge   time.Duration
	now      func() time.Time
}

func NewRateLimiter(requestsPerSecond float64, burst int, maxAge time.Duration) *RateLimiter {
	rl := &RateLimiter{
		rate:   rate.Limit(requestsPerSecond),
		burst:  burst,
		maxAge: maxAge,
		now:    time.Now,
	}

	rl.limiters = cache.New(
		cache.WithCleanup(1*time.Minute, rl.isStale),
	)

	return rl
}

func (rl *RateLimiter) isStale(_ string, entry *limiterEntry) bool {
	return rl.now().Sub(time.Unix(0, entry.lastUsed.Load())) > rl.maxAge
}

func (rl *RateLimiter) getLimiter(ip string) *rate.Limiter {
	if entry, ok := rl.limiters.Get(ip); ok {
		entry.lastUsed.Store(rl.now().UnixNano())
		return entry.limiter
	}

	newLimiter := rate.NewLimiter(rl.rate, rl.burst)
	e := &limiterEntry{limiter: newLimiter}
	e.lastUsed.Store(rl.now().UnixNano())
	rl.limiters.Set(ip, e)
	return newLimiter
}

func (rl *RateLimiter) Allow(ip string) bool {
	return rl.getLimiter(ip).Allow()
}

func (rl *RateLimiter) StartCleanup(ctx context.Context) {
	rl.limiters.StartCleanup(ctx)
}

func (rl *RateLimiter) purgeStale() {
	cutoff := rl.now().Add(-rl.maxAge)
	for _, ip := range rl.limiters.Keys() {
		entry, ok := rl.limiters.Get(ip)
		if ok && time.Unix(0, entry.lastUsed.Load()).Before(cutoff) {
			rl.limiters.Delete(ip)
		}
	}
}
