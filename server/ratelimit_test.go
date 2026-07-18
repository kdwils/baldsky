package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kdwils/baldsky/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestNewRateLimiter(t *testing.T) {
	t.Run("sets rate, burst, and maxAge", func(t *testing.T) {
		got := NewRateLimiter(10.0, 20, 3*time.Minute)
		assert.Equal(t, rate.Limit(10.0), got.rate)
		assert.Equal(t, 20, got.burst)
		assert.Equal(t, 3*time.Minute, got.maxAge)
		assert.NotNil(t, got.limiters)
	})
}

func TestAllow(t *testing.T) {
	t.Run("allows requests within burst", func(t *testing.T) {
		rl := NewRateLimiter(100, 5, 3*time.Minute)

		for range 5 {
			got := rl.Allow("1.2.3.4")
			assert.Equal(t, true, got)
		}
	})

	t.Run("rejects after burst exhausted", func(t *testing.T) {
		rl := NewRateLimiter(0.001, 1, 3*time.Minute)

		got := rl.Allow("1.2.3.4")
		assert.Equal(t, true, got)

		got = rl.Allow("1.2.3.4")
		assert.Equal(t, false, got)
	})

	t.Run("different ips have independent limits", func(t *testing.T) {
		rl := NewRateLimiter(0.001, 1, 3*time.Minute)

		got := rl.Allow("1.1.1.1")
		assert.Equal(t, true, got)

		got = rl.Allow("1.1.1.1")
		assert.Equal(t, false, got)

		got = rl.Allow("2.2.2.2")
		assert.Equal(t, true, got)

		got = rl.Allow("2.2.2.2")
		assert.Equal(t, false, got)
	})
}

func TestGetLimiter(t *testing.T) {
	t.Run("same ip returns same limiter", func(t *testing.T) {
		rl := NewRateLimiter(100, 10, 3*time.Minute)

		first := rl.getLimiter("10.0.0.1")
		got := rl.getLimiter("10.0.0.1")
		want := first
		assert.Equal(t, want, got)
	})

	t.Run("different ips return different limiters", func(t *testing.T) {
		rl := NewRateLimiter(100, 10, 3*time.Minute)

		a := rl.getLimiter("10.0.0.1")
		b := rl.getLimiter("10.0.0.2")
		require.NotSame(t, a, b)
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		rl := NewRateLimiter(100, 100, 3*time.Minute)

		var wg sync.WaitGroup
		for i := range 100 {
			wg.Add(1)
			go func(ip string) {
				defer wg.Done()
				for range 50 {
					rl.Allow(ip)
				}
			}("10.0.0." + string(rune('0'+i%10)))
		}
		wg.Wait()

		got := rl.limiters.Size()
		assert.Equal(t, 10, got)
	})
}

func TestPurgeStale(t *testing.T) {
	t.Run("removes entries older than maxAge", func(t *testing.T) {
		rl := NewRateLimiter(100, 10, 1*time.Hour)

		rl.now = func() time.Time { return time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) }
		rl.Allow("old-ip")
		rl.Allow("new-ip")

		entry, ok := rl.limiters.Get("old-ip")
		require.True(t, ok)
		entry.lastUsed.Store(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

		rl.purgeStale()

		_, gotOld := rl.limiters.Get("old-ip")
		assert.Equal(t, false, gotOld)

		_, gotNew := rl.limiters.Get("new-ip")
		assert.Equal(t, true, gotNew)
	})

	t.Run("keeps all entries within maxAge", func(t *testing.T) {
		rl := NewRateLimiter(100, 10, 1*time.Hour)

		rl.Allow("ip-a")
		rl.Allow("ip-b")

		rl.purgeStale()

		got := rl.limiters.Size()
		assert.Equal(t, 2, got)
	})
}

func TestStartCleanup(t *testing.T) {
	t.Run("purges stale entries on interval", func(t *testing.T) {
		rl := NewRateLimiter(100, 10, 1*time.Millisecond)
		rl.limiters = newTestCache(rl, 1*time.Millisecond)

		rl.Allow("stale-ip")

		ctx := t.Context()

		rl.StartCleanup(ctx)

		assert.Eventually(t, func() bool {
			_, exists := rl.limiters.Get("stale-ip")
			return !exists
		}, 2*time.Second, 5*time.Millisecond)
	})

	t.Run("stops on context cancel", func(t *testing.T) {
		rl := NewRateLimiter(100, 10, 1*time.Hour)
		rl.limiters = newTestCache(rl, 1*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		rl.StartCleanup(ctx)
		cancel()

		rl.Allow("some-ip")

		_, got := rl.limiters.Get("some-ip")
		assert.Equal(t, true, got)
	})

	t.Run("does not remove recently used entries", func(t *testing.T) {
		rl := NewRateLimiter(100, 10, 10*time.Second)

		rl.limiters = newTestCache(rl, 1*time.Millisecond)

		rl.Allow("active-ip")

		ctx := t.Context()

		rl.StartCleanup(ctx)

		assert.Eventually(t, func() bool {
			_, exists := rl.limiters.Get("active-ip")
			return exists
		}, 2*time.Second, 5*time.Millisecond)
	})
}

func newTestCache(rl *RateLimiter, cleanupInterval time.Duration) *cache.Cache[string, *limiterEntry] {
	return cache.New(
		cache.WithCleanup(cleanupInterval, rl.isStale),
	)
}
