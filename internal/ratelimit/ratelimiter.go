package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter defines the interface for rate limiting message delivery.
type Limiter interface {
	Wait(ctx context.Context, channel string) error
}

// RedisLimiter implements Limiter using Redis as a distributed backend.
type RedisLimiter struct {
	rdb       *redis.Client
	maxPerSec int
}

// NewRedisLimiter creates a new RedisLimiter.
func NewRedisLimiter(rdb *redis.Client, maxPerSec int) *RedisLimiter {
	return &RedisLimiter{rdb: rdb, maxPerSec: maxPerSec}
}

// Wait blocks until the rate limit allows for a new request or the context is cancelled.
// If maxPerSec is 0 or negative, no limit is applied and Wait returns nil immediately.
func (l *RedisLimiter) Wait(ctx context.Context, channel string) error {
	if l.maxPerSec <= 0 {
		return nil
	}
	for {
		key := fmt.Sprintf("rl:%s:%d", channel, time.Now().Unix())

		pipe := l.rdb.Pipeline()
		incr := pipe.Incr(ctx, key)
		pipe.Expire(ctx, key, 2*time.Second)
		if _, err := pipe.Exec(ctx); err != nil {
			// fail open — redis error should not block delivery
			return nil
		}

		if incr.Val() <= int64(l.maxPerSec) {
			return nil
		}

		// over limit — wait until next second
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(time.Now().Truncate(time.Second).Add(time.Second))):
		}
	}
}
