package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skip("Redis not available:", err)
	}
	return rdb
}

// uniqueChannel returns a channel name unique to this test invocation so that
// parallel or repeated runs never share the same Redis counter key.
func uniqueChannel(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test:%s:%d", t.Name(), time.Now().UnixNano())
}

// TestRedisLimiter_ExactBudget_AllPass verifies that exactly maxPerSec calls
// within one second all succeed without blocking.
func TestRedisLimiter_ExactBudget_AllPass(t *testing.T) {
	rdb := redisClient(t)
	const limit = 5
	limiter := NewRedisLimiter(rdb, limit)
	ch := uniqueChannel(t)
	for i := 0; i < limit; i++ {
		require.NoError(t, limiter.Wait(context.Background(), ch))
	}
}

// TestRedisLimiter_ExceedBudget_ExtraCallBlocks proves that the (limit+1)th call
// does NOT return immediately — it must wait for the next second window.
func TestRedisLimiter_ExceedBudget_ExtraCallBlocks(t *testing.T) {
	rdb := redisClient(t)
	const limit = 3
	limiter := NewRedisLimiter(rdb, limit)
	ch := uniqueChannel(t)
	ctx := context.Background()

	// Exhaust the per-second budget.
	for i := 0; i < limit; i++ {
		require.NoError(t, limiter.Wait(ctx, ch))
	}

	// The next call must block, not return immediately.
	done := make(chan error, 1)
	go func() { done <- limiter.Wait(ctx, ch) }()

	select {
	case <-done:
		t.Fatal("Wait returned immediately after budget exhausted — rate limit not enforced")
	case <-time.After(50 * time.Millisecond):
		// correctly blocking
	}

	// It must eventually unblock once the second rolls over.
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not unblock within 2s after budget exhausted")
	}
}

// TestRedisLimiter_ThroughputGating verifies that sending 2*limit calls is gated
// to at least two windows (~1 second apart), proving the limiter actually throttles
// throughput and does not just return nil immediately.
func TestRedisLimiter_ThroughputGating(t *testing.T) {
	rdb := redisClient(t)
	const limit = 3
	limiter := NewRedisLimiter(rdb, limit)
	ch := uniqueChannel(t)
	ctx := context.Background()

	start := time.Now()

	// First window: passes immediately.
	for i := 0; i < limit; i++ {
		require.NoError(t, limiter.Wait(ctx, ch))
	}
	// Second window: must wait for the next second.
	for i := 0; i < limit; i++ {
		require.NoError(t, limiter.Wait(ctx, ch))
	}

	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond,
		"2×limit=%d calls at limit=%d/s must take ≥1s; got %s", 2*limit, limit, elapsed)
}

// TestRedisLimiter_ContextCancelled verifies that a blocked Wait returns
// immediately with context.Canceled when the caller cancels.
func TestRedisLimiter_ContextCancelled(t *testing.T) {
	rdb := redisClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	limiter := NewRedisLimiter(rdb, 1)
	ch := uniqueChannel(t)

	require.NoError(t, limiter.Wait(ctx, ch)) // exhaust the budget
	done := make(chan error, 1)
	go func() { done <- limiter.Wait(ctx, ch) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after context cancel")
	}
}

// TestRedisLimiter_FailOpen verifies that a Redis error does not block delivery —
// the limiter should return nil (fail open) so notifications are not dropped.
func TestRedisLimiter_FailOpen(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:19999"})
	defer func() { _ = rdb.Close() }()
	limiter := NewRedisLimiter(rdb, 10)
	err := limiter.Wait(context.Background(), "test-channel")
	assert.NoError(t, err)
}

// TestRedisLimiter_NoLimit_ReturnsImmediately verifies that maxPerSec ≤ 0 disables
// rate limiting and Wait always returns nil without touching Redis.
func TestRedisLimiter_NoLimit_ReturnsImmediately(t *testing.T) {
	// Deliberately point at a dead Redis to confirm Redis is never contacted.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:19999"})
	defer func() { _ = rdb.Close() }()
	for _, maxPerSec := range []int{0, -1} {
		limiter := NewRedisLimiter(rdb, maxPerSec)
		err := limiter.Wait(context.Background(), "no-limit")
		require.NoError(t, err)
	}
}
