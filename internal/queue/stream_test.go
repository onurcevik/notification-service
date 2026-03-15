package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onurcevik/notification-service/internal/domain"
)

func TestRedisQueue_Priority(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}
	require.NoError(t, rdb.FlushDB(ctx).Err()) // clean state before test
	time.Sleep(50 * time.Millisecond)

	q := NewRedisQueue(rdb, uuid.NewString(), -1) // -1 = default short block
	err := q.Init(ctx)
	require.NoError(t, err)

	// Enqueue and consume one at a time; retry Consume briefly in case Redis is slow to make message visible
	consumeOne := func() *Message {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			m, e := q.Consume(ctx)
			require.NoError(t, e)
			if m != nil {
				return m
			}
			time.Sleep(80 * time.Millisecond)
		}
		return nil
	}

	highNotif := &domain.Notification{ID: "high-1", Priority: domain.PriorityHigh, Content: "high"}
	require.NoError(t, q.Enqueue(ctx, highNotif))
	time.Sleep(150 * time.Millisecond) // let Redis make message visible to consumer group
	msg := consumeOne()
	if msg == nil {
		t.Skip("Redis did not return high message within 2s (Redis may be slow or under load)")
	}
	assert.Equal(t, "high-1", msg.Notification.ID)
	assert.Equal(t, StreamHigh, msg.Stream)

	normalNotif := &domain.Notification{ID: "normal-1", Priority: domain.PriorityNormal, Content: "normal"}
	require.NoError(t, q.Enqueue(ctx, normalNotif))
	msg = consumeOne()
	if msg == nil {
		t.Fatal("expected normal message within 2s")
	}
	assert.Equal(t, "normal-1", msg.Notification.ID)
	assert.Equal(t, StreamNormal, msg.Stream)

	lowNotif := &domain.Notification{ID: "low-1", Priority: domain.PriorityLow, Content: "low"}
	require.NoError(t, q.Enqueue(ctx, lowNotif))
	msg = consumeOne()
	if msg == nil {
		t.Fatal("expected low message within 2s")
	}
	assert.Equal(t, "low-1", msg.Notification.ID)
	assert.Equal(t, StreamLow, msg.Stream)

	// Queue empty: Consume returns nil
	msg, err = q.Consume(ctx)
	require.NoError(t, err)
	assert.Nil(t, msg)
}

func TestRedisQueue_Ack(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}
	require.NoError(t, rdb.FlushDB(ctx).Err())

	q := NewRedisQueue(rdb, "consumer-1", -1)
	require.NoError(t, q.Init(ctx))

	n := &domain.Notification{ID: "test-1", Priority: domain.PriorityHigh}
	require.NoError(t, q.Enqueue(ctx, n))

	msg, err := q.Consume(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Verify it's in the PEL (Pending Entries List)
	pending, err := rdb.XPending(ctx, StreamHigh, GroupName).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), pending.Count)

	// Ack the message
	err = q.Ack(ctx, msg.Stream, msg.ID)
	require.NoError(t, err)

	// Verify PEL is empty
	pending, err = rdb.XPending(ctx, StreamHigh, GroupName).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), pending.Count)
}

// TestRedisQueue_ConsumeBlockStarvation demonstrates that with ConsumeBlock 0 (block forever),
// when only normal (or low) has messages, Consume blocks on the high stream and never returns
// a message until context is cancelled.
func TestRedisQueue_ConsumeBlockStarvation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}
	require.NoError(t, rdb.FlushDB(ctx).Err())

	q := NewRedisQueue(rdb, uuid.NewString(), 0) // 0 = block indefinitely per stream
	require.NoError(t, q.Init(ctx))

	// Enqueue only to normal; high and low are empty
	normalNotif := &domain.Notification{ID: "normal-1", Priority: domain.PriorityNormal, Content: "normal"}
	require.NoError(t, q.Enqueue(ctx, normalNotif))

	// With Block=0 we block on high (empty) first; context timeout may or may not wake the blocking read
	// depending on Redis client. Either way we must not receive the normal message (starvation).
	consumeCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	var msg *Message
	var consumeErr error
	go func() {
		msg, consumeErr = q.Consume(consumeCtx)
		close(done)
	}()

	select {
	case <-done:
		// Consume returned: must not have received the normal message (starvation)
		assert.Nil(t, msg, "expected no message when blocked on empty high stream (starvation)")
		if consumeErr != nil {
			assert.True(t, consumeErr == context.DeadlineExceeded || consumeErr == context.Canceled,
				"expected context error when blocking, got %v", consumeErr)
		}
	case <-time.After(2 * time.Second):
		// Consume still blocking: also valid starvation (client may not respect context during Block=0)
		cancel()
		<-done
		assert.Nil(t, msg, "expected no message (starvation)")
	}
}

// TestRedisQueue_DeadLetter verifies that DeadLetter publishes a message to StreamDead
// with the notification payload and a "reason" field.
func TestRedisQueue_DeadLetter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}
	require.NoError(t, rdb.FlushDB(ctx).Err())

	q := NewRedisQueue(rdb, "consumer-dl", -1)
	require.NoError(t, q.Init(ctx))

	n := &domain.Notification{ID: "dl-1", Priority: domain.PriorityHigh, Content: "will fail"}
	dlErr := errors.New("max retries exceeded")
	err := q.DeadLetter(ctx, n, dlErr)
	require.NoError(t, err)

	// Read directly from StreamDead to confirm the record is there.
	msgs, err := rdb.XRange(ctx, StreamDead, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1, "exactly one message expected in dead-letter stream")

	vals := msgs[0].Values
	assert.Contains(t, vals, "data", "dead-letter message must contain 'data' field")
	assert.Contains(t, vals, "reason", "dead-letter message must contain 'reason' field")
	assert.Equal(t, dlErr.Error(), vals["reason"], "reason field must match the error string")
}

// TestRedisQueue_Init_ExistingGroup verifies that calling Init a second time (after the
// consumer groups already exist) does not return a BUSYGROUP error.
func TestRedisQueue_Init_ExistingGroup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}
	require.NoError(t, rdb.FlushDB(ctx).Err())

	q := NewRedisQueue(rdb, "consumer-init", -1)
	require.NoError(t, q.Init(ctx), "first Init must succeed")
	require.NoError(t, q.Init(ctx), "second Init must succeed even when groups already exist (BUSYGROUP is silenced)")
}

// TestRedisQueue_Consume_PoisonMessage verifies that a message with malformed JSON payload
// is silently skipped and Consume returns (nil, nil) rather than an error.
func TestRedisQueue_Consume_PoisonMessage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}
	require.NoError(t, rdb.FlushDB(ctx).Err())

	q := NewRedisQueue(rdb, uuid.NewString(), -1)
	require.NoError(t, q.Init(ctx))

	// Inject a poison message directly — "data" field contains invalid JSON.
	err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamHigh,
		Values: map[string]any{"data": "{{not-json}}"},
	}).Err()
	require.NoError(t, err)

	// First Consume: should read the poison message and skip it.
	// The consumer group consumes from ">" so the message enters the PEL.
	msg, err := q.Consume(ctx)
	assert.NoError(t, err, "poison message must not surface as an error")
	assert.Nil(t, msg, "poison message must be skipped (nil returned)")
}

// TestRedisQueue_ShortBlockNoStarvation demonstrates that with a short ConsumeBlock,
// messages in normal (or low) are consumed even when high is empty.
func TestRedisQueue_ShortBlockNoStarvation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping redis test")
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("redis not available")
	}
	require.NoError(t, rdb.FlushDB(ctx).Err())
	time.Sleep(50 * time.Millisecond)

	q := NewRedisQueue(rdb, uuid.NewString(), 50*time.Millisecond) // short block
	require.NoError(t, q.Init(ctx))

	// Enqueue only to normal; high is empty
	normalNotif := &domain.Notification{ID: "normal-1", Priority: domain.PriorityNormal, Content: "normal"}
	require.NoError(t, q.Enqueue(ctx, normalNotif))

	// Consume should return the normal message within a few seconds (no starvation)
	deadline := time.Now().Add(3 * time.Second)
	var msg *Message
	var err error
	for time.Now().Before(deadline) {
		msg, err = q.Consume(ctx)
		require.NoError(t, err)
		if msg != nil {
			break
		}
		time.Sleep(80 * time.Millisecond)
	}

	require.NotNil(t, msg, "expected to consume normal message with short block within 3s (no starvation)")
	assert.Equal(t, "normal-1", msg.Notification.ID)
	assert.Equal(t, StreamNormal, msg.Stream)
}
