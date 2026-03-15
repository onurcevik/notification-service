package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/onurcevik/notification-service/internal/domain"
)

const (
	StreamHigh   = "stream:high"
	StreamNormal = "stream:normal"
	StreamLow    = "stream:low"
	StreamDead   = "stream:dead"
	GroupName    = "workers"

	// defaultConsumeBlock is the default max time to block when reading from one stream.
	// Short block ensures we check all priority streams each loop and never
	// starve normal/low when high is empty.
	defaultConsumeBlock = 100 * time.Millisecond
)

type Message struct {
	ID           string
	Stream       string
	Notification *domain.Notification
}

// RedisQueue implements a priority queue using Redis Streams.
type RedisQueue struct {
	rdb          *redis.Client
	consumerID   string
	consumeBlock time.Duration
}

// NewRedisQueue creates a new RedisQueue. consumeBlock is the max time to block per stream
// in Consume. If consumeBlock < 0, defaultConsumeBlock is used (avoids starving normal/low).
// Pass 0 to block indefinitely per stream (for starvation tests).
func NewRedisQueue(rdb *redis.Client, consumerID string, consumeBlock time.Duration) *RedisQueue {
	q := &RedisQueue{rdb: rdb, consumerID: consumerID, consumeBlock: consumeBlock}
	if q.consumeBlock < 0 {
		q.consumeBlock = defaultConsumeBlock
	}
	return q
}

// isConsumerGroupExistsErr reports whether the error is Redis BUSYGROUP (group already exists).
// go-redis does not expose a sentinel for this; Redis returns an error message containing BUSYGROUP.
func isConsumerGroupExistsErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Init creates the consumer groups for each priority stream.
// If a group already exists (e.g. after restart), that is treated as success.
func (q *RedisQueue) Init(ctx context.Context) error {
	for _, stream := range []string{StreamHigh, StreamNormal, StreamLow} {
		err := q.rdb.XGroupCreateMkStream(ctx, stream, GroupName, "0").Err()
		if err != nil && !isConsumerGroupExistsErr(err) {
			return fmt.Errorf("create group %s: %w", stream, err)
		}
	}
	return nil
}

// Enqueue adds a notification to the stream corresponding to its priority.
func (q *RedisQueue) Enqueue(ctx context.Context, n *domain.Notification) error {
	stream := q.StreamForPriority(n.Priority)
	data, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"data": data},
	}).Err()
}

// Consume reads the next available message from the streams in priority order.
// Uses a short block per stream so normal/low are not starved when high is empty.
// Returns any Redis error encountered so callers can back off or log (e.g. connection failures).
func (q *RedisQueue) Consume(ctx context.Context) (*Message, error) {
	var consumeErr error
	for _, stream := range []string{StreamHigh, StreamNormal, StreamLow} {
		msgs, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    GroupName,
			Consumer: q.consumerID,
			Streams:  []string{stream, ">"},
			Count:    1,
			Block:    q.consumeBlock,
		}).Result()
		if err != nil {
			consumeErr = err
			continue
		}
		if len(msgs) == 0 || len(msgs[0].Messages) == 0 {
			continue
		}

		msg := msgs[0].Messages[0]
		data, ok := msg.Values["data"].(string)
		if !ok {
			continue
		}

		var n domain.Notification
		if err := json.Unmarshal([]byte(data), &n); err != nil {
			continue
		}

		return &Message{ID: msg.ID, Stream: stream, Notification: &n}, nil
	}
	if consumeErr != nil {
		return nil, consumeErr
	}
	return nil, nil
}

// Ack acknowledges that a message has been processed successfully.
func (q *RedisQueue) Ack(ctx context.Context, stream string, msgID string) error {
	return q.rdb.XAck(ctx, stream, GroupName, msgID).Err()
}

// DeadLetter moves a failed notification to the dead-letter stream.
func (q *RedisQueue) DeadLetter(ctx context.Context, n *domain.Notification, reason error) error {
	data, err := json.Marshal(n)
	if err != nil {
		return err
	}
	return q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamDead,
		Values: map[string]any{
			"data":   data,
			"reason": reason.Error(),
		},
	}).Err()
}

// StreamForPriority returns the stream name for a given priority.
func (q *RedisQueue) StreamForPriority(p domain.Priority) string {
	switch p {
	case domain.PriorityHigh:
		return StreamHigh
	case domain.PriorityLow:
		return StreamLow
	default:
		return StreamNormal
	}
}
