package queue

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/onurcevik/notification-service/internal/domain"
)

// NotificationGetter is the minimal interface needed by the relay to fetch a notification by ID.
type NotificationGetter interface {
	GetByID(ctx context.Context, id string) (*domain.Notification, error)
}

// OutboxEventRepo is the minimal interface for the outbox event table used by the relay.
type OutboxEventRepo interface {
	FetchUnpublished(ctx context.Context) ([]string, error)
	MarkPublished(ctx context.Context, ids []string) error
	FetchStale(ctx context.Context) ([]string, error)
}

// OutboxRelay polls the database outbox and enqueues messages into Redis.
type OutboxRelay struct {
	notifRepo NotificationGetter
	eventRepo OutboxEventRepo
	queue     *RedisQueue
	interval  time.Duration
}

// NewOutboxRelay creates a new OutboxRelay.
func NewOutboxRelay(
	notifRepo NotificationGetter,
	eventRepo OutboxEventRepo,
	queue *RedisQueue,
	interval time.Duration,
) *OutboxRelay {
	return &OutboxRelay{
		notifRepo: notifRepo,
		eventRepo: eventRepo,
		queue:     queue,
		interval:  interval,
	}
}

// Run starts the relay loop.
func (r *OutboxRelay) Run(ctx context.Context) {
	if err := r.recover(ctx); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("relay recovery failed")
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.tick(ctx); err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("relay tick failed")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *OutboxRelay) tick(ctx context.Context) error {
	ids, err := r.eventRepo.FetchUnpublished(ctx)
	if err != nil || len(ids) == 0 {
		return err
	}

	var published []string
	for _, id := range ids {
		n, err := r.notifRepo.GetByID(ctx, id)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("notification_id", id).Msg("relay: failed to fetch notification")
			continue
		}
		if err := r.queue.Enqueue(ctx, n); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("notification_id", id).Msg("relay: failed to enqueue")
			continue
		}
		published = append(published, id)
	}

	if len(published) == 0 {
		return nil
	}
	return r.eventRepo.MarkPublished(ctx, published)
}

func (r *OutboxRelay) recover(ctx context.Context) error {
	ids, err := r.eventRepo.FetchStale(ctx)
	if err != nil || len(ids) == 0 {
		return err
	}
	log.Ctx(ctx).Info().Int("count", len(ids)).Msg("relay: recovering stale events")
	var published []string
	for _, id := range ids {
		n, err := r.notifRepo.GetByID(ctx, id)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("notification_id", id).Msg("relay: recover failed to fetch notification")
			continue
		}
		if err := r.queue.Enqueue(ctx, n); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("notification_id", id).Msg("relay: recover failed to enqueue")
			continue
		}
		published = append(published, id)
	}
	if len(published) == 0 {
		return nil
	}
	return r.eventRepo.MarkPublished(ctx, published)
}
