package service

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"gitlab.com/onurcevik/notification-service/internal/domain"
)

// Scheduler periodicially checks for scheduled notifications that are due.
type Scheduler struct {
	repo      NotificationRepository
	eventRepo EventRepository
	txr       Transactor
	interval  time.Duration
}

// NewScheduler creates a new Scheduler instance.
func NewScheduler(
	repo NotificationRepository,
	eventRepo EventRepository,
	txr Transactor,
	interval time.Duration,
) *Scheduler {
	return &Scheduler{repo: repo, eventRepo: eventRepo, txr: txr, interval: interval}
}

// Run starts the scheduler loop.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("scheduler tick failed")
			}
		case <-ctx.Done():
			return
		}
	}
}

// Tick runs one scheduler cycle (fetch due notifications and enqueue them). Exposed for testing.
func (s *Scheduler) Tick(ctx context.Context) error {
	return s.tick(ctx)
}

func (s *Scheduler) tick(ctx context.Context) error {
	due, err := s.repo.FetchDueScheduled(ctx)
	if err != nil {
		return err
	}
	for _, n := range due {
		if err := s.enqueue(ctx, n); err != nil {
			log.Ctx(ctx).Error().Err(err).
				Str("notification_id", n.ID).
				Msg("failed to enqueue scheduled notification")
		}
	}
	return nil
}

func (s *Scheduler) enqueue(ctx context.Context, n domain.Notification) error {
	return s.txr.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.eventRepo.Insert(ctx, tx, n.ID); err != nil {
			return err
		}
		return s.repo.UpdateStatus(ctx, n.ID, domain.StatusPending, "")
	})
}
