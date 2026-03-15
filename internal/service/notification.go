package service

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"gopkg.in/guregu/null.v4"

	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/onurcevik/notification-service/internal/repository"
)

var tracer = otel.Tracer("notification-service/service")

// Transactor defines the interface for running database transactions.
type Transactor interface {
	WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error
}

// NotificationRepository defines the interface for notification data access.
type NotificationRepository interface {
	Create(ctx context.Context, tx pgx.Tx, n *domain.Notification) error
	GetByID(ctx context.Context, id string) (*domain.Notification, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error)
	// GetByIdempotencyKeyTx returns the notification by idempotency key within the given transaction (for idempotent create).
	GetByIdempotencyKeyTx(ctx context.Context, tx pgx.Tx, key string) (*domain.Notification, error)
	UpdateStatus(ctx context.Context, id string, status domain.Status, providerMsgID string) error
	IncrementAttempt(ctx context.Context, id string) error
	Cancel(ctx context.Context, id string) error
	List(ctx context.Context, f domain.Filter) ([]domain.Notification, string, error)
	FetchDueScheduled(ctx context.Context) ([]domain.Notification, error)
}

// EventRepository defines the interface for outbox event data access.
type EventRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, notificationID string) error
	FetchUnpublished(ctx context.Context) ([]string, error)
	MarkPublished(ctx context.Context, ids []string) error
	FetchStale(ctx context.Context) ([]string, error)
}

// NotificationService handles business logic for notifications.
type NotificationService struct {
	repo      NotificationRepository
	eventRepo EventRepository
	txr       Transactor
}

// NewNotificationService creates a new NotificationService.
func NewNotificationService(
	repo NotificationRepository,
	eventRepo EventRepository,
	txr Transactor,
) *NotificationService {
	return &NotificationService{repo: repo, eventRepo: eventRepo, txr: txr}
}

// Create creates a new notification and an outbox event.
// It returns the notification and whether it was newly created (true) or already existed (false, same idempotency key).
func (s *NotificationService) Create(ctx context.Context, n *domain.Notification) (*domain.Notification, bool, error) {
	ctx, span := tracer.Start(ctx, "service.Create")
	defer span.End()
	span.SetAttributes(
		attribute.String("notification.channel", string(n.Channel)),
		attribute.String("notification.priority", string(n.Priority)),
	)

	var existing *domain.Notification

	if n.ScheduledAt.Valid {
		n.Status = domain.StatusScheduled
	} else {
		n.Status = domain.StatusPending
	}

	err := s.txr.WithTransaction(ctx, func(tx pgx.Tx) error {
		if n.IdempotencyKey.Valid && n.IdempotencyKey.String != "" {
			ex, err := s.repo.GetByIdempotencyKeyTx(ctx, tx, n.IdempotencyKey.String)
			if err != nil && !errors.Is(err, repository.ErrNotFound) {
				return err
			}
			if ex != nil {
				existing = ex
				return nil
			}
		}
		if err := s.repo.Create(ctx, tx, n); err != nil {
			return err
		}
		if !n.ScheduledAt.Valid {
			return s.eventRepo.Insert(ctx, tx, n.ID)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, repository.ErrDuplicate) && n.IdempotencyKey.Valid {
			ex, _ := s.repo.GetByIdempotencyKey(ctx, n.IdempotencyKey.String)
			if ex != nil {
				return ex, false, nil
			}
		}
		return nil, false, err
	}
	if existing != nil {
		return existing, false, nil
	}
	// Create already set n.CreatedAt, n.UpdatedAt via RETURNING; return n.
	return n, true, nil
}

// BatchCreate creates multiple notifications in a single transaction.
func (s *NotificationService) BatchCreate(ctx context.Context, notifications []*domain.Notification) ([]*domain.Notification, error) {
	ctx, span := tracer.Start(ctx, "service.BatchCreate")
	defer span.End()
	span.SetAttributes(attribute.Int("batch.size", len(notifications)))

	batchID := uuid.NewString()

	err := s.txr.WithTransaction(ctx, func(tx pgx.Tx) error {
		for _, n := range notifications {
			n.Status = domain.StatusPending
			n.BatchID = null.StringFrom(batchID)
			if err := s.repo.Create(ctx, tx, n); err != nil {
				return err
			}
			if err := s.eventRepo.Insert(ctx, tx, n.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return notifications, nil
}

// Get retrieves a single notification by ID.
func (s *NotificationService) Get(ctx context.Context, id string) (*domain.Notification, error) {
	return s.repo.GetByID(ctx, id)
}

// List returns a paginated list of notifications.
func (s *NotificationService) List(ctx context.Context, f domain.Filter) ([]domain.Notification, string, error) {
	return s.repo.List(ctx, f)
}

// Cancel cancels a pending notification.
func (s *NotificationService) Cancel(ctx context.Context, id string) error {
	return s.repo.Cancel(ctx, id)
}
