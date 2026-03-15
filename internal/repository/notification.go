package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/onurcevik/notification-service/internal/domain"
)

type postgresNotificationRepository struct {
	pool *pgxpool.Pool
}

// NewNotificationRepository creates a new PostgreSQL implementation of the notification repository.
func NewNotificationRepository(pool *pgxpool.Pool) *postgresNotificationRepository {
	return &postgresNotificationRepository{pool: pool}
}

// Create inserts a new notification and returns it with created_at/updated_at from the database.
func (r *postgresNotificationRepository) Create(ctx context.Context, tx pgx.Tx, n *domain.Notification) error {
	n.ID = uuid.NewString()
	err := tx.QueryRow(ctx, `
		INSERT INTO notifications (
			id, idempotency_key, batch_id, channel, priority, status,
			recipient, content, scheduled_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),NOW())
		RETURNING created_at, updated_at`,
		n.ID, n.IdempotencyKey, n.BatchID, n.Channel, n.Priority,
		n.Status, n.Recipient, n.Content, n.ScheduledAt,
	).Scan(&n.CreatedAt, &n.UpdatedAt)
	return mapError(err)
}

// GetByID retrieves a single notification by its primary key.
func (r *postgresNotificationRepository) GetByID(ctx context.Context, id string) (*domain.Notification, error) {
	return scanOne(r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, batch_id, channel, priority, status,
		       recipient, content, scheduled_at, attempt_count,
		       provider_msg_id, created_at, updated_at
		FROM   notifications
		WHERE  id = $1`, id))
}

// GetByIdempotencyKey retrieves a notification by its unique idempotency key.
func (r *postgresNotificationRepository) GetByIdempotencyKey(ctx context.Context, key string) (*domain.Notification, error) {
	return scanOne(r.pool.QueryRow(ctx, `
		SELECT id, idempotency_key, batch_id, channel, priority, status,
		       recipient, content, scheduled_at, attempt_count,
		       provider_msg_id, created_at, updated_at
		FROM   notifications
		WHERE  idempotency_key = $1`, key))
}

// GetByIdempotencyKeyTx retrieves a notification by idempotency key within the given transaction.
func (r *postgresNotificationRepository) GetByIdempotencyKeyTx(ctx context.Context, tx pgx.Tx, key string) (*domain.Notification, error) {
	return scanOne(tx.QueryRow(ctx, `
		SELECT id, idempotency_key, batch_id, channel, priority, status,
		       recipient, content, scheduled_at, attempt_count,
		       provider_msg_id, created_at, updated_at
		FROM   notifications
		WHERE  idempotency_key = $1`, key))
}

// UpdateStatus modifies the current status of a notification.
func (r *postgresNotificationRepository) UpdateStatus(ctx context.Context, id string, status domain.Status, providerMsgID string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE notifications
		SET    status          = $1,
		       provider_msg_id = CASE WHEN $2 = '' THEN provider_msg_id ELSE $2 END,
		       updated_at      = NOW()
		WHERE  id = $3`,
		status, providerMsgID, id,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// IncrementAttempt increases the delivery attempt counter for a notification.
func (r *postgresNotificationRepository) IncrementAttempt(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE notifications
		SET    attempt_count = attempt_count + 1,
		       updated_at    = NOW()
		WHERE  id = $1`, id)
	return err
}

// Cancel marks a pending or scheduled notification as cancelled.
func (r *postgresNotificationRepository) Cancel(ctx context.Context, id string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE notifications
		SET    status     = 'cancelled',
		       updated_at = NOW()
		WHERE  id = $1 AND status IN ('pending', 'scheduled')`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if err := r.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM notifications WHERE id = $1)`, id,
		).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}
		// Exists but status is not pending/scheduled (e.g. already processing/delivered)
		return ErrCannotCancel
	}
	return nil
}

// List returns a list of notifications matching the provided filters.
func (r *postgresNotificationRepository) List(ctx context.Context, f domain.Filter) ([]domain.Notification, string, error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}

	query := `
		SELECT id, idempotency_key, batch_id, channel, priority, status,
		       recipient, content, scheduled_at, attempt_count,
		       provider_msg_id, created_at, updated_at
		FROM   notifications WHERE 1=1`

	args := []any{}
	i := 1

	if f.Status != nil {
		query += fmt.Sprintf(" AND status = $%d", i)
		args = append(args, string(*f.Status))
		i++
	}
	if f.Channel != nil {
		query += fmt.Sprintf(" AND channel = $%d", i)
		args = append(args, string(*f.Channel))
		i++
	}
	if f.BatchID != nil {
		query += fmt.Sprintf(" AND batch_id = $%d", i)
		args = append(args, *f.BatchID)
		i++
	}
	if f.CreatedAfter != nil {
		query += fmt.Sprintf(" AND created_at >= $%d", i)
		args = append(args, *f.CreatedAfter)
		i++
	}
	if f.CreatedBefore != nil {
		query += fmt.Sprintf(" AND created_at <= $%d", i)
		args = append(args, *f.CreatedBefore)
		i++
	}
	if f.CursorAfter != nil && f.CursorLastID != nil {
		query += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", i, i+1)
		args = append(args, *f.CursorAfter, *f.CursorLastID)
		i += 2
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", i)
	args = append(args, f.Limit+1)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var results []domain.Notification
	for rows.Next() {
		n, err := scanRow(rows)
		if err != nil {
			return nil, "", err
		}
		results = append(results, *n)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var cursor string
	if len(results) > f.Limit {
		results = results[:f.Limit]
		last := results[len(results)-1]
		cursor = fmt.Sprintf("%s|%s",
			last.CreatedAt.UTC().Format(time.RFC3339Nano), last.ID)
	}
	return results, cursor, nil
}

// FetchDueScheduled retrieves notifications that have reached their scheduled time.
func (r *postgresNotificationRepository) FetchDueScheduled(ctx context.Context) ([]domain.Notification, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, idempotency_key, batch_id, channel, priority, status,
		       recipient, content, scheduled_at, attempt_count,
		       provider_msg_id, created_at, updated_at
		FROM   notifications
		WHERE  status = 'scheduled' AND scheduled_at <= NOW()
		ORDER  BY scheduled_at ASC
		LIMIT  100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []domain.Notification
	for rows.Next() {
		n, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, *n)
	}
	return results, rows.Err()
}

func scanOne(row pgx.Row) (*domain.Notification, error) {
	var n domain.Notification
	err := row.Scan(
		&n.ID, &n.IdempotencyKey, &n.BatchID, &n.Channel,
		&n.Priority, &n.Status, &n.Recipient, &n.Content,
		&n.ScheduledAt, &n.AttemptCount, &n.ProviderMsgID,
		&n.CreatedAt, &n.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &n, err
}

func scanRow(rows pgx.Rows) (*domain.Notification, error) {
	var n domain.Notification
	err := rows.Scan(
		&n.ID, &n.IdempotencyKey, &n.BatchID, &n.Channel,
		&n.Priority, &n.Status, &n.Recipient, &n.Content,
		&n.ScheduledAt, &n.AttemptCount, &n.ProviderMsgID,
		&n.CreatedAt, &n.UpdatedAt,
	)
	return &n, err
}
