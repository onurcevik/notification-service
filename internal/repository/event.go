package repository

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresEventRepository struct {
	pool *pgxpool.Pool
}

// NewEventRepository creates a new PostgreSQL implementation of the event repository.
func NewEventRepository(pool *pgxpool.Pool) *postgresEventRepository {
	return &postgresEventRepository{pool: pool}
}

// Insert creates a new outbox event for a notification.
func (r *postgresEventRepository) Insert(ctx context.Context, tx pgx.Tx, notificationID string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO events (id, notification_id, published, created_at)
		VALUES ($1, $2, false, NOW())`,
		uuid.NewString(), notificationID,
	)
	return err
}

// FetchUnpublished retrieves a list of outbox events that haven't been published yet.
func (r *postgresEventRepository) FetchUnpublished(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT notification_id FROM events
		WHERE  published = false
		ORDER  BY created_at ASC
		LIMIT  100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringSlice(rows)
}

// MarkPublished marks a list of notifications as published in the outbox.
func (r *postgresEventRepository) MarkPublished(ctx context.Context, ids []string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE events
		SET    published = true
		WHERE  notification_id = ANY($1::uuid[])
		AND    published = false`, ids)
	return err
}

// FetchStale retrieves unpublished events that have been stuck for more than 30 seconds.
func (r *postgresEventRepository) FetchStale(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT notification_id FROM events
		WHERE  published  = false
		  AND  created_at < NOW() - INTERVAL '30 seconds'
		ORDER  BY created_at ASC
		LIMIT  100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringSlice(rows)
}

func scanStringSlice(rows pgx.Rows) ([]string, error) {
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
