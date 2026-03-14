package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgxTransactor implements the service.Transactor interface using pgx.
type PgxTransactor struct {
	pool *pgxpool.Pool
}

// NewTransactor creates a new PgxTransactor.
func NewTransactor(pool *pgxpool.Pool) *PgxTransactor {
	return &PgxTransactor{pool: pool}
}

// WithTransaction executes the provided function within a database transaction.
func (t *PgxTransactor) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
