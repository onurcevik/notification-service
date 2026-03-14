package repository

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	// ErrNotFound is returned when a requested record does not exist.
	ErrNotFound = errors.New("not found")
	// ErrDuplicate is returned when a unique constraint (like idempotency key) is violated.
	ErrDuplicate = errors.New("duplicate idempotency key")
	// ErrCannotCancel is returned when attempting to cancel a notification that is not in a pending state.
	ErrCannotCancel = errors.New("only pending notifications can be cancelled")
)

// mapError converts database-specific errors into domain-level repository errors.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrDuplicate
	}
	return err
}
