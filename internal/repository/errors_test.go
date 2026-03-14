package repository

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapError(t *testing.T) {
	t.Run("nil returns nil", func(t *testing.T) {
		assert.Nil(t, mapError(nil))
	})

	t.Run("PgError code 23505 returns ErrDuplicate", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: "23505", Message: "duplicate key"}
		err := mapError(pgErr)
		require.Error(t, err)
		assert.Same(t, ErrDuplicate, err)
	})

	t.Run("PgError other code returns same error", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: "23503", Message: "foreign key violation"}
		err := mapError(pgErr)
		require.Error(t, err)
		assert.Same(t, pgErr, err)
	})

	t.Run("non-PgError returns same error", func(t *testing.T) {
		plainErr := errors.New("connection refused")
		err := mapError(plainErr)
		require.Error(t, err)
		assert.Same(t, plainErr, err)
	})
}
