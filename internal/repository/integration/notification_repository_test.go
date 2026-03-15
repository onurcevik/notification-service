package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/onurcevik/notification-service/internal/repository"
	"gopkg.in/guregu/null.v4"
)

func TestNotificationRepository_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pool, cleanup := pgPool(t, ctx)
	defer cleanup()
	applyMigrations(t, pool)

	notifRepo := repository.NewNotificationRepository(pool)
	txr := repository.NewTransactor(pool)

	t.Run("Create and GetByID", func(t *testing.T) {
		n := &domain.Notification{
			Channel:   domain.ChannelSMS,
			Priority:  domain.PriorityNormal,
			Status:    domain.StatusPending,
			Recipient: "+15551234567",
			Content:   "Hello",
		}
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n)
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if n.ID == "" {
			t.Fatal("Create did not set ID")
		}

		got, err := notifRepo.GetByID(ctx, n.ID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if got.ID != n.ID || got.Recipient != n.Recipient || got.Content != n.Content {
			t.Errorf("GetByID: got %+v", got)
		}
	})

	t.Run("GetByIdempotencyKey", func(t *testing.T) {
		key := "idem-key-1"
		n := &domain.Notification{
			IdempotencyKey: null.StringFrom(key),
			Channel:        domain.ChannelEmail,
			Priority:       domain.PriorityHigh,
			Status:         domain.StatusPending,
			Recipient:      "a@b.com",
			Content:        "Hi",
		}
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n)
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := notifRepo.GetByIdempotencyKey(ctx, key)
		if err != nil {
			t.Fatalf("GetByIdempotencyKey: %v", err)
		}
		if got.IdempotencyKey.String != key || got.Recipient != "a@b.com" {
			t.Errorf("GetByIdempotencyKey: got %+v", got)
		}
	})

	t.Run("Create duplicate idempotency returns ErrDuplicate", func(t *testing.T) {
		key := "idem-dup"
		n := &domain.Notification{
			IdempotencyKey: null.StringFrom(key),
			Channel:       domain.ChannelSMS,
			Priority:      domain.PriorityNormal,
			Status:        domain.StatusPending,
			Recipient:     "+15550000000",
			Content:       "Dup",
		}
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n)
		})
		if err != nil {
			t.Fatalf("first Create: %v", err)
		}
		n2 := &domain.Notification{
			IdempotencyKey: null.StringFrom(key),
			Channel:        domain.ChannelSMS,
			Priority:       domain.PriorityNormal,
			Status:         domain.StatusPending,
			Recipient:     "+15550000001",
			Content:        "Dup2",
		}
		err = txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n2)
		})
		if err != repository.ErrDuplicate {
			t.Errorf("expected ErrDuplicate, got %v", err)
		}
	})

	t.Run("UpdateStatus", func(t *testing.T) {
		n := &domain.Notification{
			Channel:   domain.ChannelPush,
			Priority:  domain.PriorityLow,
			Status:    domain.StatusPending,
			Recipient: "token",
			Content:   "Push",
		}
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n)
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		err = notifRepo.UpdateStatus(ctx, n.ID, domain.StatusDelivered, "ext-123")
		if err != nil {
			t.Fatalf("UpdateStatus: %v", err)
		}
		got, getErr := notifRepo.GetByID(ctx, n.ID)
		require.NoError(t, getErr)
		if got.Status != domain.StatusDelivered || got.ProviderMsgID.String != "ext-123" {
			t.Errorf("UpdateStatus: got status=%s provider_msg_id=%s", got.Status, got.ProviderMsgID.String)
		}
	})

	t.Run("Cancel pending", func(t *testing.T) {
		n := &domain.Notification{
			Channel:   domain.ChannelSMS,
			Priority:  domain.PriorityNormal,
			Status:    domain.StatusPending,
			Recipient: "+15559999999",
			Content:   "Cancel me",
		}
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n)
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		err = notifRepo.Cancel(ctx, n.ID)
		if err != nil {
			t.Fatalf("Cancel: %v", err)
		}
		got, getErr := notifRepo.GetByID(ctx, n.ID)
		require.NoError(t, getErr)
		if got.Status != domain.StatusCancelled {
			t.Errorf("Cancel: got status %s", got.Status)
		}
	})

	t.Run("GetByID not found returns ErrNotFound", func(t *testing.T) {
		_, err := notifRepo.GetByID(ctx, "00000000-0000-0000-0000-000000000000")
		if err != repository.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("Cancel non-existent returns ErrNotFound", func(t *testing.T) {
		err := notifRepo.Cancel(ctx, "00000000-0000-0000-0000-000000000000")
		if err != repository.ErrNotFound {
			t.Errorf("expected ErrNotFound for cancel of non-existent ID, got %v", err)
		}
	})

	t.Run("Cancel delivered returns ErrCannotCancel", func(t *testing.T) {
		n := &domain.Notification{
			Channel:   domain.ChannelSMS,
			Priority:  domain.PriorityNormal,
			Status:    domain.StatusPending,
			Recipient: "+15557777777",
			Content:   "Will be delivered before cancel attempt",
		}
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n)
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		// Move notification to delivered state; Cancel should reject it.
		if err := notifRepo.UpdateStatus(ctx, n.ID, domain.StatusDelivered, "ext-cancel-test"); err != nil {
			t.Fatalf("UpdateStatus to delivered: %v", err)
		}
		err = notifRepo.Cancel(ctx, n.ID)
		if err != repository.ErrCannotCancel {
			t.Errorf("expected ErrCannotCancel for delivered notification, got %v", err)
		}
	})

	t.Run("IncrementAttempt increments counter", func(t *testing.T) {
		n := &domain.Notification{
			Channel:   domain.ChannelEmail,
			Priority:  domain.PriorityNormal,
			Status:    domain.StatusPending,
			Recipient: "inc@test.com",
			Content:   "Increment test",
		}
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n)
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		// Initial attempt count should be 0.
		got, err := notifRepo.GetByID(ctx, n.ID)
		if err != nil {
			t.Fatalf("GetByID: %v", err)
		}
		if got.AttemptCount != 0 {
			t.Errorf("expected initial attempt_count=0, got %d", got.AttemptCount)
		}

		// Increment twice and verify each step.
		for i := 1; i <= 2; i++ {
			if err := notifRepo.IncrementAttempt(ctx, n.ID); err != nil {
				t.Fatalf("IncrementAttempt (call %d): %v", i, err)
			}
			got, err = notifRepo.GetByID(ctx, n.ID)
			if err != nil {
				t.Fatalf("GetByID after increment %d: %v", i, err)
			}
			if got.AttemptCount != i {
				t.Errorf("expected attempt_count=%d after %d increments, got %d", i, i, got.AttemptCount)
			}
		}
	})

	t.Run("List and cursor", func(t *testing.T) {
		list, cursor, err := notifRepo.List(ctx, domain.Filter{Limit: 2})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(list) > 2 {
			t.Errorf("List: expected at most 2, got %d", len(list))
		}
		if len(list) == 2 && cursor == "" {
			t.Error("List: expected cursor when full page")
		}
	})

	t.Run("FetchDueScheduled", func(t *testing.T) {
		past := time.Now().Add(-time.Hour)
		n := &domain.Notification{
			Channel:     domain.ChannelSMS,
			Priority:    domain.PriorityNormal,
			Status:      domain.StatusScheduled,
			Recipient:   "+15551111111",
			Content:     "Scheduled",
			ScheduledAt: null.TimeFrom(past),
		}
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return notifRepo.Create(ctx, tx, n)
		})
		if err != nil {
			t.Fatalf("Create scheduled: %v", err)
		}
		due, err := notifRepo.FetchDueScheduled(ctx)
		if err != nil {
			t.Fatalf("FetchDueScheduled: %v", err)
		}
		var found bool
		for _, d := range due {
			if d.ID == n.ID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("FetchDueScheduled: did not return notification %s", n.ID)
		}
	})
}
