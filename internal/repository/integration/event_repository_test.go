package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/onurcevik/notification-service/internal/repository"
)

func TestEventRepository_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	pool, cleanup := pgPool(t, ctx)
	defer cleanup()
	applyMigrations(t, pool)

	notifRepo := repository.NewNotificationRepository(pool)
	eventRepo := repository.NewEventRepository(pool)
	txr := repository.NewTransactor(pool)

	// Create a notification first (events reference notifications)
	n := &domain.Notification{
		Channel:   domain.ChannelSMS,
		Priority:  domain.PriorityNormal,
		Status:    domain.StatusPending,
		Recipient: "+15552222222",
		Content:   "Event test",
	}
	err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
		return notifRepo.Create(ctx, tx, n)
	})
	if err != nil {
		t.Fatalf("Create notification: %v", err)
	}

	t.Run("Insert and FetchUnpublished", func(t *testing.T) {
		err := txr.WithTransaction(ctx, func(tx pgx.Tx) error {
			return eventRepo.Insert(ctx, tx, n.ID)
		})
		if err != nil {
			t.Fatalf("Insert: %v", err)
		}
		ids, err := eventRepo.FetchUnpublished(ctx)
		if err != nil {
			t.Fatalf("FetchUnpublished: %v", err)
		}
		var found bool
		for _, id := range ids {
			if id == n.ID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("FetchUnpublished: did not return %s", n.ID)
		}
	})

	t.Run("MarkPublished", func(t *testing.T) {
		err := eventRepo.MarkPublished(ctx, []string{n.ID})
		if err != nil {
			t.Fatalf("MarkPublished: %v", err)
		}
		ids, err := eventRepo.FetchUnpublished(ctx)
		if err != nil {
			t.Fatalf("FetchUnpublished after MarkPublished: %v", err)
		}
		for _, id := range ids {
			if id == n.ID {
				t.Error("MarkPublished: notification still in FetchUnpublished")
				break
			}
		}
	})
}
