package provider

import (
	"context"

	"gitlab.com/onurcevik/notification-service/internal/domain"
)

// Provider defines the interface for delivering notifications to external channels.
type Provider interface {
	// Send delivers the notification and returns a DeliveryResult or an error.
	Send(ctx context.Context, n *domain.Notification) (*domain.DeliveryResult, error)
}
