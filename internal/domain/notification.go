package domain

import (
	"time"

	"gopkg.in/guregu/null.v4"
)

// Channel represents the communication medium for a notification.
type Channel string

// Priority represents the urgency of a notification used in priority queues.
type Priority string

// Status represents the current lifecycle state of a notification.
type Status string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusDelivered  Status = "delivered"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
	StatusScheduled  Status = "scheduled"
)

// Notification represents the core notification entity.
type Notification struct {
	ID             string
	IdempotencyKey null.String
	BatchID        null.String
	Channel        Channel
	Priority       Priority
	Status         Status
	Recipient      string
	Content        string
	ScheduledAt    null.Time
	AttemptCount   int
	ProviderMsgID  null.String
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Filter defines criteria for listing notifications (query by status, channel, batch, date range, cursor pagination).
type Filter struct {
	// Status filters by notification status (e.g. pending, delivered).
	Status *Status
	// Channel filters by delivery channel (sms, email, push).
	Channel *Channel
	// BatchID filters by batch ID (UUID); use when querying notifications from a batch create.
	BatchID *string
	// CreatedAfter returns only notifications created at or after this time (RFC3339).
	CreatedAfter *time.Time
	// CreatedBefore returns only notifications created before this time (RFC3339).
	CreatedBefore *time.Time
	// CursorAfter and CursorLastID together form the pagination cursor (opaque; from next_cursor).
	CursorAfter *time.Time
	CursorLastID *string
	// Limit is the page size (default 20, max 100).
	Limit int
}

// DeliveryResult represents the outcome of a provider delivery attempt.
type DeliveryResult struct {
	ProviderMessageID string
	Status            string
	Timestamp         time.Time
}
