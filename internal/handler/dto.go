package handler

import (
	"time"

	"gopkg.in/guregu/null.v4"

	"github.com/onurcevik/notification-service/internal/domain"
)

// CreateNotificationRequest defines the payload for creating a single notification.
// Either provide content (raw body) or template + template_vars for inline variable substitution.
type CreateNotificationRequest struct {
	// IdempotencyKey is an optional client-generated key to prevent duplicate sends (e.g. UUID or unique business id). If you retry with the same key, the same notification is returned.
	IdempotencyKey string `json:"idempotency_key" example:"550e8400-e29b-41d4-a716-446655440000"`
	// Channel is the delivery channel.
	Channel string `json:"channel" enums:"sms,email,push" example:"sms"`
	// Priority controls queue ordering: high, normal, or low. Defaults to normal if omitted.
	Priority string `json:"priority" enums:"high,normal,low" example:"normal"`
	// Recipient is the destination: phone number (SMS), email address, or device token (push).
	Recipient string `json:"recipient" example:"+15551234567"`
	// Content is the raw message body. Use this or template + template_vars.
	Content string `json:"content" example:"Your code is 123456"`
	// ScheduledAt is the optional send time in RFC3339 format (e.g. 2025-12-31T09:00:00Z). Omit for immediate send.
	ScheduledAt *time.Time `json:"scheduled_at" example:"2025-12-31T09:00:00Z"`
	// Template is an optional body with Go template placeholders (e.g. "Hello {{.name}}, your code is {{.code}}"). Use with template_vars; rendered result becomes content.
	Template string `json:"template" example:"Hello {{.name}}, your code is {{.code}}"`
	// TemplateVars are key-value pairs for template substitution (e.g. {"name":"Alice","code":"123"}).
	TemplateVars map[string]string `json:"template_vars"`
}

// BatchCreateRequest defines the payload for creating multiple notifications.
type BatchCreateRequest struct {
	Notifications []CreateNotificationRequest `json:"notifications"`
}

// NotificationResponse is the standard response for a single notification.
type NotificationResponse struct {
	ID             string      `json:"id" example:"550e8400-e29b-41d4-a716-446655440000"`
	IdempotencyKey null.String `json:"idempotency_key"`
	BatchID        null.String `json:"batch_id"`
	Channel        string      `json:"channel" enums:"sms,email,push"`
	Priority       string      `json:"priority" enums:"high,normal,low"`
	Status         string      `json:"status" enums:"pending,processing,delivered,failed,cancelled,scheduled"`
	Recipient      string      `json:"recipient"`
	Content        string      `json:"content"`
	ScheduledAt    null.Time   `json:"scheduled_at"`
	AttemptCount   int         `json:"attempt_count"`
	ProviderMsgID  null.String `json:"provider_msg_id"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
}

// BatchCreateResponse is the response for a batch creation request.
type BatchCreateResponse struct {
	BatchID       string                 `json:"batch_id"`
	Count         int                    `json:"count"`
	Notifications []NotificationResponse `json:"notifications"`
}

// ListResponse is the response for a paginated list of notifications.
type ListResponse struct {
	Notifications []NotificationResponse `json:"notifications"`
	NextCursor    string                 `json:"next_cursor,omitempty"`
}

// ErrorResponse is the standard error response format.
type ErrorResponse struct {
	Error  string       `json:"error"`
	Fields []FieldError `json:"fields,omitempty"`
}

// FieldError contains details about an invalid field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// toDomain converts CreateNotificationRequest to a domain model.
func toDomain(r CreateNotificationRequest) *domain.Notification {
	n := &domain.Notification{
		Channel:   domain.Channel(r.Channel),
		Priority:  domain.Priority(r.Priority),
		Recipient: r.Recipient,
		Content:   r.Content,
	}
	if n.Priority == "" {
		n.Priority = domain.PriorityNormal
	}
	if r.IdempotencyKey != "" {
		n.IdempotencyKey = null.StringFrom(r.IdempotencyKey)
	}
	if r.ScheduledAt != nil {
		n.ScheduledAt = null.TimeFrom(*r.ScheduledAt)
	}
	return n
}

// toResponse converts a domain model to a NotificationResponse.
func toResponse(n *domain.Notification) NotificationResponse {
	return NotificationResponse{
		ID:             n.ID,
		IdempotencyKey: n.IdempotencyKey,
		BatchID:        n.BatchID,
		Channel:        string(n.Channel),
		Priority:       string(n.Priority),
		Status:         string(n.Status),
		Recipient:      n.Recipient,
		Content:        n.Content,
		ScheduledAt:    n.ScheduledAt,
		AttemptCount:   n.AttemptCount,
		ProviderMsgID:  n.ProviderMsgID,
		CreatedAt:      n.CreatedAt,
		UpdatedAt:      n.UpdatedAt,
	}
}

// toResponseList converts a list of domain models to a list of responses.
func toResponseList(ns []domain.Notification) []NotificationResponse {
	out := make([]NotificationResponse, len(ns))
	for i := range ns {
		out[i] = toResponse(&ns[i])
	}
	return out
}
