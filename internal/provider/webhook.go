package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/onurcevik/notification-service/internal/domain"
)

var tracer = otel.Tracer("notification-service/provider")

type webhookRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type webhookResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// WebhookProvider simulates an external notification provider.
type WebhookProvider struct {
	url    string
	client *http.Client
}

// NewWebhookProvider creates a new WebhookProvider.
func NewWebhookProvider(url string) *WebhookProvider {
	return NewWebhookProviderWithClient(url, &http.Client{Timeout: 10 * time.Second})
}

// NewWebhookProviderWithClient creates a WebhookProvider with a custom HTTP client (e.g. for tests).
func NewWebhookProviderWithClient(url string, client *http.Client) *WebhookProvider {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &WebhookProvider{url: url, client: client}
}

// Send delivers a notification via HTTP POST to the configured webhook URL.
func (p *WebhookProvider) Send(ctx context.Context, n *domain.Notification) (*domain.DeliveryResult, error) {
	ctx, span := tracer.Start(ctx, "provider.Send")
	defer span.End()
	span.SetAttributes(
		attribute.String("notification.id", n.ID),
		attribute.String("notification.channel", string(n.Channel)),
	)

	body, err := json.Marshal(webhookRequest{
		To:      n.Recipient,
		Channel: string(n.Channel),
		Content: n.Content,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var result webhookResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// If the provider returns a non-standard body, still treat as success.
		result.Status = "accepted"
	}

	t, err := time.Parse(time.RFC3339, result.Timestamp)
	if err != nil {
		// Fallback to now if timestamp format is not RFC3339.
		t = time.Now()
	}

	return &domain.DeliveryResult{
		ProviderMessageID: result.MessageID,
		Status:            result.Status,
		Timestamp:         t,
	}, nil
}
