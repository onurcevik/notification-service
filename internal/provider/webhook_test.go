package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebhookProvider_Send(t *testing.T) {
	notif := &domain.Notification{
		ID: "n-1", Channel: domain.ChannelSMS, Recipient: "+15551234567", Content: "Hello",
	}

	t.Run("200 with JSON body", func(t *testing.T) {
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"messageId":"msg-1","status":"accepted","timestamp":"2025-01-15T10:00:00Z"}`))
		}))
		defer svr.Close()

		p := NewWebhookProvider(svr.URL)
		result, err := p.Send(context.Background(), notif)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "msg-1", result.ProviderMessageID)
		assert.Equal(t, "accepted", result.Status)
		assert.True(t, result.Timestamp.Year() == 2025)
	})

	t.Run("202 accepted", func(t *testing.T) {
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"messageId":"msg-2","status":"queued","timestamp":"2025-01-15T10:00:00Z"}`))
		}))
		defer svr.Close()

		p := NewWebhookProvider(svr.URL)
		result, err := p.Send(context.Background(), notif)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "msg-2", result.ProviderMessageID)
		assert.Equal(t, "queued", result.Status)
	})

	t.Run("4xx returns error", func(t *testing.T) {
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer svr.Close()

		p := NewWebhookProvider(svr.URL)
		result, err := p.Send(context.Background(), notif)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "400")
	})

	t.Run("5xx returns error", func(t *testing.T) {
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer svr.Close()

		p := NewWebhookProvider(svr.URL)
		result, err := p.Send(context.Background(), notif)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "500")
	})

	t.Run("200 with non-JSON body still succeeds", func(t *testing.T) {
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not json"))
		}))
		defer svr.Close()

		p := NewWebhookProvider(svr.URL)
		result, err := p.Send(context.Background(), notif)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "accepted", result.Status) // fallback when JSON decode fails
	})

	t.Run("timeout returns error", func(t *testing.T) {
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}))
		defer svr.Close()

		client := &http.Client{Timeout: 10 * time.Millisecond}
		p := NewWebhookProviderWithClient(svr.URL, client)
		result, err := p.Send(context.Background(), notif)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("connection refused returns error", func(t *testing.T) {
		// Use a URL that nothing listens on (connection refused)
		p := NewWebhookProvider("http://127.0.0.1:19999")
		result, err := p.Send(context.Background(), notif)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}
