package handler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_validate(t *testing.T) {
	tests := []struct {
		name   string
		req    CreateNotificationRequest
		assert func(t *testing.T, errs []FieldError)
	}{
		// SMS
		{
			name: "sms valid E164 and content 1-160",
			req:  CreateNotificationRequest{Channel: "sms", Recipient: "+15551234567", Content: "Hello"},
			assert: func(t *testing.T, errs []FieldError) {
				assert.Empty(t, errs)
			},
		},
		{
			name: "sms invalid recipient",
			req:  CreateNotificationRequest{Channel: "sms", Recipient: "not-e164", Content: "Hi"},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "recipient", errs[0].Field)
				assert.Contains(t, errs[0].Message, "E.164")
			},
		},
		{
			name: "sms content empty",
			req:  CreateNotificationRequest{Channel: "sms", Recipient: "+15551234567", Content: ""},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "content", errs[0].Field)
			},
		},
		{
			name: "sms content over 160",
			req:  CreateNotificationRequest{Channel: "sms", Recipient: "+15551234567", Content: string(make([]byte, 161))},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "content", errs[0].Field)
			},
		},
		// Email
		{
			name: "email valid",
			req:  CreateNotificationRequest{Channel: "email", Recipient: "user@example.com", Content: "Hi"},
			assert: func(t *testing.T, errs []FieldError) {
				assert.Empty(t, errs)
			},
		},
		{
			name: "email invalid recipient",
			req:  CreateNotificationRequest{Channel: "email", Recipient: "not-an-email", Content: "Hi"},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "recipient", errs[0].Field)
				assert.Contains(t, errs[0].Message, "email")
			},
		},
		{
			name: "email content over 1000",
			req:  CreateNotificationRequest{Channel: "email", Recipient: "a@b.co", Content: string(make([]byte, 1001))},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "content", errs[0].Field)
			},
		},
		// Push
		{
			name: "push valid",
			req:  CreateNotificationRequest{Channel: "push", Recipient: "device-token-123", Content: "Hi"},
			assert: func(t *testing.T, errs []FieldError) {
				assert.Empty(t, errs)
			},
		},
		{
			name: "push empty recipient",
			req:  CreateNotificationRequest{Channel: "push", Recipient: "", Content: "Hi"},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "recipient", errs[0].Field)
				assert.Contains(t, errs[0].Message, "device token")
			},
		},
		{
			name: "push content over 256",
			req:  CreateNotificationRequest{Channel: "push", Recipient: "token", Content: string(make([]byte, 257))},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "content", errs[0].Field)
			},
		},
		// Channel
		{
			name: "invalid channel",
			req:  CreateNotificationRequest{Channel: "invalid", Recipient: "+15551234567", Content: "Hi"},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "channel", errs[0].Field)
				assert.Contains(t, errs[0].Message, "sms, email or push")
			},
		},
		// Priority
		{
			name: "priority invalid",
			req:  CreateNotificationRequest{Channel: "sms", Recipient: "+15551234567", Content: "Hi", Priority: "urgent"},
			assert: func(t *testing.T, errs []FieldError) {
				require.Len(t, errs, 1)
				assert.Equal(t, "priority", errs[0].Field)
				assert.Contains(t, errs[0].Message, "high, normal or low")
			},
		},
		{
			name: "priority high valid",
			req:  CreateNotificationRequest{Channel: "sms", Recipient: "+15551234567", Content: "Hi", Priority: "high"},
			assert: func(t *testing.T, errs []FieldError) {
				assert.Empty(t, errs)
			},
		},
		{
			name: "priority normal valid",
			req:  CreateNotificationRequest{Channel: "sms", Recipient: "+15551234567", Content: "Hi", Priority: "normal"},
			assert: func(t *testing.T, errs []FieldError) {
				assert.Empty(t, errs)
			},
		},
		{
			name: "priority low valid",
			req:  CreateNotificationRequest{Channel: "sms", Recipient: "+15551234567", Content: "Hi", Priority: "low"},
			assert: func(t *testing.T, errs []FieldError) {
				assert.Empty(t, errs)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := validate(tt.req)
			tt.assert(t, errs)
		})
	}
}
