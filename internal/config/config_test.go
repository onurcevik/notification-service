package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear relevant env so we get defaults (other tests may set them)
	t.Setenv("DB_HOST", "")
	t.Setenv("DB_PORT", "")
	t.Setenv("PORT", "")
	t.Setenv("WEBHOOK_URL", "")
	cfg := Load()

	assert.Equal(t, "localhost", cfg.DBHost)
	assert.Equal(t, "5432", cfg.DBPort)
	assert.Equal(t, "8080", cfg.Port)
	assert.Contains(t, cfg.WebhookURL, "webhook.site")
	assert.Equal(t, 10, cfg.WorkerCountPerChannel)
}

func TestLoad_EnvOverrides(t *testing.T) {
	t.Setenv("DB_HOST", "db.example.com")
	t.Setenv("DB_PORT", "5433")
	t.Setenv("PORT", "9000")
	t.Setenv("WEBHOOK_URL", "https://hook.example.com")
	t.Setenv("WORKER_COUNT_PER_CHANNEL", "5")
	cfg := Load()

	assert.Equal(t, "db.example.com", cfg.DBHost)
	assert.Equal(t, "5433", cfg.DBPort)
	assert.Equal(t, "9000", cfg.Port)
	assert.Equal(t, "https://hook.example.com", cfg.WebhookURL)
	assert.Equal(t, 5, cfg.WorkerCountPerChannel)
}

func TestLoad_DatabaseURLFormat(t *testing.T) {
	t.Setenv("DB_HOST", "myhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_NAME", "mydb")
	t.Setenv("DB_USER", "myuser")
	t.Setenv("DB_PASSWORD", "mypass")
	t.Setenv("DB_SSL_MODE", "")
	t.Setenv("DB_SSLMODE", "disable")
	cfg := Load()

	require.NotEmpty(t, cfg.DatabaseURL)
	assert.Contains(t, cfg.DatabaseURL, "postgres://")
	assert.Contains(t, cfg.DatabaseURL, "myuser")
	assert.Contains(t, cfg.DatabaseURL, "mypass")
	assert.Contains(t, cfg.DatabaseURL, "myhost")
	assert.Contains(t, cfg.DatabaseURL, "5432")
	assert.Contains(t, cfg.DatabaseURL, "mydb")
	assert.True(t, strings.HasSuffix(cfg.DatabaseURL, "sslmode=disable") || strings.Contains(cfg.DatabaseURL, "sslmode=disable"))
}

func TestLoad_RedisURLFormat(t *testing.T) {
	t.Setenv("REDIS_HOST", "redis.example.com")
	t.Setenv("REDIS_PORT", "6380")
	cfg := Load()

	assert.Equal(t, "redis.example.com:6380", cfg.RedisURL)
}
