package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the application configuration.
// All values are read from environment variables with consistent naming: DB_*, REDIS_*, etc.
type Config struct {
	// Database
	DBHost      string
	DBPort      string
	DBName      string
	DBUser      string
	DBPassword  string
	DBSSLMode   string
	DatabaseURL string

	// Redis
	RedisHost     string
	RedisPort     string
	RedisPassword string
	RedisURL      string

	// WebhookURL and server port
	WebhookURL string
	Port       string

	// Worker and queue.
	WorkerCountPerChannel int
	RateLimitPerSec       int
	MaxRetryAttempts      int
	OutboxPollInterval    time.Duration
	SchedulerInterval     time.Duration
	QueueConsumeBlockMs   int

	// Circuit breaker.
	CircuitBreakerConsecutiveFailures int
	CircuitBreakerTimeoutSec          int
	CircuitBreakerHalfOpenMaxRequests uint32
}

// Load populates the configuration from environment variables and builds connection URLs.
func Load() *Config {
	cfg := &Config{
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnv("DB_PORT", "5432"),
		DBName:     getEnv("DB_NAME", "notifications"),
		DBUser:     getEnv("DB_USER", "postgres"),
		DBPassword: getEnv("DB_PASSWORD", "postgres"),
		DBSSLMode:  getEnv("DB_SSL_MODE", getEnv("DB_SSLMODE", "disable")),

		RedisHost:     getEnv("REDIS_HOST", "localhost"),
		RedisPort:     getEnv("REDIS_PORT", "6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),

		WebhookURL: getEnv("WEBHOOK_URL", "https://webhook.site/your-uuid-here"),
		Port:       getEnv("PORT", "8080"),

		WorkerCountPerChannel: getEnvInt("WORKER_COUNT_PER_CHANNEL", 10),
		RateLimitPerSec:       getEnvInt("RATE_LIMIT_PER_SEC", 100),
		MaxRetryAttempts:      getEnvInt("MAX_RETRY_ATTEMPTS", 4),
		OutboxPollInterval:    time.Duration(getEnvInt("OUTBOX_POLL_INTERVAL_MS", 100)) * time.Millisecond,
		SchedulerInterval:     time.Duration(getEnvInt("SCHEDULER_INTERVAL_SEC", 5)) * time.Second,
		QueueConsumeBlockMs:   getEnvInt("QUEUE_CONSUME_BLOCK_MS", 100),

		CircuitBreakerConsecutiveFailures: getEnvInt("CIRCUIT_BREAKER_CONSECUTIVE_FAILURES", 5),
		CircuitBreakerTimeoutSec:          getEnvInt("CIRCUIT_BREAKER_TIMEOUT_SEC", 30),
		CircuitBreakerHalfOpenMaxRequests: uint32(getEnvInt("CIRCUIT_BREAKER_HALF_OPEN_MAX_REQUESTS", 1)),
	}

	cfg.DatabaseURL = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName, cfg.DBSSLMode)
	cfg.RedisURL = cfg.RedisHost + ":" + cfg.RedisPort

	return cfg
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
