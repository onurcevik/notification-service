// Package integration contains end-to-end tests for the notification service.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	redis_v9 "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/onurcevik/notification-service/internal/config"
	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/onurcevik/notification-service/internal/handler"
	"github.com/onurcevik/notification-service/internal/queue"
	"github.com/onurcevik/notification-service/internal/ratelimit"
	"github.com/onurcevik/notification-service/internal/repository"
	"github.com/onurcevik/notification-service/internal/service"
	"github.com/onurcevik/notification-service/internal/worker"
)

func init() {
	// Disable Testcontainers reaper (Ryuk) so integration tests work out of the box
	// in CI and on reviewer machines where the reaper often fails (e.g. Docker-in-Docker).
	// Containers are still cleaned up via defer Terminate() in each test.
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}

// TestNotificationFlow verifies the end-to-end lifecycle of a notification:
// API Create -> service saves to DB + outbox; relay polls outbox and enqueues to Redis;
// worker consumes from queue (rate limit, provider), updates status in DB.
// We assert: response 201, notification reaches StatusDelivered, and data can be read back from DB.
func TestNotificationFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	// Start Postgres
	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("notifications"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pgContainer.Terminate(ctx) }()

	pgConnStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	// Start Redis
	redisContainer, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = redisContainer.Terminate(ctx) }()

	redisHost, err := redisContainer.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	redisPort, err := redisContainer.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	// Setup Database & Redis
	pool, err := pgxpool.New(ctx, pgConnStr)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	rdb := redis_v9.NewClient(&redis_v9.Options{Addr: redisAddr})
	defer rdb.Close()

	applyMigrations(t, pool)

	// App Configuration (only fields used by this test)
	cfg := &config.Config{
		RateLimitPerSec:       100,
		MaxRetryAttempts:      3,
		OutboxPollInterval:    10 * time.Millisecond,
		WorkerCountPerChannel: 1,
	}

	// Internal Wiring
	notifRepo := repository.NewNotificationRepository(pool)
	eventRepo := repository.NewEventRepository(pool)
	txr := repository.NewTransactor(pool)

	notifSvc := service.NewNotificationService(notifRepo, eventRepo, txr)
	redisQueue := queue.NewRedisQueue(rdb, uuid.NewString(), -1) // -1 = default short block
	if err := redisQueue.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Start Background Workers
	relay := queue.NewOutboxRelay(notifRepo, eventRepo, redisQueue, cfg.OutboxPollInterval)
	go relay.Run(ctx)

	metrics := &handler.Metrics{}
	limiter := ratelimit.NewRedisLimiter(rdb, cfg.RateLimitPerSec)

	mockProvider := &mockProvider{}
	workerPool := worker.NewPool(
		redisQueue,
		notifRepo,
		mockProvider,
		limiter,
		metrics,
		worker.ProcessorConfig{
			MaxRetryAttempts:      cfg.MaxRetryAttempts,
			WorkerCountPerChannel: cfg.WorkerCountPerChannel,
		},
		worker.CircuitBreakerConfig{},
		nil,
	)
	go workerPool.Run(ctx)

	// API Handler
	h := handler.NewNotificationHandler(notifSvc)

	// Execute Request
	payload := `{"recipient": "+905550000000", "channel": "sms", "content": "Test integration", "priority": "high"}`
	req := httptest.NewRequest(http.MethodPost, "/notifications", strings.NewReader(payload))
	rr := httptest.NewRecorder()

	h.Create(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rr.Code)
	}

	// Parse response to get created notification ID (confirms API and service returned saved data)
	var createResp struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&createResp); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if createResp.ID == "" {
		t.Fatal("create response missing id")
	}

	// Wait for processing to complete (relay → queue → worker → provider → status update)
	deadline := time.Now().Add(10 * time.Second)
	var finalStatus domain.Status
	for time.Now().Before(deadline) {
		notifs, _, err := notifRepo.List(ctx, domain.Filter{Limit: 1})
		if err != nil {
			t.Fatalf("list notifications: %v", err)
		}
		if len(notifs) > 0 {
			finalStatus = notifs[0].Status
			if finalStatus == domain.StatusDelivered {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	if finalStatus != domain.StatusDelivered {
		t.Errorf("expected status delivered, got %s", finalStatus)
	}

	// Confirm data was saved and can be retrieved from DB with correct fields and final status
	got, err := notifRepo.GetByID(ctx, createResp.ID)
	if err != nil {
		t.Fatalf("get notification by id (data saved/retrieved): %v", err)
	}
	if got.Recipient != "+905550000000" {
		t.Errorf("get by id: recipient: got %s", got.Recipient)
	}
	if got.Content != "Test integration" {
		t.Errorf("get by id: content: got %s", got.Content)
	}
	if got.Status != domain.StatusDelivered {
		t.Errorf("get by id: status: got %s", got.Status)
	}
}

// TestHealth_Integration GETs /health with real Postgres and Redis and asserts status and body shape.
func TestHealth_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("notifications"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pgContainer.Terminate(ctx) }()

	pgConnStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, pgConnStr)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	redisContainer, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = redisContainer.Terminate(ctx) }()

	redisHost, err := redisContainer.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	redisPort, err := redisContainer.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())
	rdb := redis_v9.NewClient(&redis_v9.Options{Addr: redisAddr})
	defer rdb.Close()

	healthHandler := handler.NewHealthHandler(pool, rdb, nil)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	healthHandler.Health(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200 when dependencies are up, got %d", rr.Code)
	}

	// Assert body shape: status, postgres, redis
	var body map[string]interface{}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("health response not JSON: %v", err)
	}
	for _, key := range []string{"status", "postgres", "redis"} {
		if _, ok := body[key]; !ok {
			t.Errorf("health response missing key %q", key)
		}
	}
	if body["status"] != "ok" {
		t.Errorf("expected status ok, got %v", body["status"])
	}
	if body["postgres"] != "ok" || body["redis"] != "ok" {
		t.Errorf("expected postgres and redis ok, got postgres=%v redis=%v", body["postgres"], body["redis"])
	}
}

// applyMigrations discovers all *.up.sql files in the migrations directory,
// applies them in lexicographic order (000001, 000002, ...). New migrations
// are picked up automatically
func applyMigrations(t *testing.T, pool *pgxpool.Pool) {
	dir := "migrations"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		dir = "../../migrations"
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		files = append(files, filepath.Join(dir, e.Name()))
	}
	sort.Strings(files)
	for _, f := range files {
		content, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read migration %s: %v", f, err)
		}
		_, err = pool.Exec(context.Background(), string(content))
		if err != nil {
			t.Fatalf("apply migration %s: %v", f, err)
		}
	}
}

type mockProvider struct{}

// Send implements the Provider interface for testing.
func (m *mockProvider) Send(ctx context.Context, n *domain.Notification) (*domain.DeliveryResult, error) {
	return &domain.DeliveryResult{
		ProviderMessageID: "mock-123",
		Status:            "accepted",
		Timestamp:         time.Now(),
	}, nil
}
