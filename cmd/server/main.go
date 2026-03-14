// Package main runs the notification service HTTP server, workers, and outbox relay.
//
//	@title						Notification Service API
//	@version					1.0
//	@description					Event-driven notification system: create, list, cancel notifications; batch create; WebSocket status stream.
//	@BasePath						/v1
//	@host							localhost:8080
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"gitlab.com/onurcevik/notification-service/internal/config"
	"gitlab.com/onurcevik/notification-service/internal/handler"
	"gitlab.com/onurcevik/notification-service/internal/hub"
	"gitlab.com/onurcevik/notification-service/internal/provider"
	"gitlab.com/onurcevik/notification-service/internal/queue"
	"gitlab.com/onurcevik/notification-service/internal/ratelimit"
	"gitlab.com/onurcevik/notification-service/internal/repository"
	"gitlab.com/onurcevik/notification-service/internal/service"
	"gitlab.com/onurcevik/notification-service/internal/telemetry"
	"gitlab.com/onurcevik/notification-service/internal/worker"
)

func main() {
	cfg := config.Load()
	logger := setupLogger()
	log.Logger = logger

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tracerShutdown, err := telemetry.InitTracer(nil)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init tracer")
	}
	defer func() {
		if err := tracerShutdown(context.Background()); err != nil {
			logger.Error().Err(err).Msg("tracer shutdown failed")
		}
	}()

	pool, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to postgres")
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		logger.Fatal().Err(err).Msg("postgres ping failed")
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisURL})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Fatal().Err(err).Msg("redis ping failed")
	}
	defer rdb.Close()

	notifRepo := repository.NewNotificationRepository(pool)
	eventRepo := repository.NewEventRepository(pool)
	txr := repository.NewTransactor(pool)

	notifSvc := service.NewNotificationService(notifRepo, eventRepo, txr)
	scheduler := service.NewScheduler(notifRepo, eventRepo, txr, cfg.SchedulerInterval)

	consumeBlock := time.Duration(cfg.QueueConsumeBlockMs) * time.Millisecond
	if consumeBlock <= 0 {
		consumeBlock = 100 * time.Millisecond
	}
	redisQueue := queue.NewRedisQueue(rdb, uuid.NewString(), consumeBlock)
	if err := redisQueue.Init(context.Background()); err != nil {
		logger.Fatal().Err(err).Msg("failed to init redis streams")
	}
	relay := queue.NewOutboxRelay(notifRepo, eventRepo, redisQueue, cfg.OutboxPollInterval)

	metrics := &handler.Metrics{}
	limiter := ratelimit.NewRedisLimiter(rdb, cfg.RateLimitPerSec)
	statusHub := hub.New()
	workerPool := worker.NewPool(
		redisQueue,
		notifRepo,
		provider.NewWebhookProvider(cfg.WebhookURL),
		limiter,
		metrics,
		worker.ProcessorConfig{
			MaxRetryAttempts:      cfg.MaxRetryAttempts,
			WorkerCountPerChannel: cfg.WorkerCountPerChannel,
		},
		worker.CircuitBreakerConfig{
			ConsecutiveFailures: cfg.CircuitBreakerConsecutiveFailures,
			TimeoutSec:          cfg.CircuitBreakerTimeoutSec,
			HalfOpenMaxRequests: cfg.CircuitBreakerHalfOpenMaxRequests,
		},
		statusHub,
	)

	router := handler.NewRouter(notifSvc, pool, rdb, metrics, workerPool, statusHub)
	h := otelhttp.NewHandler(router, "notification-service")
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      h,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go relay.Run(ctx) // relay is responsible for enqueuing notifications into the queue
	go workerPool.Run(ctx) // worker pool is responsible for processing notifications from the queue
	go scheduler.Run(ctx) // scheduler is responsible for scheduling notifications to be sent at a later time

	go func() {
		logger.Info().Str("port", cfg.Port).Msg("server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("server error")
		}
	}()

	<-ctx.Done()
	logger.Info().Msg("shutting down")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("server shutdown error")
	}
}

// setupLogger configures the global zerolog logger. Call once at startup.
func setupLogger() zerolog.Logger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	out := zerolog.ConsoleWriter{Out: os.Stderr}
	return zerolog.New(out).With().Timestamp().Logger()
}
