package handler

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	httpSwagger "github.com/swaggo/http-swagger"

	_ "gitlab.com/onurcevik/notification-service/docs"
)

// NewRouter initializes the chi router with middlewares and all service routes.
func NewRouter(
	notifSvc NotificationService,
	pool *pgxpool.Pool,
	rdb *redis.Client,
	metrics *Metrics,
	circuitStates CircuitStateReader,
	statusHub StatusHub,
) http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	r.Use(correlationIDMiddleware)
	r.Use(loggerMiddleware)

	notifHandler := NewNotificationHandler(notifSvc)
	healthHandler := NewHealthHandler(pool, rdb, circuitStates)
	metricsHandler := NewMetricsHandler(metrics, rdb, circuitStates)

	r.Route("/v1", func(r chi.Router) {
		r.Post("/notifications", notifHandler.Create)
		r.Post("/notifications/batch", notifHandler.CreateBatch)
		r.Get("/notifications", notifHandler.List)
		if statusHub != nil {
			r.Get("/notifications/stream", NewWebSocketHandler(statusHub).ServeHTTP)
		}
		r.Get("/notifications/{id}", notifHandler.Get)
		r.Delete("/notifications/{id}", notifHandler.Cancel)
	})

	r.Get("/health", healthHandler.Health)
	r.Get("/metrics", metricsHandler.Metrics)
	r.Get("/swagger/*", httpSwagger.WrapHandler)

	return r
}
