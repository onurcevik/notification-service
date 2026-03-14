package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// PoolPinger is implemented by *pgxpool.Pool for health checks.
type PoolPinger interface {
	Ping(ctx context.Context) error
}

// RedisPinger is implemented by adapters around *redis.Client for health checks.
type RedisPinger interface {
	Ping(ctx context.Context) error
}

// HealthHandler provides health check endpoints for the service and its dependencies.
type HealthHandler struct {
	pool          PoolPinger
	rdb           RedisPinger
	circuitStates CircuitStateReader
}

// NewHealthHandler creates a new HealthHandler.
func NewHealthHandler(pool *pgxpool.Pool, rdb *redis.Client, circuitStates CircuitStateReader) *HealthHandler {
	return &HealthHandler{
		pool:          pool,
		rdb:           &redisPingerAdapter{rdb: rdb},
		circuitStates: circuitStates,
	}
}

// redisPingerAdapter adapts *redis.Client to RedisPinger.
type redisPingerAdapter struct {
	rdb *redis.Client
}

func (a *redisPingerAdapter) Ping(ctx context.Context) error {
	return a.rdb.Ping(ctx).Err()
}

// Health checks the status of PostgreSQL and Redis and returns a JSON response.
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	pgStatus := "ok"
	if err := h.pool.Ping(ctx); err != nil {
		pgStatus = "unavailable"
	}

	redisStatus := "ok"
	if err := h.rdb.Ping(ctx); err != nil {
		redisStatus = "unavailable"
	}

	overallStatus := "ok"
	httpStatus := http.StatusOK
	if pgStatus != "ok" || redisStatus != "ok" {
		overallStatus = "unhealthy"
		httpStatus = http.StatusServiceUnavailable
	}

	resp := map[string]any{
		"status":   overallStatus,
		"postgres": pgStatus,
		"redis":    redisStatus,
	}
	if h.circuitStates != nil {
		// circuit breaker state per channel
		resp["circuit_breaker"] = map[string]string{
			"sms":   h.circuitStates.CircuitState("sms"),
			"email": h.circuitStates.CircuitState("email"),
			"push":  h.circuitStates.CircuitState("push"),
		}
	}
	writeJSON(w, httpStatus, resp)
}
