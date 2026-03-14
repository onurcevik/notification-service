package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePoolPinger implements PoolPinger for tests.
type fakePoolPinger struct {
	pingErr error
}

func (f *fakePoolPinger) Ping(ctx context.Context) error { return f.pingErr }

// fakeRedisPinger implements RedisPinger for tests.
type fakeRedisPinger struct {
	pingErr error
}

func (f *fakeRedisPinger) Ping(ctx context.Context) error { return f.pingErr }

func TestHealthHandler_Health(t *testing.T) {
	t.Run("200 when postgres and redis ok", func(t *testing.T) {
		h := &HealthHandler{
			pool: &fakePoolPinger{},
			rdb:  &fakeRedisPinger{},
		}
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()
		h.Health(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
		var body map[string]interface{}
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
		assert.Equal(t, "ok", body["status"])
		assert.Equal(t, "ok", body["postgres"])
		assert.Equal(t, "ok", body["redis"])
	})

	t.Run("503 when postgres unavailable", func(t *testing.T) {
		h := &HealthHandler{
			pool: &fakePoolPinger{pingErr: errors.New("connection refused")},
			rdb:  &fakeRedisPinger{},
		}
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()
		h.Health(rr, req)
		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
		var body map[string]interface{}
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
		assert.Equal(t, "unhealthy", body["status"])
		assert.Equal(t, "unavailable", body["postgres"])
		assert.Equal(t, "ok", body["redis"])
	})

	t.Run("503 when redis unavailable", func(t *testing.T) {
		h := &HealthHandler{
			pool: &fakePoolPinger{},
			rdb:  &fakeRedisPinger{pingErr: errors.New("connection refused")},
		}
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()
		h.Health(rr, req)
		assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
		var body map[string]interface{}
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
		assert.Equal(t, "unhealthy", body["status"])
		assert.Equal(t, "unavailable", body["redis"])
	})

	t.Run("circuit_breaker in body when CircuitStateReader set", func(t *testing.T) {
		circuitStates := &mockCircuitStateReader{states: map[string]string{"sms": "open", "email": "closed", "push": "half-open"}}
		h := &HealthHandler{
			pool:          &fakePoolPinger{},
			rdb:           &fakeRedisPinger{},
			circuitStates: circuitStates,
		}
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rr := httptest.NewRecorder()
		h.Health(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code)
		var body map[string]interface{}
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
		cb, ok := body["circuit_breaker"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "open", cb["sms"])
		assert.Equal(t, "closed", cb["email"])
		assert.Equal(t, "half-open", cb["push"])
	})
}

type mockCircuitStateReader struct {
	states map[string]string
}

func (m *mockCircuitStateReader) CircuitState(channel string) string {
	if m.states == nil {
		return ""
	}
	return m.states[channel]
}
