package handler

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/onurcevik/notification-service/internal/queue"
)

// Metrics stores various service-wide performance counters.
type Metrics struct {
	DeliveredSMS   atomic.Int64
	DeliveredEmail atomic.Int64
	DeliveredPush  atomic.Int64
	FailedSMS      atomic.Int64
	FailedEmail    atomic.Int64
	FailedPush     atomic.Int64

	// Latency tracking: total nanoseconds and count per channel.
	// Average latency = TotalNs / Count.
	LatencySMSNs   atomic.Int64
	LatencySMSCnt  atomic.Int64
	LatencyEmailNs atomic.Int64
	LatencyEmailCnt atomic.Int64
	LatencyPushNs  atomic.Int64
	LatencyPushCnt atomic.Int64
}

// IncDelivered increments the delivery success counter for a specific channel.
func (m *Metrics) IncDelivered(channel string) {
	switch channel {
	case "sms":
		m.DeliveredSMS.Add(1)
	case "email":
		m.DeliveredEmail.Add(1)
	case "push":
		m.DeliveredPush.Add(1)
	}
}

// IncFailed increments the delivery failure counter for a specific channel.
func (m *Metrics) IncFailed(channel string) {
	switch channel {
	case "sms":
		m.FailedSMS.Add(1)
	case "email":
		m.FailedEmail.Add(1)
	case "push":
		m.FailedPush.Add(1)
	}
}

// RecordLatency records delivery latency for a channel.
func (m *Metrics) RecordLatency(channel string, d time.Duration) {
	ns := d.Nanoseconds()
	switch channel {
	case "sms":
		m.LatencySMSNs.Add(ns)
		m.LatencySMSCnt.Add(1)
	case "email":
		m.LatencyEmailNs.Add(ns)
		m.LatencyEmailCnt.Add(1)
	case "push":
		m.LatencyPushNs.Add(ns)
		m.LatencyPushCnt.Add(1)
	}
}

func avgLatencyMs(totalNs, count int64) float64 {
	if count == 0 {
		return 0
	}
	return float64(totalNs) / float64(count) / 1e6
}

// CircuitStateReader provides circuit breaker state per channel for metrics/health.
type CircuitStateReader interface {
	CircuitState(channel string) string
}

// MetricsHandler provides an endpoint to expose real-time metrics.
type MetricsHandler struct {
	metrics       *Metrics
	rdb           *redis.Client
	circuitStates CircuitStateReader
}

// NewMetricsHandler creates a new MetricsHandler.
func NewMetricsHandler(metrics *Metrics, rdb *redis.Client, circuitStates CircuitStateReader) *MetricsHandler {
	return &MetricsHandler{metrics: metrics, rdb: rdb, circuitStates: circuitStates}
}

// Metrics returns the current state of queues and delivery counters in JSON format.
// queue_depth.stream_length is total messages in stream; queue_depth.pending is
// unacknowledged messages in the consumer group (work still to be done).
func (h *MetricsHandler) Metrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	pipe := h.rdb.Pipeline()
	highLen := pipe.XLen(ctx, queue.StreamHigh)
	normalLen := pipe.XLen(ctx, queue.StreamNormal)
	lowLen := pipe.XLen(ctx, queue.StreamLow)
	deadLen := pipe.XLen(ctx, queue.StreamDead)
	highPending := pipe.XPending(ctx, queue.StreamHigh, queue.GroupName)
	normalPending := pipe.XPending(ctx, queue.StreamNormal, queue.GroupName)
	lowPending := pipe.XPending(ctx, queue.StreamLow, queue.GroupName)
	_, _ = pipe.Exec(ctx)

	payload := map[string]any{
		"queue_depth": map[string]any{
			"stream_length": map[string]any{
				"high":   highLen.Val(),
				"normal": normalLen.Val(),
				"low":    lowLen.Val(),
				"dead":   deadLen.Val(),
			},
			"pending": map[string]any{
				"high":   xPendingCount(highPending),
				"normal": xPendingCount(normalPending),
				"low":    xPendingCount(lowPending),
			},
		},
		"delivered": map[string]any{
			"sms":   h.metrics.DeliveredSMS.Load(),
			"email": h.metrics.DeliveredEmail.Load(),
			"push":  h.metrics.DeliveredPush.Load(),
		},
		"failed": map[string]any{
			"sms":   h.metrics.FailedSMS.Load(),
			"email": h.metrics.FailedEmail.Load(),
			"push":  h.metrics.FailedPush.Load(),
		},
		"avg_latency_ms": map[string]any{
			"sms":   avgLatencyMs(h.metrics.LatencySMSNs.Load(), h.metrics.LatencySMSCnt.Load()),
			"email": avgLatencyMs(h.metrics.LatencyEmailNs.Load(), h.metrics.LatencyEmailCnt.Load()),
			"push":  avgLatencyMs(h.metrics.LatencyPushNs.Load(), h.metrics.LatencyPushCnt.Load()),
		},
	}
	if h.circuitStates != nil {
		payload["circuit_breaker"] = map[string]string{
			"sms":   h.circuitStates.CircuitState("sms"),
			"email": h.circuitStates.CircuitState("email"),
			"push":  h.circuitStates.CircuitState("push"),
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

// xPendingCount returns the pending message count from an XPending command (e.g. after pipeline Exec).
func xPendingCount(cmd *redis.XPendingCmd) int64 {
	r, err := cmd.Result()
	if err != nil {
		return 0
	}
	return r.Count
}
