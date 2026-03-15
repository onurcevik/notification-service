package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/onurcevik/notification-service/internal/queue"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

type fakeQueue struct {
	acked        []string
	deadLettered []*domain.Notification
}

func (q *fakeQueue) Consume(_ context.Context) (*queue.Message, error) { return nil, nil }
func (q *fakeQueue) Ack(_ context.Context, _, msgID string) error {
	q.acked = append(q.acked, msgID)
	return nil
}
func (q *fakeQueue) DeadLetter(_ context.Context, n *domain.Notification, _ error) error {
	q.deadLettered = append(q.deadLettered, n)
	return nil
}

type updateStatusCall struct {
	id     string
	status domain.Status
}

type fakeRepo struct {
	notifications    map[string]*domain.Notification
	updateStatusCalls []updateStatusCall
	incrementCalls    int
	incrementErr      error
	getErr            error
}

func (r *fakeRepo) GetByID(_ context.Context, id string) (*domain.Notification, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	n, ok := r.notifications[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return n, nil
}

func (r *fakeRepo) UpdateStatus(_ context.Context, id string, status domain.Status, _ string) error {
	r.updateStatusCalls = append(r.updateStatusCalls, updateStatusCall{id: id, status: status})
	return nil
}

func (r *fakeRepo) IncrementAttempt(_ context.Context, _ string) error {
	r.incrementCalls++
	return r.incrementErr
}

type fakeProvider struct {
	result    *domain.DeliveryResult
	err       error
	calls     int
	failFirst int
}

func (p *fakeProvider) Send(_ context.Context, _ *domain.Notification) (*domain.DeliveryResult, error) {
	p.calls++
	if p.calls <= p.failFirst {
		return nil, errors.New("provider error")
	}
	return p.result, p.err
}

type fakeLimiter struct{ err error }

func (l *fakeLimiter) Wait(_ context.Context, _ string) error { return l.err }

type fakeMetrics struct {
	delivered int
	failed    int
}

func (m *fakeMetrics) IncDelivered(_ string) { m.delivered++ }
func (m *fakeMetrics) IncFailed(_ string)    { m.failed++ }
func (m *fakeMetrics) RecordLatency(_ string, _ time.Duration) {}

type fakeBroadcast struct {
	events []string
}

func (b *fakeBroadcast) Broadcast(_, status string) {
	b.events = append(b.events, status)
}

// permissiveBreaker returns a circuit breaker that never trips during tests.
func permissiveBreaker() *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "test",
		MaxRequests: 1000,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 1000
		},
	})
}

func testMsg(id string) *queue.Message {
	return &queue.Message{
		ID:     "msg-" + id,
		Stream: queue.StreamHigh,
		Notification: &domain.Notification{
			ID:       id,
			Channel:  domain.ChannelSMS,
			Priority: domain.PriorityHigh,
			Content:  "test content",
			Status:   domain.StatusPending,
		},
	}
}

// zeroDelays replaces retryDelays with all-zero durations for test speed and
// restores the originals via t.Cleanup.
func zeroDelays(t *testing.T) {
	t.Helper()
	orig := retryDelays
	retryDelays = make([]time.Duration, len(retryDelays))
	t.Cleanup(func() { retryDelays = orig })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestProcessor_HappyPath: provider succeeds on first attempt.
// Expects: delivered metric, ack, broadcast processing+delivered, increment called once.
func TestProcessor_HappyPath(t *testing.T) {
	msg := testMsg("n-1")
	repo := &fakeRepo{notifications: map[string]*domain.Notification{"n-1": msg.Notification}}
	q := &fakeQueue{}
	provider := &fakeProvider{result: &domain.DeliveryResult{ProviderMessageID: "ext-1", Status: "accepted"}}
	metrics := &fakeMetrics{}
	broadcast := &fakeBroadcast{}

	p := newProcessor(q, repo, provider, &fakeLimiter{}, metrics, permissiveBreaker(),
		ProcessorConfig{MaxRetryAttempts: 1}, broadcast)
	p.process(context.Background(), msg)

	assert.Equal(t, 1, metrics.delivered)
	assert.Equal(t, 0, metrics.failed)
	assert.Len(t, q.acked, 1, "message must be acked")
	assert.Equal(t, "msg-n-1", q.acked[0])
	assert.Empty(t, q.deadLettered)
	assert.Contains(t, broadcast.events, string(domain.StatusProcessing))
	assert.Contains(t, broadcast.events, string(domain.StatusDelivered))
	assert.Equal(t, 1, repo.incrementCalls)
	require.Len(t, repo.updateStatusCalls, 2)
	assert.Equal(t, domain.StatusProcessing, repo.updateStatusCalls[0].status)
	assert.Equal(t, domain.StatusDelivered, repo.updateStatusCalls[1].status)
}

// TestProcessor_AllRetriesExhausted: provider always fails.
// Expects: failed metric, dead-letter, ack, broadcast failed, increment per attempt.
func TestProcessor_AllRetriesExhausted(t *testing.T) {
	zeroDelays(t)

	msg := testMsg("n-1")
	repo := &fakeRepo{notifications: map[string]*domain.Notification{"n-1": msg.Notification}}
	q := &fakeQueue{}
	provider := &fakeProvider{failFirst: 999}
	metrics := &fakeMetrics{}
	broadcast := &fakeBroadcast{}

	const maxAttempts = 3
	p := newProcessor(q, repo, provider, &fakeLimiter{}, metrics, permissiveBreaker(),
		ProcessorConfig{MaxRetryAttempts: maxAttempts}, broadcast)
	p.process(context.Background(), msg)

	assert.Equal(t, 0, metrics.delivered)
	assert.Equal(t, 1, metrics.failed)
	assert.Len(t, q.deadLettered, 1)
	assert.Len(t, q.acked, 1)
	assert.Contains(t, broadcast.events, string(domain.StatusFailed))
	assert.Equal(t, maxAttempts, repo.incrementCalls)
	require.GreaterOrEqual(t, len(repo.updateStatusCalls), 2)
	assert.Equal(t, domain.StatusProcessing, repo.updateStatusCalls[0].status)
	assert.Equal(t, domain.StatusFailed, repo.updateStatusCalls[len(repo.updateStatusCalls)-1].status)
}

// TestProcessor_ProviderFailsThenSucceeds: provider fails N times then succeeds.
// Expects: delivery on the successful attempt, correct call count.
func TestProcessor_ProviderFailsThenSucceeds(t *testing.T) {
	zeroDelays(t)

	msg := testMsg("n-1")
	repo := &fakeRepo{notifications: map[string]*domain.Notification{"n-1": msg.Notification}}
	q := &fakeQueue{}
	provider := &fakeProvider{
		result:    &domain.DeliveryResult{ProviderMessageID: "ext-2", Status: "accepted"},
		failFirst: 2, // fail twice, succeed on third attempt
	}
	metrics := &fakeMetrics{}

	p := newProcessor(q, repo, provider, &fakeLimiter{}, metrics, permissiveBreaker(),
		ProcessorConfig{MaxRetryAttempts: 5}, nil)
	p.process(context.Background(), msg)

	assert.Equal(t, 1, metrics.delivered)
	assert.Equal(t, 0, metrics.failed)
	assert.Equal(t, 3, provider.calls)
	assert.Len(t, q.acked, 1)
	assert.Empty(t, q.deadLettered)
}

// TestProcessor_CancelledNotification: notification is cancelled before delivery.
// Expects: provider never called, message acked, broadcast cancelled.
func TestProcessor_CancelledNotification(t *testing.T) {
	msg := testMsg("n-1")
	cancelled := &domain.Notification{
		ID: "n-1", Channel: domain.ChannelSMS, Status: domain.StatusCancelled,
	}
	repo := &fakeRepo{notifications: map[string]*domain.Notification{"n-1": cancelled}}
	q := &fakeQueue{}
	provider := &fakeProvider{}
	metrics := &fakeMetrics{}
	broadcast := &fakeBroadcast{}

	p := newProcessor(q, repo, provider, &fakeLimiter{}, metrics, permissiveBreaker(),
		ProcessorConfig{MaxRetryAttempts: 1}, broadcast)
	p.process(context.Background(), msg)

	assert.Equal(t, 0, provider.calls, "provider must not be called for a cancelled notification")
	assert.Equal(t, 0, metrics.delivered)
	assert.Len(t, q.acked, 1, "cancelled message must still be acked to remove it from the PEL")
	assert.Contains(t, broadcast.events, string(domain.StatusCancelled))
}

// TestProcessor_LimiterError: rate limiter always returns an error.
// Expects: all retries fail on limiter, notification dead-lettered, failed metric recorded.
func TestProcessor_LimiterError(t *testing.T) {
	zeroDelays(t)

	msg := testMsg("n-1")
	repo := &fakeRepo{notifications: map[string]*domain.Notification{"n-1": msg.Notification}}
	q := &fakeQueue{}
	provider := &fakeProvider{}
	metrics := &fakeMetrics{}

	p := newProcessor(q, repo, provider, &fakeLimiter{err: errors.New("rate limited")},
		metrics, permissiveBreaker(), ProcessorConfig{MaxRetryAttempts: 2}, nil)
	p.process(context.Background(), msg)

	assert.Equal(t, 0, provider.calls, "provider must never be reached when limiter always fails")
	assert.Equal(t, 1, metrics.failed)
	assert.Len(t, q.deadLettered, 1)
	assert.Len(t, q.acked, 1)
}

// TestProcessor_UpdateStatusProcessingFails: UpdateStatus(processing) fails on first call.
// Expects: process returns immediately without calling provider or acking.
func TestProcessor_UpdateStatusProcessingFails(t *testing.T) {
	msg := testMsg("n-1")
	repo := &failFirstUpdateRepo{
		inner: &fakeRepo{notifications: map[string]*domain.Notification{"n-1": msg.Notification}},
	}
	q := &fakeQueue{}
	provider := &fakeProvider{result: &domain.DeliveryResult{Status: "accepted"}}
	metrics := &fakeMetrics{}

	p := newProcessor(q, repo, provider, &fakeLimiter{}, metrics, permissiveBreaker(),
		ProcessorConfig{MaxRetryAttempts: 1}, nil)
	p.process(context.Background(), msg)

	assert.Equal(t, 0, provider.calls, "provider must not be called when processing-status update fails")
	assert.Equal(t, 0, metrics.delivered)
	assert.Len(t, q.acked, 0, "message must NOT be acked — it will be re-consumed on next poll")
}

// failFirstUpdateRepo fails only the first UpdateStatus call (the StatusProcessing write).
type failFirstUpdateRepo struct {
	inner      *fakeRepo
	callsCount int
}

func (r *failFirstUpdateRepo) GetByID(ctx context.Context, id string) (*domain.Notification, error) {
	return r.inner.GetByID(ctx, id)
}
func (r *failFirstUpdateRepo) UpdateStatus(ctx context.Context, id string, status domain.Status, pID string) error {
	r.callsCount++
	if r.callsCount == 1 {
		return errors.New("simulated DB write failure")
	}
	return r.inner.UpdateStatus(ctx, id, status, pID)
}
func (r *failFirstUpdateRepo) IncrementAttempt(ctx context.Context, id string) error {
	return r.inner.IncrementAttempt(ctx, id)
}

// TestProcessor_GetByIDFails: GetByID fails on first attempt but succeeds on second.
// Expects: delivery succeeds on the second attempt.
func TestProcessor_GetByIDFails(t *testing.T) {
	zeroDelays(t)

	msg := testMsg("n-1")
	inner := &fakeRepo{notifications: map[string]*domain.Notification{"n-1": msg.Notification}}
	repo := &failFirstGetRepo{inner: inner}
	q := &fakeQueue{}
	provider := &fakeProvider{result: &domain.DeliveryResult{Status: "accepted"}}
	metrics := &fakeMetrics{}

	p := newProcessor(q, repo, provider, &fakeLimiter{}, metrics, permissiveBreaker(),
		ProcessorConfig{MaxRetryAttempts: 3}, nil)
	p.process(context.Background(), msg)

	assert.Equal(t, 1, provider.calls)
	assert.Equal(t, 1, metrics.delivered)
	assert.Len(t, q.acked, 1)
}

type failFirstGetRepo struct {
	inner    *fakeRepo
	getCalls int
}

func (r *failFirstGetRepo) GetByID(ctx context.Context, id string) (*domain.Notification, error) {
	r.getCalls++
	if r.getCalls == 1 {
		return nil, errors.New("simulated DB timeout")
	}
	return r.inner.GetByID(ctx, id)
}
func (r *failFirstGetRepo) UpdateStatus(ctx context.Context, id string, status domain.Status, pID string) error {
	return r.inner.UpdateStatus(ctx, id, status, pID)
}
func (r *failFirstGetRepo) IncrementAttempt(ctx context.Context, id string) error {
	return r.inner.IncrementAttempt(ctx, id)
}

// TestProcessor_IncrementAttemptRetries: IncrementAttempt always fails.
// Expects: delivery still succeeds; IncrementAttempt is retried incrementAttemptRetries times.
func TestProcessor_IncrementAttemptRetries(t *testing.T) {
	// Allow real short sleeps (50ms + 100ms) from incrementAttemptWithRetry.
	msg := testMsg("n-1")
	repo := &fakeRepo{
		notifications: map[string]*domain.Notification{"n-1": msg.Notification},
		incrementErr:  errors.New("db flaky"),
	}
	q := &fakeQueue{}
	provider := &fakeProvider{result: &domain.DeliveryResult{Status: "accepted"}}
	metrics := &fakeMetrics{}

	p := newProcessor(q, repo, provider, &fakeLimiter{}, metrics, permissiveBreaker(),
		ProcessorConfig{MaxRetryAttempts: 1}, nil)
	p.process(context.Background(), msg)

	// delivery must still succeed even when IncrementAttempt always errors
	assert.Equal(t, 1, metrics.delivered)
	// incrementAttemptRetries (3) retries are exhausted for the single delivery attempt
	assert.Equal(t, incrementAttemptRetries, repo.incrementCalls,
		"IncrementAttempt must be retried the configured number of times before giving up")
}

// TestProcessor_ContextCancelledDuringRetryDelay: context is cancelled while the
// processor is waiting in the retry back-off delay.
// Expects: process returns promptly without dead-lettering or acking.
func TestProcessor_ContextCancelledDuringRetryDelay(t *testing.T) {
	// First attempt has no delay; second attempt has 500ms delay.
	orig := retryDelays
	retryDelays = []time.Duration{0, 500 * time.Millisecond, 500 * time.Millisecond}
	t.Cleanup(func() { retryDelays = orig })

	msg := testMsg("n-1")
	repo := &fakeRepo{notifications: map[string]*domain.Notification{"n-1": msg.Notification}}
	q := &fakeQueue{}
	provider := &fakeProvider{failFirst: 999} // always fails
	metrics := &fakeMetrics{}

	ctx, cancel := context.WithCancel(context.Background())
	p := newProcessor(q, repo, provider, &fakeLimiter{}, metrics, permissiveBreaker(),
		ProcessorConfig{MaxRetryAttempts: 5}, nil)

	done := make(chan struct{})
	go func() {
		p.process(ctx, msg)
		close(done)
	}()

	// Cancel after the first attempt completes and the second is sleeping.
	time.Sleep(60 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("process did not return after context cancellation")
	}

	assert.Equal(t, 0, metrics.failed, "context cancellation must not trigger a failed-metric or dead-letter")
	assert.Empty(t, q.deadLettered)
	assert.Empty(t, q.acked)
}
