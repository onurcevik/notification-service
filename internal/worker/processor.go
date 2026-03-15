package worker

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/onurcevik/notification-service/internal/queue"
)

var tracer = otel.Tracer("notification-service/worker")

const incrementAttemptRetries = 3

var retryDelays = []time.Duration{
	0,
	1 * time.Second,
	5 * time.Second,
	30 * time.Second,
}

// Queue abstracts message consumption, acknowledgement, and dead-lettering.
type Queue interface {
	Consume(ctx context.Context) (*queue.Message, error)
	Ack(ctx context.Context, stream string, msgID string) error
	DeadLetter(ctx context.Context, n *domain.Notification, reason error) error
}

// NotificationRepository is the subset of repository operations the processor needs.
type NotificationRepository interface {
	GetByID(ctx context.Context, id string) (*domain.Notification, error)
	UpdateStatus(ctx context.Context, id string, status domain.Status, providerMsgID string) error
	IncrementAttempt(ctx context.Context, id string) error
}

// Provider delivers notifications to external channels.
type Provider interface {
	Send(ctx context.Context, n *domain.Notification) (*domain.DeliveryResult, error)
}

// Limiter gates delivery throughput per channel.
type Limiter interface {
	Wait(ctx context.Context, channel string) error
}

// MetricsRecorder tracks delivery success/failure counts and latency.
type MetricsRecorder interface {
	IncDelivered(channel string)
	IncFailed(channel string)
	RecordLatency(channel string, d time.Duration)
}

// StatusBroadcaster notifies listeners (e.g. WebSocket) when a notification's status changes.
type StatusBroadcaster interface {
	Broadcast(notificationID, status string)
}

// ProcessorConfig holds only the configuration values the processor needs.
type ProcessorConfig struct {
	MaxRetryAttempts      int
	WorkerCountPerChannel int
}

type processor struct {
	queue     Queue
	repo      NotificationRepository
	provider  Provider
	limiter   Limiter
	metrics   MetricsRecorder
	breaker   *gobreaker.CircuitBreaker
	cfg       ProcessorConfig
	broadcast StatusBroadcaster
}

func newProcessor(
	q Queue,
	repo NotificationRepository,
	p Provider,
	limiter Limiter,
	metrics MetricsRecorder,
	breaker *gobreaker.CircuitBreaker,
	cfg ProcessorConfig,
	broadcast StatusBroadcaster,
) *processor {
	return &processor{
		queue:     q,
		repo:      repo,
		provider:  p,
		limiter:   limiter,
		metrics:   metrics,
		breaker:   breaker,
		cfg:       cfg,
		broadcast: broadcast,
	}
}

func (p *processor) incrementAttemptWithRetry(ctx context.Context, notificationID string) error {
	var lastErr error
	for i := 0; i < incrementAttemptRetries; i++ {
		lastErr = p.repo.IncrementAttempt(ctx, notificationID)
		if lastErr == nil {
			return nil
		}
		if i < incrementAttemptRetries-1 { // back off
			time.Sleep(time.Duration(50*(i+1)) * time.Millisecond) // 50ms, 100ms, 150ms
		}
	}
	return lastErr // return the last error
}

func (p *processor) run(ctx context.Context, consumerID string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := p.queue.Consume(ctx)
			if err != nil {
				log.Ctx(ctx).Error().Err(err).Str("consumer", consumerID).Msg("failed to consume")
				time.Sleep(time.Second)
				continue
			}
			if msg == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			p.process(ctx, msg)
		}
	}
}

func (p *processor) process(ctx context.Context, msg *queue.Message) {
	ctx, span := tracer.Start(ctx, "worker.process")
	defer span.End()

	n := msg.Notification
	span.SetAttributes(
		attribute.String("notification.id", n.ID),
		attribute.String("notification.channel", string(n.Channel)),
		attribute.String("notification.priority", string(n.Priority)),
	)
	start := time.Now()

	if err := p.repo.UpdateStatus(ctx, n.ID, domain.StatusProcessing, ""); err != nil {
		log.Ctx(ctx).Error().Err(err).Str("notification_id", n.ID).Msg("failed to set processing status, will retry on next consume")
		return
	}
	if p.broadcast != nil {
		p.broadcast.Broadcast(n.ID, string(domain.StatusProcessing))
	}

	var lastErr error
	for attempt := 0; attempt < p.cfg.MaxRetryAttempts; attempt++ {
		if attempt > 0 {
			idx := attempt
			if idx >= len(retryDelays) {
				idx = len(retryDelays) - 1
			}
			delay := retryDelays[idx]
			jitter := time.Duration(rand.Int63n(int64(delay)/4 + 1))
			select {
			case <-time.After(delay + jitter):
			case <-ctx.Done():
				return
			}
		}

		if err := p.limiter.Wait(ctx, string(n.Channel)); err != nil {
			lastErr = err
			continue
		}

		cur, err := p.repo.GetByID(ctx, n.ID)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("notification_id", n.ID).Msg("failed to re-fetch notification")
			lastErr = err
			continue
		}
		if cur.Status == domain.StatusCancelled {
			log.Ctx(ctx).Info().Str("notification_id", n.ID).Msg("notification cancelled, skipping delivery")
			if p.broadcast != nil {
				p.broadcast.Broadcast(n.ID, string(domain.StatusCancelled))
			}
			_ = p.queue.Ack(ctx, msg.Stream, msg.ID)
			return
		}

		result, err := p.breaker.Execute(func() (any, error) {
			return p.provider.Send(ctx, n)
		})

		if incErr := p.incrementAttemptWithRetry(ctx, n.ID); incErr != nil {
			log.Ctx(ctx).Error().Err(incErr).Str("notification_id", n.ID).Msg("failed to increment attempt after retries")
		}

		if err != nil {
			lastErr = err
			log.Ctx(ctx).Warn().
				Err(err).
				Str("notification_id", n.ID).
				Str("channel", string(n.Channel)).
				Int("attempt", attempt+1).
				Msg("delivery attempt failed")
			continue
		}

		dr, ok := result.(*domain.DeliveryResult)
		if !ok || dr == nil {
			lastErr = fmt.Errorf("unexpected provider result type")
			log.Ctx(ctx).Warn().Str("notification_id", n.ID).Msg("provider returned unexpected result type")
			continue
		}
		if err := p.repo.UpdateStatus(ctx, n.ID, domain.StatusDelivered, dr.ProviderMessageID); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("notification_id", n.ID).Msg("failed to set delivered status")
		}
		if p.broadcast != nil {
			p.broadcast.Broadcast(n.ID, string(domain.StatusDelivered))
		}
		latency := time.Since(start)
		p.metrics.IncDelivered(string(n.Channel))
		p.metrics.RecordLatency(string(n.Channel), latency)
		_ = p.queue.Ack(ctx, msg.Stream, msg.ID)
		span.SetAttributes(attribute.Int64("delivery.latency_ms", latency.Milliseconds()))

		log.Ctx(ctx).Info().
			Str("notification_id", n.ID).
			Str("channel", string(n.Channel)).
			Str("provider_msg_id", dr.ProviderMessageID).
			Dur("latency", latency).
			Msg("notification delivered")
		return
	}

	if err := p.repo.UpdateStatus(ctx, n.ID, domain.StatusFailed, ""); err != nil {
		log.Ctx(ctx).Error().Err(err).Str("notification_id", n.ID).Msg("failed to set failed status")
	}
	if p.broadcast != nil {
		p.broadcast.Broadcast(n.ID, string(domain.StatusFailed))
	}
	p.metrics.IncFailed(string(n.Channel))
	_ = p.queue.DeadLetter(ctx, n, lastErr)
	_ = p.queue.Ack(ctx, msg.Stream, msg.ID)

	log.Ctx(ctx).Error().
		Err(lastErr).
		Str("notification_id", n.ID).
		Str("channel", string(n.Channel)).
		Msg("notification failed after all attempts")
}
