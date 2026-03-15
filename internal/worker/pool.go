package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/sony/gobreaker"

	"github.com/onurcevik/notification-service/internal/domain"
)

const (
	DefaultConsecutiveFailures   = 5
	DefaultTimeoutSec            = 30
	DefaultHalfOpenMaxRequests   = 1
	DefaultWorkerCountPerChannel = 10
)

var channelNames = []string{"sms", "email", "push"}

// CircuitBreakerConfig holds settings for the per-channel circuit breakers.
type CircuitBreakerConfig struct {
	ConsecutiveFailures int
	TimeoutSec          int
	HalfOpenMaxRequests uint32
}

// Pool manages a group of notification processors.
type Pool struct {
	queue     Queue
	repo      NotificationRepository
	provider  Provider
	limiter   Limiter
	metrics   MetricsRecorder
	breakers  map[string]*gobreaker.CircuitBreaker
	cfg       ProcessorConfig
	broadcast StatusBroadcaster
}

// NewPool creates a new Pool of workers. broadcast may be nil to disable status broadcasting via WebSocket.
func NewPool(
	q Queue,
	repo NotificationRepository,
	p Provider,
	limiter Limiter,
	metrics MetricsRecorder,
	cfg ProcessorConfig,
	cbCfg CircuitBreakerConfig,
	broadcast StatusBroadcaster,
) *Pool {
	timeout := time.Duration(cbCfg.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = DefaultTimeoutSec * time.Second
	}
	consecutiveFailures := cbCfg.ConsecutiveFailures
	if consecutiveFailures <= 0 {
		consecutiveFailures = DefaultConsecutiveFailures
	}
	maxRequests := cbCfg.HalfOpenMaxRequests
	if maxRequests == 0 {
		maxRequests = DefaultHalfOpenMaxRequests
	}
	breakers := make(map[string]*gobreaker.CircuitBreaker)
	for _, name := range channelNames {
		breakers[name] = gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        name,
			MaxRequests: maxRequests,
			Interval:    0,
			Timeout:     timeout,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= uint32(consecutiveFailures)
			},
		})
	}
	return &Pool{
		queue:     q,
		repo:      repo,
		provider:  p,
		limiter:   limiter,
		metrics:   metrics,
		breakers:  breakers,
		cfg:       cfg,
		broadcast: broadcast,
	}
}

// Run starts the worker pool and blocks until context cancellation.
func (p *Pool) Run(ctx context.Context) {
	n := p.cfg.WorkerCountPerChannel
	if n <= 0 {
		n = DefaultWorkerCountPerChannel
	}
	var wg sync.WaitGroup
	for _, name := range channelNames {
		ch := domain.Channel(name)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go p.runWorker(ctx, &wg, ch, i)
		}
	}
	wg.Wait()
	log.Info().Msg("worker pool stopped")
}

func (p *Pool) runWorker(ctx context.Context, wg *sync.WaitGroup, ch domain.Channel, id int) {
	defer wg.Done()
	proc := newProcessor(
		p.queue,
		p.repo,
		p.provider,
		p.limiter,
		p.metrics,
		p.breakers[string(ch)],
		p.cfg,
		p.broadcast,
	)
	proc.run(ctx, fmt.Sprintf("%s-%d", ch, id))
}

// Breakers returns a copy of the circuit breakers per channel for metrics/health exposure.
// Callers must not modify the returned map.
func (p *Pool) Breakers() map[string]*gobreaker.CircuitBreaker {
	out := make(map[string]*gobreaker.CircuitBreaker, len(p.breakers))
	for k, v := range p.breakers {
		out[k] = v
	}
	return out
}

// CircuitState returns the current circuit breaker state for a channel.
func (p *Pool) CircuitState(ch string) string {
	if cb, ok := p.breakers[ch]; ok {
		return cb.State().String()
	}
	return "unknown"
}
