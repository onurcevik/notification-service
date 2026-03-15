package service

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/onurcevik/notification-service/internal/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gopkg.in/guregu/null.v4"
)

func TestScheduler_Tick_FetchDueScheduledReturnsError(t *testing.T) {
	repo := mocks.NewNotificationRepository(t)
	eventRepo := mocks.NewEventRepository(t)
	txr := mocks.NewTransactor(t)

	repo.On("FetchDueScheduled", mock.Anything).Return([]domain.Notification(nil), assert.AnError).Once()

	s := NewScheduler(repo, eventRepo, txr, time.Minute)
	err := s.Tick(context.Background())
	require.Error(t, err)
	repo.AssertExpectations(t)
}

func TestScheduler_Tick_NoDueNotifications(t *testing.T) {
	repo := mocks.NewNotificationRepository(t)
	eventRepo := mocks.NewEventRepository(t)
	txr := mocks.NewTransactor(t)

	repo.On("FetchDueScheduled", mock.Anything).Return([]domain.Notification{}, nil).Once()

	s := NewScheduler(repo, eventRepo, txr, time.Minute)
	err := s.Tick(context.Background())
	require.NoError(t, err)
	repo.AssertExpectations(t)
	txr.AssertNotCalled(t, "WithTransaction")
}

func TestScheduler_Tick_DueNotifications_EnqueuesEach(t *testing.T) {
	repo := mocks.NewNotificationRepository(t)
	eventRepo := mocks.NewEventRepository(t)
	txr := mocks.NewTransactor(t)

	n1 := domain.Notification{ID: "id-1", Status: domain.StatusScheduled}
	n2 := domain.Notification{ID: "id-2", Status: domain.StatusScheduled}
	repo.On("FetchDueScheduled", mock.Anything).Return([]domain.Notification{n1, n2}, nil).Once()

	txr.On("WithTransaction", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(pgx.Tx) error)
		_ = fn(nil)
	}).Twice()
	eventRepo.On("Insert", mock.Anything, nil, "id-1").Return(nil).Once()
	eventRepo.On("Insert", mock.Anything, nil, "id-2").Return(nil).Once()
	repo.On("UpdateStatus", mock.Anything, "id-1", domain.StatusPending, "").Return(nil).Once()
	repo.On("UpdateStatus", mock.Anything, "id-2", domain.StatusPending, "").Return(nil).Once()

	s := NewScheduler(repo, eventRepo, txr, time.Minute)
	err := s.Tick(context.Background())
	require.NoError(t, err)
	repo.AssertExpectations(t)
	eventRepo.AssertExpectations(t)
	txr.AssertExpectations(t)
}

func TestScheduler_Tick_EnqueueFailure_ContinuesToNext(t *testing.T) {
	repo := mocks.NewNotificationRepository(t)
	eventRepo := mocks.NewEventRepository(t)
	txr := mocks.NewTransactor(t)

	n1 := domain.Notification{ID: "id-1", Status: domain.StatusScheduled}
	n2 := domain.Notification{ID: "id-2", Status: domain.StatusScheduled}
	repo.On("FetchDueScheduled", mock.Anything).Return([]domain.Notification{n1, n2}, nil).Once()

	// First enqueue fails (WithTransaction returns error)
	txr.On("WithTransaction", mock.Anything, mock.Anything).Return(assert.AnError).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(pgx.Tx) error)
		_ = fn(nil)
	}).Once()
	eventRepo.On("Insert", mock.Anything, nil, "id-1").Return(nil).Once()
	repo.On("UpdateStatus", mock.Anything, "id-1", domain.StatusPending, "").Return(nil).Once()

	// Second enqueue succeeds
	txr.On("WithTransaction", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(pgx.Tx) error)
		_ = fn(nil)
	}).Once()
	eventRepo.On("Insert", mock.Anything, nil, "id-2").Return(nil).Once()
	repo.On("UpdateStatus", mock.Anything, "id-2", domain.StatusPending, "").Return(nil).Once()

	s := NewScheduler(repo, eventRepo, txr, time.Minute)
	err := s.Tick(context.Background())
	require.NoError(t, err)
	repo.AssertExpectations(t)
	eventRepo.AssertExpectations(t)
	txr.AssertExpectations(t)
}

func TestScheduler_Run_ExitsWhenContextCancelled(t *testing.T) {
	repo := mocks.NewNotificationRepository(t)
	eventRepo := mocks.NewEventRepository(t)
	txr := mocks.NewTransactor(t)

	repo.On("FetchDueScheduled", mock.Anything).Return([]domain.Notification{}, nil).Maybe()

	s := NewScheduler(repo, eventRepo, txr, 10*time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
		// Run exited
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancel")
	}
}

func TestScheduler_Enqueue_InsertFailure_ReturnsError(t *testing.T) {
	repo := mocks.NewNotificationRepository(t)
	eventRepo := mocks.NewEventRepository(t)
	txr := mocks.NewTransactor(t)

	n := domain.Notification{ID: "id-1", Status: domain.StatusScheduled, ScheduledAt: null.TimeFrom(time.Now())}
	repo.On("FetchDueScheduled", mock.Anything).Return([]domain.Notification{n}, nil).Once()
	txr.On("WithTransaction", mock.Anything, mock.Anything).Return(assert.AnError).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(pgx.Tx) error)
		_ = fn(nil)
	}).Once()
	eventRepo.On("Insert", mock.Anything, nil, "id-1").Return(assert.AnError).Once()

	s := NewScheduler(repo, eventRepo, txr, time.Minute)
	err := s.Tick(context.Background())
	require.NoError(t, err) // tick doesn't return enqueue errors, only FetchDueScheduled error
	eventRepo.AssertExpectations(t)
}
