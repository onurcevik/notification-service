package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"gopkg.in/guregu/null.v4"

	"gitlab.com/onurcevik/notification-service/internal/domain"
	"gitlab.com/onurcevik/notification-service/internal/mocks"
	"gitlab.com/onurcevik/notification-service/internal/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNotificationService_Create_NewNotification(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)
	mockTx := mocks.NewTransactor(t)

	n := &domain.Notification{
		Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Status: domain.StatusPending,
		Recipient: "+15551234567", Content: "Hello",
	}
	mockTx.On("WithTransaction", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(pgx.Tx) error)
		_ = fn(nil) // run callback so service code executes and hits Create/Insert mocks
	}).Once()
	mockRepo.On("Create", mock.Anything, nil, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		notif := args.Get(2).(*domain.Notification)
		notif.ID = "new-id"
		notif.CreatedAt = time.Now()
		notif.UpdatedAt = time.Now()
	}).Once()
	mockEvent.On("Insert", mock.Anything, nil, "new-id").Return(nil).Once()

	svc := NewNotificationService(mockRepo, mockEvent, mockTx)
	out, created, err := svc.Create(ctx, n)
	require.NoError(t, err)
	assert.True(t, created)
	require.NotNil(t, out)
	assert.Equal(t, "new-id", out.ID)
	mockRepo.AssertExpectations(t)
	mockEvent.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

func TestNotificationService_Create_IdempotentReturnExisting(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)
	mockTx := mocks.NewTransactor(t)

	existing := &domain.Notification{
		ID: "existing-id", Recipient: "+15551234567", Content: "Hello",
		Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Status: domain.StatusDelivered,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	existing.IdempotencyKey = null.StringFrom("key-1")

	n := &domain.Notification{
		IdempotencyKey: null.StringFrom("key-1"),
		Channel:        domain.ChannelSMS, Priority: domain.PriorityNormal,
		Recipient: "+15551234567", Content: "Hello",
	}
	mockTx.On("WithTransaction", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(pgx.Tx) error)
		_ = fn(nil) // run callback so service code executes (e.g. GetByIdempotencyKeyTx)
	}).Once()
	mockRepo.On("GetByIdempotencyKeyTx", mock.Anything, nil, "key-1").Return(existing, nil).Once()
	// Create and Insert must not be called

	svc := NewNotificationService(mockRepo, mockEvent, mockTx)
	out, created, err := svc.Create(ctx, n)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, existing, out)
	mockRepo.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

func TestNotificationService_Get(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)

	expected := &domain.Notification{ID: "id-1", Content: "Hi"}
	mockRepo.On("GetByID", mock.Anything, "id-1").Return(expected, nil).Once()

	svc := NewNotificationService(mockRepo, mockEvent, nil)
	out, err := svc.Get(ctx, "id-1")
	require.NoError(t, err)
	assert.Equal(t, expected, out)
	mockRepo.AssertExpectations(t)
}

func TestNotificationService_Cancel(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)

	mockRepo.On("Cancel", mock.Anything, "id-1").Return(nil).Once()

	svc := NewNotificationService(mockRepo, mockEvent, nil)
	err := svc.Cancel(ctx, "id-1")
	require.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestNotificationService_List(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)

	f := domain.Filter{Limit: 20}
	mockRepo.On("List", mock.Anything, mock.Anything).Return([]domain.Notification{}, "", nil).Once()

	svc := NewNotificationService(mockRepo, mockEvent, nil)
	list, cursor, err := svc.List(ctx, f)
	require.NoError(t, err)
	assert.Empty(t, list)
	assert.Empty(t, cursor)
	mockRepo.AssertExpectations(t)
}

func TestNotificationService_Create_TxReturnsError(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)
	mockTx := mocks.NewTransactor(t)

	txErr := errors.New("tx failed")
	mockTx.On("WithTransaction", mock.Anything, mock.Anything).Return(txErr).Once()

	n := &domain.Notification{
		Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		Recipient: "+15551234567", Content: "Hello",
	}
	svc := NewNotificationService(mockRepo, mockEvent, mockTx)
	out, created, err := svc.Create(ctx, n)
	require.Error(t, err)
	assert.True(t, errors.Is(err, txErr))
	assert.False(t, created)
	assert.Nil(t, out)
	mockTx.AssertExpectations(t)
}

func TestNotificationService_Create_ErrDuplicateReturnsExisting(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)
	mockTx := mocks.NewTransactor(t)

	existing := &domain.Notification{
		ID: "existing-id", Recipient: "+15551234567", Content: "Hello",
		Channel: domain.ChannelSMS, Priority: domain.PriorityNormal,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	existing.IdempotencyKey = null.StringFrom("key-1")

	n := &domain.Notification{
		IdempotencyKey: null.StringFrom("key-1"),
		Channel:        domain.ChannelSMS, Priority: domain.PriorityNormal,
		Recipient: "+15551234567", Content: "Hello",
	}
	mockTx.On("WithTransaction", mock.Anything, mock.Anything).Return(repository.ErrDuplicate).Once()
	mockRepo.On("GetByIdempotencyKey", mock.Anything, "key-1").Return(existing, nil).Once()

	svc := NewNotificationService(mockRepo, mockEvent, mockTx)
	out, created, err := svc.Create(ctx, n)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, existing, out)
	mockTx.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
}

func TestNotificationService_BatchCreate_Success(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)
	mockTx := mocks.NewTransactor(t)

	n1 := &domain.Notification{ID: "id-1", Channel: domain.ChannelSMS, Recipient: "+15551111111", Content: "Hi"}
	n2 := &domain.Notification{ID: "id-2", Channel: domain.ChannelEmail, Recipient: "a@b.co", Content: "Hello"}
	notifications := []*domain.Notification{n1, n2}

	mockTx.On("WithTransaction", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(pgx.Tx) error)
		_ = fn(nil) // run callback so BatchCreate loop runs Create/Insert for each item
	}).Once()
	mockRepo.On("Create", mock.Anything, nil, mock.Anything).Return(nil).Twice()
	mockEvent.On("Insert", mock.Anything, nil, "id-1").Return(nil).Once()
	mockEvent.On("Insert", mock.Anything, nil, "id-2").Return(nil).Once()

	svc := NewNotificationService(mockRepo, mockEvent, mockTx)
	out, err := svc.BatchCreate(ctx, notifications)
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, "id-1", out[0].ID)
	assert.Equal(t, "id-2", out[1].ID)
	assert.True(t, out[0].BatchID.Valid)
	assert.Equal(t, out[0].BatchID.String, out[1].BatchID.String)
	mockRepo.AssertExpectations(t)
	mockEvent.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

func TestNotificationService_BatchCreate_RepoCreateError(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)
	mockTx := mocks.NewTransactor(t)

	createErr := errors.New("create failed")
	n1 := &domain.Notification{ID: "id-1", Channel: domain.ChannelSMS, Recipient: "+15551111111", Content: "Hi"}

	// Simulate transaction failing with repo.Create error (no need to run callback)
	mockTx.On("WithTransaction", mock.Anything, mock.Anything).Return(createErr).Once()

	svc := NewNotificationService(mockRepo, mockEvent, mockTx)
	out, err := svc.BatchCreate(ctx, []*domain.Notification{n1})
	require.Error(t, err)
	assert.Nil(t, out)
	assert.True(t, errors.Is(err, createErr))
	mockTx.AssertExpectations(t)
}

func TestNotificationService_BatchCreate_InsertError(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)
	mockTx := mocks.NewTransactor(t)

	insertErr := errors.New("insert failed")
	n1 := &domain.Notification{ID: "id-1", Channel: domain.ChannelSMS, Recipient: "+15551111111", Content: "Hi"}

	// Simulate transaction failing with eventRepo.Insert error (no need to run callback)
	mockTx.On("WithTransaction", mock.Anything, mock.Anything).Return(insertErr).Once()

	svc := NewNotificationService(mockRepo, mockEvent, mockTx)
	out, err := svc.BatchCreate(ctx, []*domain.Notification{n1})
	require.Error(t, err)
	assert.Nil(t, out)
	assert.True(t, errors.Is(err, insertErr))
	mockTx.AssertExpectations(t)
}

func TestNotificationService_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)

	mockRepo.On("GetByID", mock.Anything, "missing").Return(nil, repository.ErrNotFound).Once()

	svc := NewNotificationService(mockRepo, mockEvent, nil)
	out, err := svc.Get(ctx, "missing")
	require.Error(t, err)
	assert.Nil(t, out)
	assert.True(t, errors.Is(err, repository.ErrNotFound))
	mockRepo.AssertExpectations(t)
}

func TestNotificationService_Get_OtherError(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)

	dbErr := errors.New("db error")
	mockRepo.On("GetByID", mock.Anything, "id-1").Return(nil, dbErr).Once()

	svc := NewNotificationService(mockRepo, mockEvent, nil)
	out, err := svc.Get(ctx, "id-1")
	require.Error(t, err)
	assert.Nil(t, out)
	assert.True(t, errors.Is(err, dbErr))
	mockRepo.AssertExpectations(t)
}

func TestNotificationService_Cancel_NotFound(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)

	mockRepo.On("Cancel", mock.Anything, "missing").Return(repository.ErrNotFound).Once()

	svc := NewNotificationService(mockRepo, mockEvent, nil)
	err := svc.Cancel(ctx, "missing")
	require.Error(t, err)
	assert.True(t, errors.Is(err, repository.ErrNotFound))
	mockRepo.AssertExpectations(t)
}

func TestNotificationService_Cancel_CannotCancel(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)

	mockRepo.On("Cancel", mock.Anything, "id-1").Return(repository.ErrCannotCancel).Once()

	svc := NewNotificationService(mockRepo, mockEvent, nil)
	err := svc.Cancel(ctx, "id-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, repository.ErrCannotCancel))
	mockRepo.AssertExpectations(t)
}

func TestNotificationService_List_RepoError(t *testing.T) {
	ctx := context.Background()
	mockRepo := mocks.NewNotificationRepository(t)
	mockEvent := mocks.NewEventRepository(t)

	listErr := errors.New("list failed")
	mockRepo.On("List", mock.Anything, mock.Anything).Return(nil, "", listErr).Once()

	svc := NewNotificationService(mockRepo, mockEvent, nil)
	list, cursor, err := svc.List(ctx, domain.Filter{Limit: 20})
	require.Error(t, err)
	assert.Nil(t, list)
	assert.Empty(t, cursor)
	assert.True(t, errors.Is(err, listErr))
	mockRepo.AssertExpectations(t)
}
