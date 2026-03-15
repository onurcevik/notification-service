package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/onurcevik/notification-service/internal/domain"
	"github.com/onurcevik/notification-service/internal/mocks"
	"github.com/onurcevik/notification-service/internal/repository"
	"github.com/onurcevik/notification-service/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gopkg.in/guregu/null.v4"
)

// stubNotificationService is a test double that returns configured values for each method.
type stubNotificationService struct {
	getResp    *domain.Notification
	getErr     error
	listResp   []domain.Notification
	listCursor string
	listErr    error
	cancelErr  error
	createResp *domain.Notification
	createNew  bool
	createErr  error
	batchResp  []*domain.Notification
	batchErr   error
}

func (s *stubNotificationService) Create(ctx context.Context, n *domain.Notification) (*domain.Notification, bool, error) {
	return s.createResp, s.createNew, s.createErr
}
func (s *stubNotificationService) BatchCreate(ctx context.Context, notifications []*domain.Notification) ([]*domain.Notification, error) {
	return s.batchResp, s.batchErr
}
func (s *stubNotificationService) Get(ctx context.Context, id string) (*domain.Notification, error) {
	return s.getResp, s.getErr
}
func (s *stubNotificationService) List(ctx context.Context, f domain.Filter) ([]domain.Notification, string, error) {
	return s.listResp, s.listCursor, s.listErr
}
func (s *stubNotificationService) Cancel(ctx context.Context, id string) error {
	return s.cancelErr
}

// requestWithChiParam returns r with chi route context so URLParam(r, "id") returns id.
func requestWithChiParam(r *http.Request, id string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", id)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func TestNotificationHandler_Create(t *testing.T) {
	tests := []struct {
		name       string
		req        CreateNotificationRequest
		setup      func(mNotif *mocks.NotificationRepository, mEvent *mocks.EventRepository, mTx *mocks.Transactor)
		wantStatus int
	}{
		{
			name: "valid request",
			req: CreateNotificationRequest{
				Recipient: "+905551234567",
				Channel:   "sms",
				Content:   "Hello",
				Priority:  "high",
			},
			setup: func(mNotif *mocks.NotificationRepository, mEvent *mocks.EventRepository, mTx *mocks.Transactor) {
				mNotif.On("GetByIdempotencyKeyTx", mock.Anything, nil, mock.Anything).Return(nil, repository.ErrNotFound).Maybe()
				mTx.On("WithTransaction", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
					fn := args.Get(1).(func(pgx.Tx) error)
					_ = fn(nil)
				}).Once()
				mNotif.On("Create", mock.Anything, nil, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
					n := args.Get(2).(*domain.Notification)
					n.ID = "test-id"
					n.CreatedAt = time.Now()
					n.UpdatedAt = time.Now()
				}).Once()
				mEvent.On("Insert", mock.Anything, nil, mock.Anything).Return(nil).Once()
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "valid request with template",
			req: CreateNotificationRequest{
				Recipient:   "+905551234567",
				Channel:     "sms",
				Template:    "Hello {{.name}}, code: {{.code}}",
				TemplateVars: map[string]string{"name": "Alice", "code": "123"},
				Priority:    "normal",
			},
			setup: func(mNotif *mocks.NotificationRepository, mEvent *mocks.EventRepository, mTx *mocks.Transactor) {
				mTx.On("WithTransaction", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
					fn := args.Get(1).(func(pgx.Tx) error)
					_ = fn(nil)
				}).Once()
				mNotif.On("Create", mock.Anything, nil, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
					n := args.Get(2).(*domain.Notification)
					n.ID = "test-id"
					n.CreatedAt = time.Now()
					n.UpdatedAt = time.Now()
					if n.Content != "Hello Alice, code: 123" {
						panic("expected content to be rendered from template")
					}
				}).Once()
				mEvent.On("Insert", mock.Anything, nil, mock.Anything).Return(nil).Once()
			},
			wantStatus: http.StatusCreated,
		},
		{
			name: "invalid template syntax",
			req: CreateNotificationRequest{
				Recipient:    "+905551234567",
				Channel:      "sms",
				Template:     "Hello {{.name",
				TemplateVars: map[string]string{"name": "Alice"},
			},
			setup:      func(mNotif *mocks.NotificationRepository, mEvent *mocks.EventRepository, mTx *mocks.Transactor) {},
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name: "missing recipient",
			req: CreateNotificationRequest{
				Channel: "sms",
				Content: "Hello",
			},
			setup:      func(mNotif *mocks.NotificationRepository, mEvent *mocks.EventRepository, mTx *mocks.Transactor) {},
			wantStatus: http.StatusUnprocessableEntity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNotifRepo := mocks.NewNotificationRepository(t)
			mockEventRepo := mocks.NewEventRepository(t)
			mockTx := mocks.NewTransactor(t)

			tt.setup(mockNotifRepo, mockEventRepo, mockTx)

			svc := service.NewNotificationService(mockNotifRepo, mockEventRepo, mockTx)
			h := NewNotificationHandler(svc)

			body, err := json.Marshal(tt.req)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(body))
			rr := httptest.NewRecorder()

			h.Create(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, tt.wantStatus)
			}
		})
	}
}

func TestNotificationHandler_CreateBatch(t *testing.T) {
	t.Run("over batch limit", func(t *testing.T) {
		mockNotifRepo := mocks.NewNotificationRepository(t)
		mockEventRepo := mocks.NewEventRepository(t)
		mockTx := mocks.NewTransactor(t)

		svc := service.NewNotificationService(mockNotifRepo, mockEventRepo, mockTx)
		h := NewNotificationHandler(svc)

		req := BatchCreateRequest{
			Notifications: make([]CreateNotificationRequest, 1001),
		}
		body, err := json.Marshal(req)
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, "/notifications/batch", bytes.NewReader(body))
		rr := httptest.NewRecorder()

		h.CreateBatch(rr, r)

		if rr.Code != http.StatusUnprocessableEntity {
			t.Errorf("expected 422 for batch limit, got %d", rr.Code)
		}
	})
}

func TestNotificationHandler_Get(t *testing.T) {
	notif := &domain.Notification{
		ID: "n-1", Channel: domain.ChannelSMS, Recipient: "+15551234567", Content: "Hi",
		Status: domain.StatusPending, Priority: domain.PriorityNormal,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	tests := []struct {
		name       string
		id         string
		svc        NotificationService
		wantStatus int
		wantBody   bool // has notification id in JSON
	}{
		{
			name: "200 service returns notification",
			id:   "n-1",
			svc: &stubNotificationService{
				getResp: notif,
				getErr:  nil,
			},
			wantStatus: http.StatusOK,
			wantBody:   true,
		},
		{
			name: "404 service returns ErrNotFound",
			id:   "missing",
			svc: &stubNotificationService{
				getResp: nil,
				getErr:  repository.ErrNotFound,
			},
			wantStatus: http.StatusNotFound,
			wantBody:   false,
		},
		{
			name: "500 service returns other error",
			id:   "n-1",
			svc: &stubNotificationService{
				getResp: nil,
				getErr:  errors.New("db error"),
			},
			wantStatus: http.StatusInternalServerError,
			wantBody:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewNotificationHandler(tt.svc)
			r := requestWithChiParam(httptest.NewRequest(http.MethodGet, "/notifications/"+tt.id, nil), tt.id)
			rr := httptest.NewRecorder()
			h.Get(rr, r)
			assert.Equal(t, tt.wantStatus, rr.Code)
			if tt.wantBody {
				var resp NotificationResponse
				require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
				assert.Equal(t, notif.ID, resp.ID)
			} else if rr.Code >= 400 {
				var errResp ErrorResponse
				require.NoError(t, json.NewDecoder(rr.Body).Decode(&errResp))
				assert.NotEmpty(t, errResp.Error)
			}
		})
	}
}

func TestNotificationHandler_List(t *testing.T) {
	listNotifs := []domain.Notification{
		{ID: "n-1", Channel: domain.ChannelSMS, Recipient: "+15551111111", Content: "Hi", Status: domain.StatusPending},
	}
	tests := []struct {
		name       string
		svc        NotificationService
		wantStatus int
		wantCount  int
	}{
		{
			name: "200 service returns list and cursor",
			svc: &stubNotificationService{
				listResp:   listNotifs,
				listCursor: "cursor-1",
				listErr:    nil,
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "500 service returns error",
			svc: &stubNotificationService{
				listResp: nil,
				listErr:  errors.New("db error"),
			},
			wantStatus: http.StatusInternalServerError,
			wantCount:  0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewNotificationHandler(tt.svc)
			r := httptest.NewRequest(http.MethodGet, "/notifications", nil)
			rr := httptest.NewRecorder()
			h.List(rr, r)
			assert.Equal(t, tt.wantStatus, rr.Code)
			if tt.wantStatus == http.StatusOK {
				var resp ListResponse
				require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
				assert.Len(t, resp.Notifications, tt.wantCount)
				if tt.wantCount > 0 {
					assert.Equal(t, "n-1", resp.Notifications[0].ID)
				}
			}
		})
	}
}

func TestNotificationHandler_Cancel(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		svc        NotificationService
		wantStatus int
	}{
		{"204 success", "n-1", &stubNotificationService{cancelErr: nil}, http.StatusNoContent},
		{"404 ErrNotFound", "missing", &stubNotificationService{cancelErr: repository.ErrNotFound}, http.StatusNotFound},
		{"409 ErrCannotCancel", "n-1", &stubNotificationService{cancelErr: repository.ErrCannotCancel}, http.StatusConflict},
		{"500 other error", "n-1", &stubNotificationService{cancelErr: errors.New("db error")}, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewNotificationHandler(tt.svc)
			r := requestWithChiParam(httptest.NewRequest(http.MethodDelete, "/notifications/"+tt.id, nil), tt.id)
			rr := httptest.NewRecorder()
			h.Cancel(rr, r)
			assert.Equal(t, tt.wantStatus, rr.Code)
			if tt.wantStatus == http.StatusNoContent {
				assert.Empty(t, rr.Body.Len())
			}
		})
	}
}

func TestNotificationHandler_Create_ServiceError(t *testing.T) {
	svc := &stubNotificationService{createErr: errors.New("tx failed")}
	h := NewNotificationHandler(svc)
	body, err := json.Marshal(CreateNotificationRequest{
		Recipient: "+15551234567", Channel: "sms", Content: "Hi",
	})
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.Create(rr, r)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&errResp))
	assert.NotEmpty(t, errResp.Error)
}

func TestNotificationHandler_CreateBatch_Success(t *testing.T) {
	n1 := &domain.Notification{ID: "n-1", Channel: domain.ChannelSMS, Recipient: "+15551111111", Content: "Hi", BatchID: null.StringFrom("batch-1")}
	n2 := &domain.Notification{ID: "n-2", Channel: domain.ChannelEmail, Recipient: "a@b.com", Content: "Hello", BatchID: null.StringFrom("batch-1")}
	svc := &stubNotificationService{batchResp: []*domain.Notification{n1, n2}, batchErr: nil}
	h := NewNotificationHandler(svc)
	req := BatchCreateRequest{
		Notifications: []CreateNotificationRequest{
			{Recipient: "+15551111111", Channel: "sms", Content: "Hi"},
			{Recipient: "a@b.com", Channel: "email", Content: "Hello"},
		},
	}
	body, err := json.Marshal(req)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/notifications/batch", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.CreateBatch(rr, r)
	require.Equal(t, http.StatusCreated, rr.Code)
	var resp BatchCreateResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, 2, resp.Count)
	assert.Equal(t, "batch-1", resp.BatchID)
	assert.Len(t, resp.Notifications, 2)
}

func TestNotificationHandler_CreateBatch_EmptyArray(t *testing.T) {
	svc := &stubNotificationService{}
	h := NewNotificationHandler(svc)
	req := BatchCreateRequest{Notifications: []CreateNotificationRequest{}}
	body, err := json.Marshal(req)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/notifications/batch", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.CreateBatch(rr, r)
	assert.Equal(t, http.StatusUnprocessableEntity, rr.Code)
	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&errResp))
	assert.Contains(t, errResp.Error, "empty")
}

// TestNotificationHandler_List_QueryFilters verifies that query parameters (status, channel,
// batch_id, limit) are correctly parsed and forwarded to the service as a domain.Filter.
func TestNotificationHandler_List_QueryFilters(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantStatus  domain.Status
		wantChannel domain.Channel
		wantLimit   int
		wantBatchID string
	}{
		{
			name:        "filters status and channel",
			query:       "?status=delivered&channel=email&limit=5",
			wantStatus:  domain.StatusDelivered,
			wantChannel: domain.ChannelEmail,
			wantLimit:   5,
		},
		{
			name:        "filters batch_id",
			query:       "?batch_id=batch-abc",
			wantBatchID: "batch-abc",
			wantLimit:   20, // default
		},
		{
			name:      "limit clamped to 100 when over",
			query:     "?limit=9999",
			wantLimit: 20, // clamped to default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured domain.Filter
			svc := &capturingService{
				onList: func(f domain.Filter) ([]domain.Notification, string, error) {
					captured = f
					return nil, "", nil
				},
			}
			h := NewNotificationHandler(svc)
			r := httptest.NewRequest(http.MethodGet, "/notifications"+tt.query, nil)
			rr := httptest.NewRecorder()
			h.List(rr, r)

			require.Equal(t, http.StatusOK, rr.Code)

			if tt.wantStatus != "" {
				require.NotNil(t, captured.Status, "expected Status filter to be set")
				assert.Equal(t, tt.wantStatus, *captured.Status)
			} else {
				assert.Nil(t, captured.Status)
			}
			if tt.wantChannel != "" {
				require.NotNil(t, captured.Channel, "expected Channel filter to be set")
				assert.Equal(t, tt.wantChannel, *captured.Channel)
			} else {
				assert.Nil(t, captured.Channel)
			}
			if tt.wantBatchID != "" {
				require.NotNil(t, captured.BatchID, "expected BatchID filter to be set")
				assert.Equal(t, tt.wantBatchID, *captured.BatchID)
			}
			assert.Equal(t, tt.wantLimit, captured.Limit)
		})
	}
}

// capturingService is a test double that captures the filter passed to List.
type capturingService struct {
	onList func(f domain.Filter) ([]domain.Notification, string, error)
}

func (s *capturingService) Create(_ context.Context, _ *domain.Notification) (*domain.Notification, bool, error) {
	return nil, false, errors.New("not implemented")
}
func (s *capturingService) BatchCreate(_ context.Context, _ []*domain.Notification) ([]*domain.Notification, error) {
	return nil, errors.New("not implemented")
}
func (s *capturingService) Get(_ context.Context, _ string) (*domain.Notification, error) {
	return nil, errors.New("not implemented")
}
func (s *capturingService) List(_ context.Context, f domain.Filter) ([]domain.Notification, string, error) {
	if s.onList != nil {
		return s.onList(f)
	}
	return nil, "", nil
}
func (s *capturingService) Cancel(_ context.Context, _ string) error {
	return errors.New("not implemented")
}

// TestNotificationHandler_Create_Idempotent_Returns200 verifies that a second request
// carrying the same idempotency key gets HTTP 200 (not 201) and the existing notification.
func TestNotificationHandler_Create_Idempotent_Returns200(t *testing.T) {
	existing := &domain.Notification{
		ID:        "existing-id",
		Channel:   domain.ChannelSMS,
		Recipient: "+15551234567",
		Content:   "Hello",
		Status:    domain.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	svc := &stubNotificationService{
		createResp: existing,
		createNew:  false, // existing resource, not newly created
		createErr:  nil,
	}
	h := NewNotificationHandler(svc)

	body, err := json.Marshal(CreateNotificationRequest{
		Recipient:      "+15551234567",
		Channel:        "sms",
		Content:        "Hello",
		IdempotencyKey: "key-abc",
	})
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/notifications", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.Create(rr, r)

	assert.Equal(t, http.StatusOK, rr.Code, "duplicate idempotency key must return 200, not 201")
	var resp NotificationResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "existing-id", resp.ID)
}

func TestNotificationHandler_CreateBatch_ServiceError(t *testing.T) {
	svc := &stubNotificationService{batchErr: errors.New("batch failed")}
	h := NewNotificationHandler(svc)
	req := BatchCreateRequest{
		Notifications: []CreateNotificationRequest{
			{Recipient: "+15551111111", Channel: "sms", Content: "Hi"},
		},
	}
	body, err := json.Marshal(req)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/notifications/batch", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.CreateBatch(rr, r)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	var errResp ErrorResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&errResp))
	assert.NotEmpty(t, errResp.Error)
}
