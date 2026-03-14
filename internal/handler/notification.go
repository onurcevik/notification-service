package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"gitlab.com/onurcevik/notification-service/internal/domain"
	"gitlab.com/onurcevik/notification-service/internal/repository"
)

var tracer = otel.Tracer("notification-service/handler")

// NotificationService defines the operations the handler needs from the service layer.
type NotificationService interface {
	Create(ctx context.Context, n *domain.Notification) (*domain.Notification, bool, error)
	BatchCreate(ctx context.Context, notifications []*domain.Notification) ([]*domain.Notification, error)
	Get(ctx context.Context, id string) (*domain.Notification, error)
	List(ctx context.Context, f domain.Filter) ([]domain.Notification, string, error)
	Cancel(ctx context.Context, id string) error
}

// NotificationHandler handles HTTP requests for notifications.
type NotificationHandler struct {
	svc NotificationService
}

// NewNotificationHandler creates a new NotificationHandler.
func NewNotificationHandler(svc NotificationService) *NotificationHandler {
	return &NotificationHandler{svc: svc}
}

// Create handles a single notification creation request.
// @Summary Create notification
// @Description Create a single notification. Provide content directly or use template + template_vars for inline rendering. Use idempotency_key to avoid duplicates on retries. scheduled_at must be RFC3339 (e.g. 2025-12-31T09:00:00Z); omit for immediate send.
// @Tags Notifications
// @Accept json
// @Produce json
// @Param request body CreateNotificationRequest true "Notification details"
// @Success 201 {object} NotificationResponse
// @Failure 400 {object} ErrorResponse
// @Failure 422 {object} ErrorResponse
// @Router /notifications [post]
func (h *NotificationHandler) Create(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "handler.Create")
	defer span.End()
	r = r.WithContext(ctx)

	var req CreateNotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body", nil)
		return
	}
	if req.Template != "" {
		content, err := renderTemplate(req.Template, req.TemplateVars)
		if err != nil {
			writeError(w, http.StatusUnprocessableEntity, err.Error(), nil)
			return
		}
		req.Content = content
	}
	if errs := validate(req); len(errs) > 0 {
		writeError(w, http.StatusUnprocessableEntity, "validation failed", errs)
		return
	}

	n, created, err := h.svc.Create(r.Context(), toDomain(req))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create notification", nil)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK // idempotent: same key, return existing
	}
	writeJSON(w, status, toResponse(n))
}

// CreateBatch handles multiple notification requests in a single call.
// @Summary Create batch notifications
// @Description Send up to 1000 notifications in one request
// @Tags Notifications
// @Accept json
// @Produce json
// @Param request body BatchCreateRequest true "List of notifications"
// @Success 201 {object} BatchCreateResponse
// @Failure 422 {object} ErrorResponse
// @Router /notifications/batch [post]
func (h *NotificationHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "handler.CreateBatch")
	defer span.End()
	r = r.WithContext(ctx)

	var req BatchCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body", nil)
		return
	}
	if len(req.Notifications) == 0 {
		writeError(w, http.StatusUnprocessableEntity, "notifications array is empty", nil)
		return
	}
	if len(req.Notifications) > 1000 {
		writeError(w, http.StatusUnprocessableEntity, "batch limit is 1000", nil)
		return
	}

	var domainNotifs []*domain.Notification
	for _, item := range req.Notifications {
		if errs := validate(item); len(errs) > 0 {
			writeError(w, http.StatusUnprocessableEntity, "validation failed", errs)
			return
		}
		domainNotifs = append(domainNotifs, toDomain(item))
	}

	created, err := h.svc.BatchCreate(r.Context(), domainNotifs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to create batch", nil)
		return
	}

	resp := BatchCreateResponse{Count: len(created)}
	if len(created) > 0 {
		resp.BatchID = created[0].BatchID.String
	}
	for _, n := range created {
		resp.Notifications = append(resp.Notifications, toResponse(n))
	}
	writeJSON(w, http.StatusCreated, resp)
}

// Get retrieves a single notification by ID.
// @Summary Get notification
// @Description Fetch details of a specific notification
// @Tags Notifications
// @Produce json
// @Param id path string true "Notification ID (UUID)"
// @Success 200 {object} NotificationResponse
// @Failure 404 {object} ErrorResponse
// @Router /notifications/{id} [get]
func (h *NotificationHandler) Get(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "handler.Get")
	defer span.End()
	r = r.WithContext(ctx)

	id := chi.URLParam(r, "id")
	span.SetAttributes(attribute.String("notification.id", id))
	n, err := h.svc.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			writeError(w, http.StatusNotFound, "notification not found", nil)
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to get notification", nil)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(n))
}

// List returns a paginated list of notifications with filters.
// @Summary List notifications
// @Description Query notifications with status, channel, and optional date range. Use cursor from response for next page.
// @Tags Notifications
// @Produce json
// @Param status query string false "Filter by status" Enums(pending, processing, delivered, failed, cancelled, scheduled)
// @Param channel query string false "Filter by channel" Enums(sms, email, push)
// @Param batch_id query string false "Filter by batch ID (UUID)"
// @Param created_after query string false "Only notifications created after this time (RFC3339)"
// @Param created_before query string false "Only notifications created before this time (RFC3339)"
// @Param limit query int false "Results per page (default 20, max 100)"
// @Param cursor query string false "Pagination cursor from previous response (opaque)"
// @Success 200 {object} ListResponse
// @Router /notifications [get]
func (h *NotificationHandler) List(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "handler.List")
	defer span.End()
	r = r.WithContext(ctx)

	q := r.URL.Query()
	f := domain.Filter{Limit: 20}

	if s := q.Get("status"); s != "" {
		span.SetAttributes(attribute.String("filter.status", s))
		status := domain.Status(s)
		f.Status = &status
	}
	if c := q.Get("channel"); c != "" {
		span.SetAttributes(attribute.String("filter.channel", c))
		ch := domain.Channel(c)
		f.Channel = &ch
	}
	if b := q.Get("batch_id"); b != "" {
		span.SetAttributes(attribute.String("filter.batch_id", b))
		f.BatchID = &b
	}
	if v := q.Get("created_after"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			span.SetAttributes(attribute.String("filter.created_after", v))
			f.CreatedAfter = &t
		}
	}
	if v := q.Get("created_before"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			span.SetAttributes(attribute.String("filter.created_before", v))
			f.CreatedBefore = &t
		}
	}
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			f.Limit = n
		}
	}
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	span.SetAttributes(attribute.Int("filter.limit", f.Limit))

	if cursor := q.Get("cursor"); cursor != "" {
		span.SetAttributes(attribute.String("filter.cursor", cursor))
		parts := strings.SplitN(cursor, "|", 2)
		if len(parts) == 2 {
			if t, err := time.Parse(time.RFC3339Nano, parts[0]); err == nil {
				f.CursorAfter = &t
				f.CursorLastID = &parts[1]
			}
		}
	}

	notifications, nextCursor, err := h.svc.List(r.Context(), f)
	if err != nil {
		span.RecordError(err)
		writeError(w, http.StatusInternalServerError, "failed to list notifications", nil)
		return
	}

	writeJSON(w, http.StatusOK, ListResponse{
		Notifications: toResponseList(notifications),
		NextCursor:    nextCursor,
	})
}

// Cancel cancels a pending or scheduled notification.
// @Summary Cancel notification
// @Description Stop a notification that hasn't been processed yet
// @Tags Notifications
// @Param id path string true "Notification ID (UUID)"
// @Success 204 "No Content"
// @Failure 404 {object} ErrorResponse
// @Failure 409 {object} ErrorResponse
// @Router /notifications/{id} [delete]
func (h *NotificationHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "handler.Cancel")
	defer span.End()
	r = r.WithContext(ctx)

	id := chi.URLParam(r, "id")
	span.SetAttributes(attribute.String("notification.id", id))
	err := h.svc.Cancel(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, repository.ErrNotFound):
			writeError(w, http.StatusNotFound, "notification not found", nil)
		case errors.Is(err, repository.ErrCannotCancel):
			writeError(w, http.StatusConflict, "only pending or scheduled notifications can be cancelled", nil)
		default:
			writeError(w, http.StatusInternalServerError, "failed to cancel notification", nil)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
