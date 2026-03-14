package handler

import (
	"bufio"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const correlationIDHeader = "X-Correlation-ID"

func correlationIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(correlationIDHeader, id)
		logger := log.With().Str("correlation_id", id).Logger()
		next.ServeHTTP(w, r.WithContext(logger.WithContext(r.Context())))
	})
}

func loggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		log.Ctx(r.Context()).Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", rw.status).
			Int64("latency_ms", time.Since(start).Milliseconds()).
			Msg("request")
	})
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// Hijack delegates to the underlying ResponseWriter when it implements http.Hijacker,
// so WebSocket upgrades work through the logger middleware.
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errHijackerNotSupported
	}
	return h.Hijack()
}

var errHijackerNotSupported = &errHijacker{}

type errHijacker struct{}

func (*errHijacker) Error() string { return "response does not implement http.Hijacker" }
