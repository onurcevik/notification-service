package handler

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResponseWriter_Hijack_UnderlyingNotHijacker(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: rec, status: http.StatusOK}
	conn, buf, err := rw.Hijack()
	assert.Nil(t, conn)
	assert.Nil(t, buf)
	require.Error(t, err)
	assert.Same(t, errHijackerNotSupported, err)
	assert.Contains(t, err.Error(), "Hijacker")
}

func TestResponseWriter_Hijack_DelegatesToUnderlying(t *testing.T) {
	sentinel := errors.New("hijack failed")
	fake := &fakeHijacker{ResponseWriter: httptest.NewRecorder(), hijackErr: sentinel}
	rw := &responseWriter{ResponseWriter: fake, status: http.StatusOK}
	conn, buf, err := rw.Hijack()
	assert.Nil(t, conn)
	assert.Nil(t, buf)
	require.Error(t, err)
	assert.Same(t, sentinel, err)
	assert.True(t, fake.hijackCalled, "underlying Hijack should have been called")
}

// fakeHijacker is an http.ResponseWriter that implements http.Hijacker for tests.
type fakeHijacker struct {
	http.ResponseWriter
	hijackCalled bool
	hijackErr    error
}

func (f *fakeHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	f.hijackCalled = true
	return nil, nil, f.hijackErr
}
