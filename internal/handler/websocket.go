package handler

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// StatusHub is the minimal interface needed for the WebSocket handler.
type StatusHub interface {
	Subscribe() (ch chan []byte, unsubscribe func())
}

// WebSocketHandler streams notification status updates to connected clients.
type WebSocketHandler struct {
	hub StatusHub
}

// NewWebSocketHandler creates a new WebSocket handler.
func NewWebSocketHandler(h StatusHub) *WebSocketHandler {
	return &WebSocketHandler{hub: h}
}

// ServeHTTP upgrades the connection to WebSocket and streams status events as JSON lines.
func (h *WebSocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Ctx(r.Context()).Error().Err(err).Msg("websocket upgrade failed")
		return
	}
	defer conn.Close()

	ch, unsubscribe := h.hub.Subscribe()
	defer unsubscribe()

	// Ping to keep connection alive and detect client disconnect
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		case payload, ok := <-ch:
			if !ok {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
				return
			}
		}
	}
}
