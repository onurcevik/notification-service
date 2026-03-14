package hub

import (
	"encoding/json"
	"sync"
)

// StatusEvent is sent to WebSocket clients when a notification's status changes.
type StatusEvent struct {
	NotificationID string `json:"notification_id"`
	Status         string `json:"status"`
}

// Hub broadcasts notification status updates to connected WebSocket clients.
type Hub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

// New creates a new Hub.
func New() *Hub {
	return &Hub{clients: make(map[chan []byte]struct{})}
}

// Subscribe adds a new client and returns a channel that receives JSON-serialized StatusEvents.
// The caller must receive from the channel until it is closed, and call Unsubscribe when done.
func (h *Hub) Subscribe() (ch chan []byte, unsubscribe func()) {
	ch = make(chan []byte, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	unsubscribe = func() {
		h.mu.Lock()
		delete(h.clients, ch)
		h.mu.Unlock()
		close(ch)
	}
	return ch, unsubscribe
}

// Broadcast sends a status event to all connected clients (non-blocking; drops if client buffer full).
func (h *Hub) Broadcast(notificationID, status string) {
	ev := StatusEvent{NotificationID: notificationID, Status: status}
	payload, err := json.Marshal(ev)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- payload:
		default:
			//skip if client is slow
		}
	}
}
