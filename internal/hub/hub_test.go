package hub

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHub_Subscribe_Unsubscribe(t *testing.T) {
	h := New()
	ch, unsub := h.Subscribe()

	// Broadcast to the single subscriber
	h.Broadcast("id1", "delivered")
	payload := <-ch
	var ev StatusEvent
	require.NoError(t, json.Unmarshal(payload, &ev))
	assert.Equal(t, "id1", ev.NotificationID)
	assert.Equal(t, "delivered", ev.Status)

	unsub() // closes ch
	_, ok := <-ch
	assert.False(t, ok, "channel should be closed after unsubscribe")
}

func TestHub_Broadcast_MultipleSubscribers(t *testing.T) {
	h := New()
	ch1, unsub1 := h.Subscribe()
	ch2, unsub2 := h.Subscribe()
	defer unsub2()

	h.Broadcast("id2", "processing")
	assert.Equal(t, "id2", mustUnmarshal(t, <-ch1).NotificationID)
	assert.Equal(t, "id2", mustUnmarshal(t, <-ch2).NotificationID)

	unsub1() // only ch1 unsubscribed
	h.Broadcast("id3", "delivered")
	// ch1 is closed; ch2 should still receive
	assert.Equal(t, "id3", mustUnmarshal(t, <-ch2).NotificationID)
}

func TestHub_Broadcast_Concurrent(t *testing.T) {
	h := New()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		ch, unsub := h.Subscribe()
		go func() {
			defer wg.Done()
			defer unsub()
			for j := 0; j < 20; j++ {
				<-ch
			}
		}()
	}
	for i := 0; i < 20; i++ {
		h.Broadcast("id", "delivered")
	}
	wg.Wait()
}

func mustUnmarshal(t *testing.T, payload []byte) StatusEvent {
	t.Helper()
	var ev StatusEvent
	require.NoError(t, json.Unmarshal(payload, &ev))
	return ev
}
