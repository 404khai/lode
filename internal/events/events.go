// Package events is the in-process structured event bus that Lantern and
// Foreman consume.
package events

import (
	"sync"
	"time"
)

// Event is a single structured occurrence in the cluster.
type Event struct {
	Type      string         `json:"type"`
	Component string         `json:"component"`
	Time      time.Time      `json:"time"`
	Attrs     map[string]any `json:"attrs,omitempty"`
}

// Bus fans events out to subscribers. Slow subscribers drop events rather
// than block publishers.
type Bus struct {
	mu   sync.RWMutex
	subs map[chan Event]struct{}
}

func NewBus() *Bus { return &Bus{subs: make(map[chan Event]struct{})} }

// Publish delivers e to every subscriber that has room. A nil Bus is a no-op.
func (b *Bus) Publish(e Event) {
	if b == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

// Subscribe returns a channel of events and a function that cancels it.
func (b *Bus) Subscribe(buffer int) (<-chan Event, func()) {
	ch := make(chan Event, buffer)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
		b.mu.Unlock()
	}
}
