package events

import "testing"

func TestPublishSubscribe(t *testing.T) {
	b := NewBus()
	ch, cancel := b.Subscribe(1)
	b.Publish(Event{Type: "a"})
	b.Publish(Event{Type: "dropped"}) // buffer full, must not block
	if e := <-ch; e.Type != "a" || e.Time.IsZero() {
		t.Fatalf("got %+v", e)
	}
	cancel()
	cancel()
	if _, ok := <-ch; ok {
		t.Fatal("channel not closed")
	}
	var nilBus *Bus
	nilBus.Publish(Event{Type: "x"})
}
