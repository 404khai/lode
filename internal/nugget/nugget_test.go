package nugget

import "testing"

func TestRoundTrip(t *testing.T) {
	id := Sum([]byte("hello"))
	got, err := Parse(id.String())
	if err != nil || got != id {
		t.Fatalf("parse: %v %v", got, err)
	}
	if Sum([]byte("hello")) != id || Sum([]byte("hellO")) == id {
		t.Fatal("hash not content-addressed")
	}
	if _, err := FromBytes([]byte{1}); err == nil {
		t.Fatal("expected length error")
	}
}
