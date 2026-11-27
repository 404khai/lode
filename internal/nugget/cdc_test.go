package nugget

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
)

func chunks(t *testing.T, data []byte) [][]byte {
	t.Helper()
	c := NewChunker(bytes.NewReader(data))
	var out [][]byte
	for {
		b, err := c.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, append([]byte(nil), b...))
	}
}

func random(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func TestChunkerRoundTripAndBounds(t *testing.T) {
	data := random(40<<20, 1)
	cs := chunks(t, data)
	if !bytes.Equal(bytes.Join(cs, nil), data) {
		t.Fatal("chunks do not reassemble the input")
	}
	for i, c := range cs {
		if len(c) > MaxSize || (i < len(cs)-1 && len(c) < MinSize) {
			t.Fatalf("chunk %d has size %d", i, len(c))
		}
	}
	avg := len(data) / len(cs)
	if avg < 2<<20 || avg > 6<<20 {
		t.Fatalf("average chunk size %d far from target", avg)
	}
	if got := chunks(t, nil); len(got) != 0 {
		t.Fatal("empty input produced chunks")
	}
	if got := chunks(t, []byte("tiny")); len(got) != 1 {
		t.Fatal("tiny input should be one chunk")
	}
}

// Inserting bytes near the start must leave most later chunks unchanged:
// the point of content-defined chunking.
func TestChunkerResynchronises(t *testing.T) {
	data := random(40<<20, 2)
	shifted := append(append(append([]byte(nil), data[:1000]...), []byte("inserted!")...), data[1000:]...)
	seen := map[ID]bool{}
	for _, c := range chunks(t, data) {
		seen[Sum(c)] = true
	}
	cs := chunks(t, shifted)
	shared := 0
	for _, c := range cs {
		if seen[Sum(c)] {
			shared++
		}
	}
	if shared < len(cs)-2 {
		t.Fatalf("only %d of %d chunks survived a 9-byte insert", shared, len(cs))
	}
}
