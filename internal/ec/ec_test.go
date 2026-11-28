package ec

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/404khai/lode/internal/nugget"
)

func TestRoundTripWithLosses(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for _, size := range []int{0, 1, 4095, 1 << 20, 8<<20 - 3} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(r.Uint32())
		}
		id := nugget.Sum(data)
		shards, err := Encode(id, data, 4, 2)
		if err != nil {
			t.Fatal(err)
		}
		raw := func() [][]byte {
			out := make([][]byte, len(shards))
			for i, s := range shards {
				if nugget.Sum(s.Blob) != s.ID {
					t.Fatal("shard id mismatch")
				}
				b, err := ParseBlob(s.Blob, id, i)
				if err != nil {
					t.Fatal(err)
				}
				out[i] = b
			}
			return out
		}
		// Any two losses are survivable.
		for a := 0; a < 6; a++ {
			for b := a + 1; b < 6; b++ {
				in := raw()
				in[a], in[b] = nil, nil
				got, err := Decode(id, int64(size), 4, 2, in)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("size %d losing %d,%d: %v", size, a, b, err)
				}
			}
		}
		in := raw()
		in[0], in[1], in[2] = nil, nil, nil
		if _, err := Decode(id, int64(size), 4, 2, in); err == nil {
			t.Fatal("three losses with m=2 must fail")
		}
		// Reconstruct restores every shard bit for bit.
		in = raw()
		in[1], in[5] = nil, nil
		if err := Reconstruct(4, 2, in); err != nil {
			t.Fatal(err)
		}
		if Blob(id, 5, in[5]).ID != shards[5].ID || Blob(id, 1, in[1]).ID != shards[1].ID {
			t.Fatal("reconstructed shards differ")
		}
	}
}

func TestShardIDsAreUniquePerNugget(t *testing.T) {
	zeros := make([]byte, 4096)
	other := append(make([]byte, 4095), 1)
	a, _ := Encode(nugget.Sum(zeros), zeros, 2, 1)
	b, _ := Encode(nugget.Sum(other), other, 2, 1)
	if a[0].ID == b[0].ID {
		t.Fatal("identical shard bytes of different nuggets share an ID")
	}
	if _, err := ParseBlob(a[0].Blob, nugget.Sum(other), 0); err == nil {
		t.Fatal("blob accepted for the wrong nugget")
	}
	if Validate(0, 2) == nil || Validate(30, 3) == nil {
		t.Fatal("bad layouts accepted")
	}
}
