// Package ec erasure-codes nuggets with Reed-Solomon. A nugget is split into
// k data shards and m parity shards; any k of them reconstruct it. Each
// shard is stored on a vein as a blob prefixed with a header naming its
// nugget and index, and is addressed by the BLAKE3 of that blob, so shards of
// different nuggets never share an ID even when their bytes are equal.
package ec

import (
	"errors"
	"fmt"

	"github.com/klauspost/reedsolomon"

	"github.com/404khai/lode/internal/nugget"
)

// Blob layout: magic(4) | version(1) | index(1) | nugget id(32) | shard bytes.
const (
	magic      = "LDSH"
	version    = 1
	headerSize = 4 + 1 + 1 + 32
	MaxShards  = 32
)

var (
	ErrLayout = errors.New("ec: invalid layout")
	// ErrTooFewShards means fewer than k verified shards were supplied.
	ErrTooFewShards = errors.New("ec: not enough shards to reconstruct")
)

// Shard is one encoded shard ready to store.
type Shard struct {
	Index int
	ID    nugget.ID // BLAKE3 of Blob
	Blob  []byte
}

// Validate checks a k+m layout.
func Validate(k, m int) error {
	if k < 1 || m < 1 || k+m > MaxShards {
		return fmt.Errorf("%w: %d+%d (need k>=1, m>=1, k+m<=%d)", ErrLayout, k, m, MaxShards)
	}
	return nil
}

func blob(id nugget.ID, idx int, data []byte) []byte {
	b := make([]byte, headerSize+len(data))
	copy(b, magic)
	b[4] = version
	b[5] = byte(idx) // #nosec G115 -- idx < MaxShards
	copy(b[6:], id[:])
	copy(b[headerSize:], data)
	return b
}

// ParseBlob checks a stored blob belongs to (id, idx) and returns its shard bytes.
func ParseBlob(b []byte, id nugget.ID, idx int) ([]byte, error) {
	if len(b) < headerSize || string(b[:4]) != magic || b[4] != version || int(b[5]) != idx || nugget.ID(b[6:headerSize]) != id {
		return nil, fmt.Errorf("ec: shard %d of %s has a bad header", idx, id)
	}
	return b[headerSize:], nil
}

// Encode splits data (whose ID is id) into k data and m parity shards.
func Encode(id nugget.ID, data []byte, k, m int) ([]Shard, error) {
	if err := Validate(k, m); err != nil {
		return nil, err
	}
	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, err
	}
	in := data
	if len(in) == 0 {
		in = []byte{0} // reedsolomon cannot split an empty buffer
	}
	parts, err := enc.Split(append([]byte(nil), in...))
	if err != nil {
		return nil, err
	}
	if err := enc.Encode(parts); err != nil {
		return nil, err
	}
	out := make([]Shard, len(parts))
	for i, p := range parts {
		b := blob(id, i, p)
		out[i] = Shard{Index: i, ID: nugget.Sum(b), Blob: b}
	}
	return out, nil
}

// Decode rebuilds the nugget from shard bytes indexed by shard number (nil =
// missing) and checks the result hashes to id. It needs at least k shards.
func Decode(id nugget.ID, length int64, k, m int, shards [][]byte) ([]byte, error) {
	if err := Validate(k, m); err != nil {
		return nil, err
	}
	if len(shards) != k+m {
		return nil, fmt.Errorf("%w: got %d shard slots, want %d", ErrLayout, len(shards), k+m)
	}
	have := 0
	for _, s := range shards {
		if s != nil {
			have++
		}
	}
	if have < k {
		return nil, fmt.Errorf("%w: %d of %d needed", ErrTooFewShards, have, k)
	}
	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, err
	}
	work := append([][]byte(nil), shards...)
	if err := enc.ReconstructData(work); err != nil {
		return nil, err
	}
	out := make([]byte, 0, max(length, 0))
	for _, s := range work[:k] {
		out = append(out, s...)
	}
	if int64(len(out)) < length {
		return nil, fmt.Errorf("ec: reconstructed %d bytes, want %d", len(out), length)
	}
	out = out[:length]
	if nugget.Sum(out) != id {
		return nil, fmt.Errorf("ec: reconstructed nugget %s fails verification", id)
	}
	return out, nil
}

// Reconstruct rebuilds every missing shard (data and parity) in place.
func Reconstruct(k, m int, shards [][]byte) error {
	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return err
	}
	return enc.Reconstruct(shards)
}

// Blob returns the stored form of shard idx of nugget id.
func Blob(id nugget.ID, idx int, data []byte) Shard {
	b := blob(id, idx, data)
	return Shard{Index: idx, ID: nugget.Sum(b), Blob: b}
}
