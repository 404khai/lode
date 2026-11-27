// Package nugget defines content-addressed chunks: a nugget's ID is the
// BLAKE3-256 hash of its bytes.
package nugget

import (
	"encoding/hex"
	"fmt"

	"github.com/zeebo/blake3"
)

// Size is the target nugget size. Phase 1 splits objects at fixed offsets;
// content-defined chunking replaces this later.
const Size = 4 << 20

// ID identifies a nugget by content.
type ID [32]byte

// Sum returns the ID of data.
func Sum(data []byte) ID { return ID(blake3.Sum256(data)) }

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// FromBytes converts a 32-byte slice to an ID.
func FromBytes(b []byte) (ID, error) {
	var id ID
	if len(b) != len(id) {
		return id, fmt.Errorf("nugget: id must be %d bytes, got %d", len(id), len(b))
	}
	copy(id[:], b)
	return id, nil
}

// Parse decodes a hex nugget ID.
func Parse(s string) (ID, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return ID{}, fmt.Errorf("nugget: %w", err)
	}
	return FromBytes(b)
}
