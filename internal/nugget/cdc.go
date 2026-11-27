package nugget

import (
	"errors"
	"io"
	"math/bits"
)

// Content-defined chunking parameters (FastCDC with normalised chunking,
// level 2). Changing any of these, or the gear table, changes where objects
// are cut and therefore defeats dedup against existing data: treat them as
// part of the on-disk format (ADR 0002).
const (
	MinSize = 1 << 20
	AvgSize = Size // 4 MiB
	MaxSize = 8 << 20
)

var (
	gear         [256]uint64
	maskS, maskL uint64
)

func init() {
	// Deterministic gear table from splitmix64; never change the seed.
	x := uint64(0x6c6f64655f636463) // "lode_cdc"
	for i := range gear {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		gear[i] = z ^ (z >> 31)
	}
	b := bits.Len(uint(AvgSize)) - 1 // log2(avg)
	// The gear hash shifts left, so the high bits carry the most history.
	maskS = ^uint64(0) << (64 - (b + 2))
	maskL = ^uint64(0) << (64 - (b - 2))
}

// cut returns the length of the first chunk of data. eof says no more data
// follows, so a short tail may be returned as the final chunk.
func cut(data []byte) int {
	n := len(data)
	if n <= MinSize {
		return n
	}
	normal := min(AvgSize, n)
	limit := min(MaxSize, n)
	var fp uint64
	i := MinSize
	for ; i < normal; i++ {
		fp = (fp << 1) + gear[data[i]]
		if fp&maskS == 0 {
			return i + 1
		}
	}
	for ; i < limit; i++ {
		fp = (fp << 1) + gear[data[i]]
		if fp&maskL == 0 {
			return i + 1
		}
	}
	return limit
}

// Chunker splits a stream into content-defined chunks.
type Chunker struct {
	r          io.Reader
	buf        []byte
	start, end int
	eof        bool
}

func NewChunker(r io.Reader) *Chunker {
	return &Chunker{r: r, buf: make([]byte, 2*MaxSize)}
}

// Next returns the next chunk, or io.EOF after the last one. The returned
// slice is only valid until the following call.
func (c *Chunker) Next() ([]byte, error) {
	if c.end-c.start < MaxSize && !c.eof {
		copy(c.buf, c.buf[c.start:c.end])
		c.end -= c.start
		c.start = 0
		for c.end < len(c.buf) && !c.eof {
			n, err := c.r.Read(c.buf[c.end:])
			c.end += n
			if errors.Is(err, io.EOF) {
				c.eof = true
			} else if err != nil {
				return nil, err
			}
		}
	}
	if c.start == c.end {
		return nil, io.EOF
	}
	avail := c.buf[c.start:c.end]
	n := len(avail)
	if !c.eof || n > MinSize {
		n = cut(avail)
	}
	c.start += n
	return avail[:n], nil
}
