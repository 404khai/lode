package adit

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const maxChunkSize = 64 << 20

var errMalformedChunk = errf(http.StatusBadRequest, "IncompleteBody", "The aws-chunked request body is malformed.")

// chunkedReader decodes an aws-chunked body, verifying chunk signatures
// when auth is non-nil, and collecting trailing headers.
type chunkedReader struct {
	br        *bufio.Reader
	auth      *authContext // nil for unsigned streams
	prevSig   string
	pending   string // signature of the chunk being read
	hasher    hash.Hash
	remaining int64
	trailers  map[string]string
	hasTrail  bool
	err       error
}

func newChunkedReader(r io.Reader, auth *authContext, signed, trailer bool) *chunkedReader {
	c := &chunkedReader{br: bufio.NewReaderSize(r, 64<<10), hasher: sha256.New(), trailers: map[string]string{}, hasTrail: trailer}
	if signed {
		c.auth = auth
		c.prevSig = auth.Signature
	}
	return c
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.remaining == 0 {
		if err := c.nextChunk(); err != nil {
			c.err = err
			return 0, err
		}
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.br.Read(p)
	c.remaining -= int64(n)
	c.hasher.Write(p[:n])
	if err == io.EOF {
		err = errIncompleteBody
	}
	if err != nil {
		c.err = err
		return n, err
	}
	if c.remaining == 0 {
		if err := c.endChunk(); err != nil {
			c.err = err
		}
	}
	return n, nil
}

func (c *chunkedReader) readLine() (string, error) {
	line, err := c.br.ReadSlice('\n')
	if err != nil {
		if err == io.EOF {
			return "", errIncompleteBody
		}
		return "", errMalformedChunk
	}
	return strings.TrimRight(string(line), "\r\n"), nil
}

func (c *chunkedReader) chunkSig(dataHash string) string {
	sts := "AWS4-HMAC-SHA256-PAYLOAD\n" + c.auth.AmzDate + "\n" + c.auth.Scope + "\n" + c.prevSig + "\n" + emptySHA256 + "\n" + dataHash
	return hex.EncodeToString(hmacSHA256(c.auth.SigningKey, sts))
}

func (c *chunkedReader) checkSig(want string) error {
	if c.auth == nil {
		return nil
	}
	got := c.chunkSig(hex.EncodeToString(c.hasher.Sum(nil)))
	if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return errSignatureMismatch
	}
	c.prevSig = want
	return nil
}

func (c *chunkedReader) nextChunk() error {
	line, err := c.readLine()
	if err != nil {
		return err
	}
	sizeStr, ext, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(strings.TrimSpace(sizeStr), 16, 64)
	if err != nil || size < 0 || size > maxChunkSize {
		return errMalformedChunk
	}
	c.pending = ""
	if c.auth != nil {
		sig, ok := strings.CutPrefix(ext, "chunk-signature=")
		if !ok {
			return errMalformedChunk
		}
		c.pending = sig
	}
	c.hasher.Reset()
	if size == 0 {
		if err := c.checkSig(c.pending); err != nil {
			return err
		}
		if err := c.readTrailers(); err != nil {
			return err
		}
		return io.EOF
	}
	c.remaining = size
	return nil
}

func (c *chunkedReader) endChunk() error {
	if line, err := c.readLine(); err != nil || line != "" {
		return errMalformedChunk
	}
	return c.checkSig(c.pending)
}

func (c *chunkedReader) readTrailers() error {
	var block bytes.Buffer
	var trailerSig string
	for {
		line, err := c.readLine()
		if err != nil {
			if !c.hasTrail && errors.Is(err, errIncompleteBody) {
				return nil // some clients omit the final CRLF
			}
			return err
		}
		if line == "" {
			break
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			return errMalformedChunk
		}
		k = strings.ToLower(strings.TrimSpace(k))
		v = strings.TrimSpace(v)
		if k == "x-amz-trailer-signature" {
			trailerSig = v
			continue
		}
		c.trailers[k] = v
		block.WriteString(k + ":" + v + "\n")
	}
	if c.auth != nil && c.hasTrail {
		sts := "AWS4-HMAC-SHA256-TRAILER\n" + c.auth.AmzDate + "\n" + c.auth.Scope + "\n" + c.prevSig + "\n" + sha256Hex(block.Bytes())
		want := hex.EncodeToString(hmacSHA256(c.auth.SigningKey, sts))
		if subtle.ConstantTimeCompare([]byte(want), []byte(trailerSig)) != 1 {
			return errSignatureMismatch
		}
	}
	return nil
}
