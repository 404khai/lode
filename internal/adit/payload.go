package adit

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// payload is a request body decoded according to its x-amz-content-sha256
// mode. Call verify after reading it to EOF.
type payload struct {
	io.Reader
	size     int64 // decoded length, -1 if unknown
	chunked  *chunkedReader
	sha      hash.Hash
	expected string
}

func (p *payload) verify() error {
	if p.sha != nil && hex.EncodeToString(p.sha.Sum(nil)) != p.expected {
		return errContentSHA256
	}
	return nil
}

func (p *payload) trailer(name string) string {
	if p.chunked == nil {
		return ""
	}
	return p.chunked.trailers[name]
}

func (s *Server) payloadReader(req *request) (*payload, error) {
	r := req.r
	mode := req.auth.PayloadHash
	p := &payload{Reader: r.Body, size: r.ContentLength}
	switch mode {
	case unsignedPayload:
	case streamingSigned, streamingSignedTrailer, streamingUnsignedTrailer:
		n, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
		if err != nil || n < 0 {
			return nil, errMissingLength
		}
		p.size = n
		p.chunked = newChunkedReader(r.Body, req.auth, mode != streamingUnsignedTrailer, mode != streamingSigned)
		p.Reader = p.chunked
	default:
		if len(mode) != 64 || strings.Trim(strings.ToLower(mode), "0123456789abcdef") != "" {
			return nil, errf(http.StatusBadRequest, "InvalidArgument", "x-amz-content-sha256 must be UNSIGNED-PAYLOAD, a STREAMING-* value, or a SHA-256 hex digest")
		}
		p.sha = sha256.New()
		p.expected = strings.ToLower(mode)
		p.Reader = io.TeeReader(r.Body, p.sha)
	}
	return p, nil
}
