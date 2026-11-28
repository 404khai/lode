package adit

import (
	"bytes"
	"crypto/md5" // #nosec G501 -- S3 ETag and Content-MD5, not used for security
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"net/http"

	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
)

// ingested is a verified request body stored as nuggets.
type ingested struct {
	nuggets []ledger.NuggetRef
	size    int64
	md5     []byte
	sum     *checksum
}

// countingReader counts bytes read from the decoded body.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// ingest reads, chunks and stores a request body, verifying every declared
// digest. Each nugget is touched in the Ledger before it is written to its
// replicas, so Tailings cannot reclaim it while the upload is in flight, and
// is written to a quorum of veins chosen by Survey. On failure the written
// nuggets stay unreferenced and are reclaimed after the grace window.
func (s *Server) ingest(req *request, maxSize int64, bucket ledger.Bucket) (*ingested, error) {
	r := req.r
	replicas, quorum := s.writePolicy(bucket)
	var md5Want []byte
	if v := r.Header.Get("Content-MD5"); v != "" {
		var err error
		if md5Want, err = base64.StdEncoding.DecodeString(v); err != nil || len(md5Want) != md5.Size {
			return nil, errInvalidDigest
		}
	}
	sum, aerr := requestChecksum(r)
	if aerr != nil {
		return nil, aerr
	}
	body, err := s.payloadReader(req)
	if err != nil {
		return nil, err
	}
	if body.size < 0 {
		return nil, errMissingLength
	}
	if body.size > maxSize {
		return nil, errEntityTooLarge
	}

	md5h := md5.New() // #nosec G401
	hashers := []hash.Hash{md5h}
	if sum != nil {
		hashers = append(hashers, sum.h)
	}
	counted := &countingReader{r: io.LimitReader(body, body.size)}
	chunker := nugget.NewChunker(counted)
	out := &ingested{sum: sum}
	for {
		data, err := chunker.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errIncompleteBody
			}
			return nil, err
		}
		for _, h := range hashers {
			h.Write(data)
		}
		ref := ledger.NuggetRef{ID: nugget.Sum(data), Length: int64(len(data))}
		existing, err := s.ledger.TouchNugget(r.Context(), ref)
		if err != nil {
			return nil, err
		}
		written, serr := s.cluster.Store(r.Context(), ref.ID, data, existing, replicas, quorum)
		// Record every copy that landed, even on failure, so Tailings can find it.
		if err := s.ledger.AddReplicas(r.Context(), ref.ID, written); err != nil {
			return nil, err
		}
		if serr != nil {
			return nil, serr
		}
		out.nuggets = append(out.nuggets, ref)
		out.size += ref.Length
	}
	if counted.n != body.size {
		return nil, errIncompleteBody
	}
	// Drain the stream terminator (and trailers); nothing else may follow.
	if extra, rerr := io.Copy(io.Discard, io.LimitReader(body, 1)); rerr != nil {
		return nil, rerr
	} else if extra > 0 {
		return nil, errf(http.StatusBadRequest, "IncompleteBody", "The request body is longer than the declared content length.")
	}
	if err := body.verify(); err != nil {
		return nil, err
	}
	out.md5 = md5h.Sum(nil)
	if md5Want != nil && !bytes.Equal(md5Want, out.md5) {
		return nil, errBadDigest
	}
	if sum != nil {
		if sum.trailer {
			sum.expected = body.trailer(sum.header())
		}
		if sum.expected == "" {
			return nil, errInvalidDigest
		}
		if sum.sum() != sum.expected {
			return nil, errBadDigest
		}
	}
	s.bytesIn.Add(float64(out.size))
	return out, nil
}
