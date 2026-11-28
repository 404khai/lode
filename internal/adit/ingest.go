package adit

import (
	"bytes"
	"context"
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
//
// defaultAlgo, if set and the request declares no checksum, makes ingest
// compute that checksum itself (multipart uploads with a checksum algorithm).
func (s *Server) ingest(req *request, maxSize int64, bucket ledger.Bucket, defaultAlgo string) (*ingested, error) {
	r := req.r
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
	computed := false
	if sum == nil && defaultAlgo != "" {
		sum = &checksum{algorithm: defaultAlgo, h: checksumAlgorithms[defaultAlgo]()}
		computed = true
	} else if sum != nil && defaultAlgo != "" && sum.algorithm != defaultAlgo {
		return nil, errf(http.StatusBadRequest, "InvalidRequest", "Checksum Type mismatch occurred, expected checksum Type: %s, actual checksum Type: %s", defaultAlgo, sum.algorithm)
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

	counted := &countingReader{r: io.LimitReader(body, body.size)}
	out, err := s.storeStream(r.Context(), counted, bucket, sum)
	if err != nil {
		return nil, err
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
	if md5Want != nil && !bytes.Equal(md5Want, out.md5) {
		return nil, errBadDigest
	}
	if computed {
		sum.expected = sum.sum()
	} else if sum != nil {
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

// storeStream chunks src and stores every nugget at the bucket's replication,
// touching each in the Ledger before writing it. sum, if set, is fed the
// bytes. It returns the manifest entries, size and MD5.
func (s *Server) storeStream(ctx context.Context, src io.Reader, bucket ledger.Bucket, sum *checksum) (*ingested, error) {
	replicas, quorum := s.writePolicy(bucket)
	placer, err := s.strategy(bucket)
	if err != nil {
		return nil, err
	}
	md5h := md5.New() // #nosec G401
	hashers := []hash.Hash{md5h}
	if sum != nil {
		hashers = append(hashers, sum.h)
	}
	chunker := nugget.NewChunker(src)
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
		layout, err := s.ledger.TouchNugget(ctx, ref, replicas, bucket.ECData, bucket.ECParity)
		if err != nil {
			return nil, err
		}
		// A nugget keeps the layout it was first written with, so content
		// shared with another bucket is stored once whatever its class.
		var serr error
		if layout.ECData > 0 {
			var written []ledger.ShardLoc
			written, serr = s.cluster.StoreEC(ctx, ref.ID, data, layout.Shards, layout.ECData, layout.ECParity, 0, placer)
			err = s.ledger.AddShards(ctx, ref.ID, written)
		} else {
			var written []string
			written, serr = s.cluster.Store(ctx, ref.ID, data, layout.Replicas, replicas, quorum, placer)
			err = s.ledger.AddReplicas(ctx, ref.ID, written)
		}
		// Every copy or shard that landed is recorded, even on failure, so Tailings can find it.
		if err != nil {
			return nil, err
		}
		if serr != nil {
			return nil, serr
		}
		out.nuggets = append(out.nuggets, ref)
		out.size += ref.Length
	}
	out.md5 = md5h.Sum(nil)
	return out, nil
}
