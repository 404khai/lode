package adit

import (
	"crypto/md5" // #nosec G501 -- S3 ETag and Content-MD5, not used for security
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
)

const (
	maxPutSize      = 5 << 30
	maxMetadataSize = 2 << 10
	defaultCT       = "binary/octet-stream"
)

// storedHeaders are the request headers kept with an object and returned on
// GET/HEAD, in addition to x-amz-meta-*.
var storedHeaders = []string{"content-type", "cache-control", "content-disposition", "content-encoding", "content-language", "expires"}

func objectHeaders(r *http.Request) (map[string]string, error) {
	h := map[string]string{}
	for _, name := range storedHeaders {
		if v := r.Header.Get(name); v != "" {
			h[name] = v
		}
	}
	if ce, ok := h["content-encoding"]; ok {
		var keep []string
		for _, enc := range strings.Split(ce, ",") {
			if enc = strings.TrimSpace(enc); enc != "" && enc != "aws-chunked" {
				keep = append(keep, enc)
			}
		}
		if len(keep) == 0 {
			delete(h, "content-encoding")
		} else {
			h["content-encoding"] = strings.Join(keep, ",")
		}
	}
	metaSize := 0
	for name, vs := range r.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") {
			v := strings.Join(vs, ",")
			h[lower] = v
			metaSize += len(lower) - len("x-amz-meta-") + len(v)
		}
	}
	if metaSize > maxMetadataSize {
		return nil, errMetadataTooLarge
	}
	return h, nil
}

func (s *Server) releaseAll(ids []nugget.ID) {
	for _, id := range ids {
		if err := s.vein.Release(id); err != nil {
			s.log.Error("release nugget", "nugget", id.String(), "err", err)
		}
	}
}

func (s *Server) putObject(req *request) (err error) {
	r := req.r
	headers, err := objectHeaders(r)
	if err != nil {
		return err
	}
	var md5Want []byte
	if v := r.Header.Get("Content-MD5"); v != "" {
		if md5Want, err = base64.StdEncoding.DecodeString(v); err != nil || len(md5Want) != md5.Size {
			return errInvalidDigest
		}
	}
	sum, aerr := requestChecksum(r)
	if aerr != nil {
		return aerr
	}
	body, err := s.payloadReader(req)
	if err != nil {
		return err
	}
	if body.size < 0 {
		return errMissingLength
	}
	if body.size > maxPutSize {
		return errEntityTooLarge
	}

	md5h := md5.New() // #nosec G401
	hashers := []hash.Hash{md5h}
	if sum != nil {
		hashers = append(hashers, sum.h)
	}

	// Nuggets written so far hold a reference on the vein; release them if
	// the object never commits.
	var stored []nugget.ID
	committed := false
	defer func() {
		if !committed {
			s.releaseAll(stored)
		}
	}()

	v := ledger.Version{ChunkSize: nugget.Size, Headers: headers}
	buf := make([]byte, nugget.Size)
	for v.Size < body.size {
		want := min(int64(len(buf)), body.size-v.Size)
		n, rerr := io.ReadFull(body, buf[:want])
		if rerr != nil {
			if errors.Is(rerr, io.ErrUnexpectedEOF) || errors.Is(rerr, io.EOF) {
				return errIncompleteBody
			}
			return rerr
		}
		data := buf[:n]
		for _, h := range hashers {
			h.Write(data)
		}
		id := nugget.Sum(data)
		if err := s.vein.Put(id, data); err != nil {
			return err
		}
		stored = append(stored, id)
		v.Nuggets = append(v.Nuggets, ledger.NuggetRef{ID: id, Length: int64(n)})
		v.Size += int64(n)
	}
	// Drain the stream terminator (and trailers) and make sure nothing extra follows.
	if extra, rerr := io.Copy(io.Discard, io.LimitReader(body, 1)); rerr != nil {
		return rerr
	} else if extra > 0 {
		return errf(http.StatusBadRequest, "IncompleteBody", "The request body is longer than the declared content length.")
	}
	if err := body.verify(); err != nil {
		return err
	}
	md5Sum := md5h.Sum(nil)
	if md5Want != nil && hex.EncodeToString(md5Want) != hex.EncodeToString(md5Sum) {
		return errBadDigest
	}
	if sum != nil {
		if sum.trailer {
			sum.expected = body.trailer(sum.header())
		}
		if sum.expected == "" {
			return errInvalidDigest
		}
		if sum.sum() != sum.expected {
			return errBadDigest
		}
		v.ChecksumAlgorithm, v.ChecksumValue = sum.algorithm, sum.expected
	}
	v.ETag = `"` + hex.EncodeToString(md5Sum) + `"`

	released, err := s.ledger.PutObject(r.Context(), req.bucket, req.key, v)
	if err != nil {
		return err
	}
	committed = true
	s.releaseAll(released)
	s.bytesIn.Add(float64(v.Size))
	s.emit("object.put", map[string]any{"bucket": req.bucket, "key": req.key, "size": v.Size, "nuggets": len(v.Nuggets)})

	req.w.Header().Set("ETag", v.ETag)
	if sum != nil {
		req.w.Header().Set(sum.header(), sum.expected)
	}
	req.w.WriteHeader(http.StatusOK)
	return nil
}

// byteRange is an inclusive range within an object.
type byteRange struct{ start, end int64 }

// parseRange parses a single-range Range header. ok is false when the header
// should be ignored (absent, malformed or multi-range), matching S3.
func parseRange(h string, size int64) (br byteRange, ok bool, err error) {
	spec, found := strings.CutPrefix(h, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return br, false, nil
	}
	a, b, found := strings.Cut(strings.TrimSpace(spec), "-")
	if !found {
		return br, false, nil
	}
	if a == "" {
		n, perr := strconv.ParseInt(b, 10, 64)
		if perr != nil || n < 0 {
			return br, false, nil
		}
		if n == 0 || size == 0 {
			return br, false, errInvalidRange
		}
		return byteRange{max(0, size-n), size - 1}, true, nil
	}
	start, perr := strconv.ParseInt(a, 10, 64)
	if perr != nil || start < 0 {
		return br, false, nil
	}
	end := size - 1
	if b != "" {
		if end, perr = strconv.ParseInt(b, 10, 64); perr != nil || end < start {
			return br, false, nil
		}
		end = min(end, size-1)
	}
	if start >= size {
		return br, false, errInvalidRange
	}
	return byteRange{start, end}, true, nil
}

func etagMatches(list, etag string) bool {
	for _, e := range strings.Split(list, ",") {
		e = strings.TrimSpace(e)
		if e == "*" || strings.Trim(e, `"`) == strings.Trim(etag, `"`) {
			return true
		}
	}
	return false
}

// checkConditions evaluates If-* headers. notModified reports a 304.
func checkConditions(r *http.Request, o *ledger.Object) (notModified bool, err error) {
	lm := o.LastModified.Truncate(time.Second)
	if v := r.Header.Get("If-Match"); v != "" {
		if !etagMatches(v, o.ETag) {
			return false, errPreconditionFailed
		}
	} else if t, perr := http.ParseTime(r.Header.Get("If-Unmodified-Since")); perr == nil && lm.After(t) {
		return false, errPreconditionFailed
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		return etagMatches(v, o.ETag), nil
	}
	if t, perr := http.ParseTime(r.Header.Get("If-Modified-Since")); perr == nil && !lm.After(t) {
		return true, nil
	}
	return false, nil
}

func (s *Server) getObject(req *request, head bool) error {
	r := req.r
	o, err := s.ledger.GetObject(r.Context(), req.bucket, req.key)
	if err != nil {
		return err
	}
	notModified, err := checkConditions(r, o)
	if err != nil {
		return err
	}

	h := req.w.Header()
	h.Set("ETag", o.ETag)
	h.Set("Last-Modified", o.LastModified.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	if notModified {
		req.w.WriteHeader(http.StatusNotModified)
		return nil
	}
	for k, v := range o.Headers {
		h.Set(k, v)
	}
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", defaultCT)
	}

	rng := byteRange{0, o.Size - 1}
	status := http.StatusOK
	if v := r.Header.Get("Range"); v != "" {
		br, ok, rerr := parseRange(v, o.Size)
		if rerr != nil {
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", o.Size))
			return rerr
		}
		if ok {
			rng, status = br, http.StatusPartialContent
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", br.start, br.end, o.Size))
		}
	}
	// The stored checksum covers the whole object, so (like S3) it is only
	// returned for full reads.
	if status == http.StatusOK && o.ChecksumAlgorithm != "" && strings.EqualFold(r.Header.Get("X-Amz-Checksum-Mode"), "ENABLED") {
		h.Set("x-amz-checksum-"+o.ChecksumAlgorithm, o.ChecksumValue)
	}
	length := rng.end - rng.start + 1
	h.Set("Content-Length", strconv.FormatInt(length, 10))
	if head || length == 0 {
		req.w.WriteHeader(status)
		return nil
	}
	return s.streamNuggets(req, o, rng, status)
}

// streamNuggets writes the bytes of rng. Every nugget is verified before any
// of its bytes are sent. A failure before the first byte becomes an S3 error;
// a failure mid-stream aborts the connection so the client sees a truncated
// response, never wrong bytes.
func (s *Server) streamNuggets(req *request, o *ledger.Object, rng byteRange, status int) error {
	var off int64
	started := false
	for _, n := range o.Nuggets {
		nStart, nEnd := off, off+n.Length-1
		off += n.Length
		if nEnd < rng.start {
			continue
		}
		if nStart > rng.end {
			break
		}
		data, err := s.vein.Get(n.ID)
		if err == nil && int64(len(data)) != n.Length {
			err = fmt.Errorf("nugget %s has length %d, manifest says %d", n.ID, len(data), n.Length)
		}
		if err != nil {
			s.emit("object.read_failed", map[string]any{"bucket": req.bucket, "key": req.key, "nugget": n.ID.String(), "error": err.Error()})
			if !started {
				// Drop the object's headers; the error response replaces them.
				for k := range req.w.Header() {
					if k != "X-Amz-Request-Id" && k != "Server" {
						req.w.Header().Del(k)
					}
				}
				return err
			}
			s.log.Error("aborting response mid-stream", "bucket", req.bucket, "key", req.key, "err", err, "request_id", req.id)
			panic(http.ErrAbortHandler)
		}
		lo := max(rng.start, nStart) - nStart
		hi := min(rng.end, nEnd) - nStart + 1
		if !started {
			req.w.WriteHeader(status)
			started = true
		}
		written, werr := req.w.Write(data[lo:hi])
		s.bytesOut.Add(float64(written))
		if werr != nil {
			return nil // client went away
		}
	}
	return nil
}

func (s *Server) deleteObject(req *request) error {
	released, err := s.ledger.DeleteObject(req.r.Context(), req.bucket, req.key)
	if err != nil {
		return err
	}
	s.releaseAll(released)
	if released != nil {
		s.emit("object.deleted", map[string]any{"bucket": req.bucket, "key": req.key})
	}
	req.w.WriteHeader(http.StatusNoContent)
	return nil
}

type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name       `xml:"DeleteResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Deleted []deletedXML   `xml:"Deleted"`
	Errors  []deleteErrXML `xml:"Error"`
}

type deletedXML struct {
	Key string `xml:"Key"`
}

type deleteErrXML struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func (s *Server) deleteObjects(req *request) error {
	b, err := s.readSmallBody(req)
	if err != nil {
		return err
	}
	var in deleteRequest
	if err := xml.Unmarshal(b, &in); err != nil || len(in.Objects) == 0 || len(in.Objects) > 1000 {
		return errMalformedXML
	}
	if _, err := s.ledger.HeadBucket(req.r.Context(), req.bucket); err != nil {
		return err
	}
	out := deleteResult{Xmlns: s3Namespace}
	for _, obj := range in.Objects {
		released, err := s.ledger.DeleteObject(req.r.Context(), req.bucket, obj.Key)
		if err != nil {
			ae := s.toAPIError(req, err)
			out.Errors = append(out.Errors, deleteErrXML{Key: obj.Key, Code: ae.Code, Message: ae.Message})
			continue
		}
		s.releaseAll(released)
		if released != nil {
			s.emit("object.deleted", map[string]any{"bucket": req.bucket, "key": obj.Key})
		}
		if !in.Quiet {
			out.Deleted = append(out.Deleted, deletedXML{Key: obj.Key})
		}
	}
	s.writeXML(req, http.StatusOK, out)
	return nil
}
