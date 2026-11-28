package adit

import (
	"context"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
)

const maxCopySize = 5 << 30

// parseCopySource parses x-amz-copy-source: "[/]bucket/key[?versionId=...]".
func parseCopySource(v string) (ledger.CopySource, *apiError) {
	var src ledger.CopySource
	path, query, _ := strings.Cut(v, "?")
	p, err := url.PathUnescape(strings.TrimPrefix(path, "/"))
	if err != nil {
		return src, errf(http.StatusBadRequest, "InvalidArgument", "Invalid copy source encoding")
	}
	b, k, ok := strings.Cut(p, "/")
	if !ok || b == "" || k == "" {
		return src, errf(http.StatusBadRequest, "InvalidArgument", "Copy Source must mention the source bucket and key: sourcebucket/sourcekey")
	}
	src.Bucket, src.Key = b, k
	if query != "" {
		q, err := url.ParseQuery(query)
		if err != nil {
			return src, errf(http.StatusBadRequest, "InvalidArgument", "Invalid copy source")
		}
		src.VersionID = q.Get("versionId")
	}
	return src, nil
}

// checkCopyConditions evaluates x-amz-copy-source-if-* against the source.
func checkCopyConditions(r *http.Request, o *ledger.Object) error {
	lm := o.LastModified.Truncate(time.Second)
	if v := r.Header.Get("X-Amz-Copy-Source-If-Match"); v != "" && !etagMatches(v, o.ETag) {
		return errPreconditionFailed
	}
	if v := r.Header.Get("X-Amz-Copy-Source-If-None-Match"); v != "" && etagMatches(v, o.ETag) {
		return errPreconditionFailed
	}
	if t, err := http.ParseTime(r.Header.Get("X-Amz-Copy-Source-If-Unmodified-Since")); err == nil && lm.After(t) {
		return errPreconditionFailed
	}
	if t, err := http.ParseTime(r.Header.Get("X-Amz-Copy-Source-If-Modified-Since")); err == nil && !lm.After(t) {
		return errPreconditionFailed
	}
	return nil
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

func (s *Server) sourceObject(req *request, src ledger.CopySource) (*ledger.Object, error) {
	o, err := s.ledger.GetObject(req.r.Context(), src.Bucket, src.Key, src.VersionID)
	if o != nil && o.IsDeleteMarker {
		if src.VersionID != "" {
			return nil, errf(http.StatusBadRequest, "InvalidRequest", "The source of a copy request may not specifically refer to a delete marker by version id.")
		}
		return nil, errNoSuchKey
	}
	if err != nil {
		return nil, err
	}
	return o, checkCopyConditions(req.r, o)
}

// copyObject writes a new version that shares the source's nuggets.
func (s *Server) copyObject(req *request) error {
	r := req.r
	src, aerr := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if aerr != nil {
		return aerr
	}
	o, err := s.sourceObject(req, src)
	if err != nil {
		return err
	}
	if o.Size > maxCopySize {
		return errf(http.StatusBadRequest, "InvalidRequest", "The specified copy source is larger than the maximum allowable size for a copy source: 5368709120")
	}
	var headers, tags map[string]string
	switch strings.ToUpper(r.Header.Get("X-Amz-Metadata-Directive")) {
	case "", "COPY":
	case "REPLACE":
		if headers, err = objectHeaders(r); err != nil {
			return err
		}
	default:
		return errf(http.StatusBadRequest, "InvalidArgument", "Unknown metadata directive.")
	}
	switch strings.ToUpper(r.Header.Get("X-Amz-Tagging-Directive")) {
	case "", "COPY":
	case "REPLACE":
		var aerr *apiError
		if tags, aerr = parseTagHeader(r.Header.Get("X-Amz-Tagging"), maxObjectTags); aerr != nil {
			return aerr
		}
	default:
		return errf(http.StatusBadRequest, "InvalidArgument", "Unknown tagging directive.")
	}
	if src.Bucket == req.bucket && src.Key == req.key && src.VersionID == "" && headers == nil && tags == nil {
		b, err := s.ledger.HeadBucket(r.Context(), req.bucket)
		if err != nil {
			return err
		}
		if b.Versioning != ledger.VersioningEnabled {
			return errf(http.StatusBadRequest, "InvalidRequest", "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.")
		}
	}
	src.VersionID = o.VersionID // copy exactly the version we checked conditions against
	copied, res, err := s.ledger.CopyObject(r.Context(), src, req.bucket, req.key, headers, tags, writeCondition(r))
	if err != nil {
		return err
	}
	s.emit("object.copied", map[string]any{"from_bucket": src.Bucket, "from_key": src.Key, "bucket": req.bucket,
		"key": req.key, "size": copied.Size, "version_id": res.VersionID})
	if o.Versioning != "" {
		req.w.Header().Set("x-amz-copy-source-version-id", copied.VersionID)
	}
	setWriteVersionHeader(req.w.Header(), res.Versioning, res.VersionID)
	s.writeXML(req, http.StatusOK, copyObjectResult{Xmlns: s3Namespace, ETag: copied.ETag, LastModified: s3Time(time.Now())})
	return nil
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// uploadPartCopy stores a byte range of an existing object as a part. The
// range is re-chunked, so whole-nugget overlaps dedup to no new writes.
func (s *Server) uploadPartCopy(req *request, uploadID string) error {
	r := req.r
	n, err := parsePartNumber(r.URL.Query().Get("partNumber"))
	if err != nil {
		return err
	}
	src, aerr := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if aerr != nil {
		return aerr
	}
	up, err := s.ledger.GetUpload(r.Context(), req.bucket, req.key, uploadID)
	if err != nil {
		return err
	}
	o, err := s.sourceObject(req, src)
	if err != nil {
		return err
	}
	rng := byteRange{0, o.Size - 1}
	if v := r.Header.Get("X-Amz-Copy-Source-Range"); v != "" {
		spec, ok := strings.CutPrefix(v, "bytes=")
		a, b, ok2 := strings.Cut(spec, "-")
		start, e1 := strconv.ParseInt(a, 10, 64)
		end, e2 := strconv.ParseInt(b, 10, 64)
		if !ok || !ok2 || e1 != nil || e2 != nil || start > end {
			return errf(http.StatusBadRequest, "InvalidArgument", "The x-amz-copy-source-range value must be of the form bytes=first-last where first and last are the zero-based offsets of the first and last bytes to copy")
		}
		if end >= o.Size {
			return errf(http.StatusBadRequest, "InvalidRange", "The requested range is not satisfiable")
		}
		rng = byteRange{start, end}
	}
	if rng.end-rng.start+1 > maxCopySize {
		return errEntityTooLarge
	}
	bucket, err := s.ledger.HeadBucket(r.Context(), req.bucket)
	if err != nil {
		return err
	}
	var sum *checksum
	if up.ChecksumAlgorithm != "" {
		sum = &checksum{algorithm: up.ChecksumAlgorithm, h: checksumAlgorithms[up.ChecksumAlgorithm]()}
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	in, err := s.storeStream(ctx, s.objectReader(ctx, o, rng), bucket, sum)
	if err != nil {
		return err
	}
	p := ledger.Part{Number: n, Size: in.size, ETag: `"` + hex.EncodeToString(in.md5) + `"`, ChunkSize: nugget.AvgSize, Nuggets: in.nuggets}
	if sum != nil {
		p.ChecksumValue = sum.sum()
	}
	if err := s.ledger.PutPart(r.Context(), req.bucket, req.key, uploadID, p); err != nil {
		return err
	}
	if o.Versioning != "" {
		req.w.Header().Set("x-amz-copy-source-version-id", o.VersionID)
	}
	s.writeXML(req, http.StatusOK, copyPartResult{Xmlns: s3Namespace, ETag: p.ETag, LastModified: s3Time(time.Now())})
	return nil
}

// objectReader streams rng of o, reading every nugget verified from the cluster.
func (s *Server) objectReader(ctx context.Context, o *ledger.Object, rng byteRange) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		var off int64
		for _, n := range o.Nuggets {
			nStart, nEnd := off, off+n.Length-1
			off += n.Length
			if nEnd < rng.start {
				continue
			}
			if nStart > rng.end {
				break
			}
			data, err := s.cluster.Read(ctx, n)
			if err == nil && int64(len(data)) != n.Length {
				err = fmt.Errorf("nugget %s has length %d, manifest says %d", n.ID, len(data), n.Length)
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			lo := max(rng.start, nStart) - nStart
			hi := min(rng.end, nEnd) - nStart + 1
			if _, err := pw.Write(data[lo:hi]); err != nil {
				return
			}
		}
		_ = pw.Close()
	}()
	return pr
}
