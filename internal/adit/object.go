package adit

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
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

// setVersionHeader adds x-amz-version-id for buckets that have ever had
// versioning enabled.
func setVersionHeader(h http.Header, versioning, versionID string) {
	if versioning != "" && versionID != "" {
		h.Set("x-amz-version-id", versionID)
	}
}

// setWriteVersionHeader is setVersionHeader for write responses, which (like
// S3) omit the "null" version ID.
func setWriteVersionHeader(h http.Header, versioning, versionID string) {
	if versionID != ledger.NullVersion {
		setVersionHeader(h, versioning, versionID)
	}
}

func writeCondition(r *http.Request) ledger.Condition {
	return ledger.Condition{IfMatch: r.Header.Get("If-Match"), IfNoneMatch: r.Header.Get("If-None-Match")}
}

func (s *Server) putObject(req *request) error {
	headers, err := objectHeaders(req.r)
	if err != nil {
		return err
	}
	if aerr := checkCannedACL(req.r); aerr != nil {
		return aerr
	}
	var tags map[string]string
	if v := req.r.Header.Get("X-Amz-Tagging"); v != "" {
		var aerr *apiError
		if tags, aerr = parseTagHeader(v, maxObjectTags); aerr != nil {
			return aerr
		}
	}
	bucket, err := s.ledger.HeadBucket(req.r.Context(), req.bucket)
	if err != nil {
		return err
	}
	in, err := s.ingest(req, maxPutSize, bucket, "")
	if err != nil {
		return err
	}
	v := ledger.Version{
		Size: in.size, ETag: `"` + hex.EncodeToString(in.md5) + `"`, Headers: headers,
		ChunkSize: nugget.AvgSize, Nuggets: in.nuggets, Tags: tags,
	}
	if in.sum != nil {
		v.ChecksumAlgorithm, v.ChecksumValue = in.sum.algorithm, in.sum.expected
	}
	res, err := s.ledger.PutObject(req.r.Context(), req.bucket, req.key, v, writeCondition(req.r))
	if err != nil {
		return err
	}
	s.emit("object.put", map[string]any{"bucket": req.bucket, "key": req.key, "size": v.Size, "nuggets": len(v.Nuggets), "version_id": res.VersionID})

	req.w.Header().Set("ETag", v.ETag)
	setWriteVersionHeader(req.w.Header(), res.Versioning, res.VersionID)
	if in.sum != nil {
		req.w.Header().Set(in.sum.header(), in.sum.expected)
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
	q := r.URL.Query()
	partNumber := 0
	if v := q.Get("partNumber"); v != "" {
		n, err := parsePartNumber(v)
		if err != nil {
			return err
		}
		partNumber = n
	}
	versionID := q.Get("versionId")
	o, err := s.ledger.GetObject(r.Context(), req.bucket, req.key, versionID)
	h := req.w.Header()
	if o != nil && o.IsDeleteMarker {
		h.Set("x-amz-delete-marker", "true")
		setVersionHeader(h, o.Versioning, o.VersionID)
		if versionID != "" {
			h.Set("Last-Modified", o.LastModified.UTC().Format(http.TimeFormat))
			return errMethodNotAllowed
		}
		return errNoSuchKey
	}
	if err != nil {
		return err
	}
	notModified, err := checkConditions(r, o)
	if err != nil {
		return err
	}

	h.Set("ETag", o.ETag)
	h.Set("Last-Modified", o.LastModified.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	setVersionHeader(h, o.Versioning, o.VersionID)
	if notModified {
		req.w.WriteHeader(http.StatusNotModified)
		return nil
	}
	for k, v := range o.Headers {
		if strings.HasPrefix(k, "x-amz-meta-") {
			h[k] = []string{v} // S3 returns user metadata names in lower case
		} else {
			h.Set(k, v)
		}
	}
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", defaultCT)
	}
	if o.PartsCount > 0 {
		h.Set("x-amz-mp-parts-count", strconv.Itoa(o.PartsCount))
	}

	rng := byteRange{0, o.Size - 1}
	status := http.StatusOK
	if partNumber > 0 {
		sizes := o.PartSizes
		if len(sizes) == 0 {
			sizes = []int64{o.Size} // a plain object is a single part
		}
		if partNumber > len(sizes) {
			return errInvalidPart
		}
		var start int64
		for _, sz := range sizes[:partNumber-1] {
			start += sz
		}
		rng, status = byteRange{start, start + sizes[partNumber-1] - 1}, http.StatusPartialContent
		h.Set("x-amz-mp-parts-count", strconv.Itoa(len(sizes)))
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", rng.start, rng.end, o.Size))
	} else if v := r.Header.Get("Range"); v != "" {
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
		if o.ChecksumType != "" {
			h.Set("x-amz-checksum-type", o.ChecksumType)
		} else {
			h.Set("x-amz-checksum-type", checksumFullObject)
		}
	}
	setTagCount(h, o.Tags)
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
		data, err := s.cluster.Read(req.r.Context(), n)
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
	res, err := s.ledger.DeleteObject(req.r.Context(), req.bucket, req.key, req.r.URL.Query().Get("versionId"))
	if err != nil {
		return err
	}
	if res.VersionID != "" {
		s.emit("object.deleted", map[string]any{"bucket": req.bucket, "key": req.key, "version_id": res.VersionID, "delete_marker": res.DeleteMarker})
	}
	setVersionHeader(req.w.Header(), res.Versioning, res.VersionID)
	if res.DeleteMarker {
		req.w.Header().Set("x-amz-delete-marker", "true")
	}
	req.w.WriteHeader(http.StatusNoContent)
	return nil
}

type deleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name       `xml:"DeleteResult"`
	Xmlns   string         `xml:"xmlns,attr"`
	Deleted []deletedXML   `xml:"Deleted"`
	Errors  []deleteErrXML `xml:"Error"`
}

type deletedXML struct {
	Key                   string `xml:"Key"`
	VersionID             string `xml:"VersionId,omitempty"`
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
}

type deleteErrXML struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
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
		res, err := s.ledger.DeleteObject(req.r.Context(), req.bucket, obj.Key, obj.VersionID)
		if err != nil {
			ae := s.toAPIError(req, err)
			out.Errors = append(out.Errors, deleteErrXML{Key: obj.Key, VersionID: obj.VersionID, Code: ae.Code, Message: ae.Message})
			continue
		}
		if res.VersionID != "" {
			s.emit("object.deleted", map[string]any{"bucket": req.bucket, "key": obj.Key, "version_id": res.VersionID, "delete_marker": res.DeleteMarker})
		}
		if in.Quiet {
			continue
		}
		d := deletedXML{Key: obj.Key, VersionID: obj.VersionID}
		if res.DeleteMarker {
			d.DeleteMarker = true
			if obj.VersionID == "" {
				d.DeleteMarkerVersionID = res.VersionID
			}
		}
		out.Deleted = append(out.Deleted, d)
	}
	s.writeXML(req, http.StatusOK, out)
	return nil
}

type versioningConfiguration struct {
	XMLName   xml.Name `xml:"VersioningConfiguration"`
	Xmlns     string   `xml:"xmlns,attr,omitempty"`
	Status    string   `xml:"Status,omitempty"`
	MFADelete string   `xml:"MfaDelete,omitempty"`
}

func (s *Server) getBucketVersioning(req *request) error {
	b, err := s.ledger.HeadBucket(req.r.Context(), req.bucket)
	if err != nil {
		return err
	}
	s.writeXML(req, http.StatusOK, versioningConfiguration{Xmlns: s3Namespace, Status: b.Versioning})
	return nil
}

func (s *Server) putBucketVersioning(req *request) error {
	body, err := s.readSmallBody(req)
	if err != nil {
		return err
	}
	var in versioningConfiguration
	if err := xml.Unmarshal(body, &in); err != nil {
		return errMalformedXML
	}
	if in.Status != ledger.VersioningEnabled && in.Status != ledger.VersioningSuspended {
		return errf(http.StatusBadRequest, "IllegalVersioningConfigurationException", "The Versioning element must be specified")
	}
	if in.MFADelete == "Enabled" {
		return errNotImplemented
	}
	if err := s.ledger.SetVersioning(req.r.Context(), req.bucket, in.Status); err != nil {
		return err
	}
	s.emit("bucket.versioning", map[string]any{"bucket": req.bucket, "status": in.Status})
	req.w.WriteHeader(http.StatusOK)
	return nil
}
