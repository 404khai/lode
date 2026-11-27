package adit

import (
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
)

type initiateResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

func (s *Server) createUpload(req *request) error {
	headers, err := objectHeaders(req.r)
	if err != nil {
		return err
	}
	algo := strings.ToLower(req.r.Header.Get("X-Amz-Checksum-Algorithm"))
	if _, ok := checksumAlgorithms[algo]; algo != "" && !ok {
		return errf(http.StatusBadRequest, "InvalidRequest", "Checksum algorithm %q is not supported", algo)
	}
	id, err := s.ledger.CreateUpload(req.r.Context(), req.bucket, req.key, headers, algo)
	if err != nil {
		return err
	}
	s.emit("upload.created", map[string]any{"bucket": req.bucket, "key": req.key, "upload_id": id})
	if algo != "" {
		req.w.Header().Set("x-amz-checksum-algorithm", strings.ToUpper(algo))
	}
	s.writeXML(req, http.StatusOK, initiateResult{Xmlns: s3Namespace, Bucket: req.bucket, Key: req.key, UploadID: id})
	return nil
}

func parsePartNumber(v string) (int, error) {
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 10000 {
		return 0, errf(http.StatusBadRequest, "InvalidArgument", "Part number must be an integer between 1 and 10000, inclusive")
	}
	return n, nil
}

func (s *Server) uploadPart(req *request, uploadID string) error {
	n, err := parsePartNumber(req.r.URL.Query().Get("partNumber"))
	if err != nil {
		return err
	}
	// Fail fast before reading the body.
	if _, err := s.ledger.GetUpload(req.r.Context(), req.bucket, req.key, uploadID); err != nil {
		return err
	}
	in, err := s.ingest(req, maxPutSize)
	if err != nil {
		return err
	}
	p := ledger.Part{
		Number: n, Size: in.size, ETag: `"` + hex.EncodeToString(in.md5) + `"`,
		ChunkSize: nugget.AvgSize, Nuggets: in.nuggets,
	}
	if in.sum != nil {
		p.ChecksumValue = in.sum.expected
	}
	if err := s.ledger.PutPart(req.r.Context(), req.bucket, req.key, uploadID, p); err != nil {
		return err
	}
	req.w.Header().Set("ETag", p.ETag)
	if in.sum != nil {
		req.w.Header().Set(in.sum.header(), in.sum.expected)
	}
	req.w.WriteHeader(http.StatusOK)
	return nil
}

type completeRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type completeResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

func (s *Server) completeUpload(req *request, uploadID string) error {
	body, err := s.readSmallBody(req)
	if err != nil {
		return err
	}
	var in completeRequest
	if err := xml.Unmarshal(body, &in); err != nil {
		return errMalformedXML
	}
	if len(in.Parts) == 0 {
		return errf(http.StatusBadRequest, "MalformedXML", "You must specify at least one part")
	}
	parts := make([]ledger.CompletePart, len(in.Parts))
	for i, p := range in.Parts {
		parts[i] = ledger.CompletePart{Number: p.PartNumber, ETag: p.ETag}
	}
	etag, res, err := s.ledger.CompleteUpload(req.r.Context(), req.bucket, req.key, uploadID, parts, writeCondition(req.r))
	if err != nil {
		return err
	}
	s.emit("upload.completed", map[string]any{"bucket": req.bucket, "key": req.key, "upload_id": uploadID, "parts": len(parts), "version_id": res.VersionID})
	setWriteVersionHeader(req.w.Header(), res.Versioning, res.VersionID)
	s.writeXML(req, http.StatusOK, completeResult{
		Xmlns: s3Namespace, Location: "/" + req.bucket + "/" + uriEncode(req.key, false),
		Bucket: req.bucket, Key: req.key, ETag: etag,
	})
	return nil
}

func (s *Server) abortUpload(req *request, uploadID string) error {
	if err := s.ledger.AbortUpload(req.r.Context(), req.bucket, req.key, uploadID); err != nil {
		return err
	}
	s.emit("upload.aborted", map[string]any{"bucket": req.bucket, "key": req.key, "upload_id": uploadID})
	req.w.WriteHeader(http.StatusNoContent)
	return nil
}

type partXML struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

type listPartsResult struct {
	XMLName              xml.Name  `xml:"ListPartsResult"`
	Xmlns                string    `xml:"xmlns,attr"`
	Bucket               string    `xml:"Bucket"`
	Key                  string    `xml:"Key"`
	UploadID             string    `xml:"UploadId"`
	Initiator            owner     `xml:"Initiator"`
	Owner                owner     `xml:"Owner"`
	StorageClass         string    `xml:"StorageClass"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker"`
	MaxParts             int       `xml:"MaxParts"`
	IsTruncated          bool      `xml:"IsTruncated"`
	Parts                []partXML `xml:"Part"`
}

func parseMax(v string, def int, name string) (int, error) {
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, errf(http.StatusBadRequest, "InvalidArgument", "Provided %s not an integer or within integer range", name)
	}
	return min(n, def), nil
}

func (s *Server) listParts(req *request, uploadID string) error {
	q := req.r.URL.Query()
	maxParts, err := parseMax(q.Get("max-parts"), 1000, "max-parts")
	if err != nil {
		return err
	}
	marker := 0
	if v := q.Get("part-number-marker"); v != "" {
		if marker, err = strconv.Atoi(v); err != nil || marker < 0 {
			return errf(http.StatusBadRequest, "InvalidArgument", "Provided part-number-marker not an integer or within integer range")
		}
	}
	_, parts, err := s.ledger.ListParts(req.r.Context(), req.bucket, req.key, uploadID, marker, maxParts+1)
	if err != nil {
		return err
	}
	out := listPartsResult{
		Xmlns: s3Namespace, Bucket: req.bucket, Key: req.key, UploadID: uploadID,
		Initiator: s.owner(req), Owner: s.owner(req), StorageClass: "STANDARD",
		PartNumberMarker: marker, MaxParts: maxParts,
	}
	if len(parts) > maxParts {
		parts, out.IsTruncated = parts[:maxParts], true
	}
	for _, p := range parts {
		out.Parts = append(out.Parts, partXML{PartNumber: p.Number, LastModified: s3Time(p.LastModified), ETag: p.ETag, Size: p.Size})
		out.NextPartNumberMarker = p.Number
	}
	s.writeXML(req, http.StatusOK, out)
	return nil
}

type uploadXML struct {
	Key          string `xml:"Key"`
	UploadID     string `xml:"UploadId"`
	Initiator    owner  `xml:"Initiator"`
	Owner        owner  `xml:"Owner"`
	StorageClass string `xml:"StorageClass"`
	Initiated    string `xml:"Initiated"`
}

type listUploadsResult struct {
	XMLName            xml.Name       `xml:"ListMultipartUploadsResult"`
	Xmlns              string         `xml:"xmlns,attr"`
	Bucket             string         `xml:"Bucket"`
	KeyMarker          string         `xml:"KeyMarker"`
	UploadIDMarker     string         `xml:"UploadIdMarker"`
	NextKeyMarker      string         `xml:"NextKeyMarker"`
	NextUploadIDMarker string         `xml:"NextUploadIdMarker"`
	Prefix             string         `xml:"Prefix"`
	Delimiter          string         `xml:"Delimiter,omitempty"`
	MaxUploads         int            `xml:"MaxUploads"`
	IsTruncated        bool           `xml:"IsTruncated"`
	Uploads            []uploadXML    `xml:"Upload"`
	CommonPrefixes     []commonPrefix `xml:"CommonPrefixes"`
}

func (s *Server) listUploads(req *request) error {
	q := req.r.URL.Query()
	maxUploads, err := parseMax(q.Get("max-uploads"), 1000, "max-uploads")
	if err != nil {
		return err
	}
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	out := listUploadsResult{
		Xmlns: s3Namespace, Bucket: req.bucket, KeyMarker: q.Get("key-marker"), UploadIDMarker: q.Get("upload-id-marker"),
		Prefix: prefix, Delimiter: delim, MaxUploads: maxUploads,
	}
	ups, err := s.ledger.ListUploads(req.r.Context(), req.bucket, prefix, out.KeyMarker, out.UploadIDMarker, 10000)
	if err != nil {
		return err
	}
	var lastCP string
	count := 0
	for _, u := range ups {
		if delim != "" {
			if i := strings.Index(u.Key[len(prefix):], delim); i >= 0 {
				cp := u.Key[:len(prefix)+i+len(delim)]
				if cp == lastCP {
					continue
				}
				if count == maxUploads {
					out.IsTruncated = true
					break
				}
				lastCP = cp
				out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix{Prefix: cp})
				out.NextKeyMarker, out.NextUploadIDMarker = cp, ""
				count++
				continue
			}
		}
		if count == maxUploads {
			out.IsTruncated = true
			break
		}
		owner := s.owner(req)
		out.Uploads = append(out.Uploads, uploadXML{
			Key: u.Key, UploadID: u.UploadID, Initiator: owner, Owner: owner, StorageClass: "STANDARD", Initiated: s3Time(u.Initiated),
		})
		out.NextKeyMarker, out.NextUploadIDMarker = u.Key, u.UploadID
		count++
	}
	if !out.IsTruncated {
		out.NextKeyMarker, out.NextUploadIDMarker = "", ""
	}
	s.writeXML(req, http.StatusOK, out)
	return nil
}
