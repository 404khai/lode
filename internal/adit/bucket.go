package adit

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/404khai/lode/internal/ledger"
)

const maxXMLBody = 1 << 20

type listAllMyBucketsResult struct {
	XMLName xml.Name    `xml:"ListAllMyBucketsResult"`
	Xmlns   string      `xml:"xmlns,attr"`
	Owner   owner       `xml:"Owner"`
	Buckets []bucketXML `xml:"Buckets>Bucket"`
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

func s3Time(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

func (s *Server) owner(req *request) owner {
	return owner{ID: req.auth.AccessKey, DisplayName: req.auth.AccessKey}
}

// readSmallBody reads and verifies a small (XML) request body.
func (s *Server) readSmallBody(req *request) ([]byte, error) {
	body, err := s.payloadReader(req)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(body, maxXMLBody+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxXMLBody {
		return nil, errMalformedXML
	}
	if err := body.verify(); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *Server) listBuckets(req *request) error {
	bs, err := s.ledger.ListBuckets(req.r.Context())
	if err != nil {
		return err
	}
	out := listAllMyBucketsResult{Xmlns: s3Namespace, Owner: s.owner(req), Buckets: make([]bucketXML, len(bs))}
	for i, b := range bs {
		out.Buckets[i] = bucketXML{Name: b.Name, CreationDate: s3Time(b.CreatedAt)}
	}
	s.writeXML(req, http.StatusOK, out)
	return nil
}

func (s *Server) createBucket(req *request) error {
	if _, err := s.readSmallBody(req); err != nil {
		return err
	}
	if aerr := checkCannedACL(req.r); aerr != nil {
		return aerr
	}
	replication := 0
	if v := req.r.Header.Get("X-Lode-Replication"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 1 || n > 16 {
			return errf(http.StatusBadRequest, "InvalidArgument", "x-lode-replication must be between 1 and 16")
		}
		replication = n
	}
	err := s.ledger.CreateBucket(req.r.Context(), req.bucket, replication)
	// In us-east-1, S3 answers 200 when you re-create a bucket you own.
	if errors.Is(err, ledger.ErrBucketExists) && s.region == "us-east-1" {
		err = nil
	}
	if err != nil {
		return err
	}
	s.emit("bucket.created", map[string]any{"bucket": req.bucket})
	req.w.Header().Set("Location", "/"+req.bucket)
	req.w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) deleteBucket(req *request) error {
	if err := s.ledger.DeleteBucket(req.r.Context(), req.bucket); err != nil {
		return err
	}
	s.emit("bucket.deleted", map[string]any{"bucket": req.bucket})
	req.w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) headBucket(req *request) error {
	if _, err := s.ledger.HeadBucket(req.r.Context(), req.bucket); err != nil {
		return err
	}
	req.w.Header().Set("x-amz-bucket-region", s.region)
	req.w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) getBucketLocation(req *request) error {
	if _, err := s.ledger.HeadBucket(req.r.Context(), req.bucket); err != nil {
		return err
	}
	type locationConstraint struct {
		XMLName xml.Name `xml:"LocationConstraint"`
		Xmlns   string   `xml:"xmlns,attr"`
		Region  string   `xml:",chardata"`
	}
	region := s.region
	if region == "us-east-1" {
		region = ""
	}
	s.writeXML(req, http.StatusOK, locationConstraint{Xmlns: s3Namespace, Region: region})
	return nil
}
