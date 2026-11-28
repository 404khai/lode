package adit

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/404khai/lode/internal/ec"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/survey"
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
	cfg, aerr := bucketConfig(req.r)
	if aerr != nil {
		return aerr
	}
	err := s.ledger.CreateBucket(req.r.Context(), req.bucket, cfg)
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

// bucketConfig reads Lode's bucket options from CreateBucket headers:
//
//	x-lode-replication: N          copies per nugget (replicated class)
//	x-lode-storage-class: ec:K+M   Reed-Solomon with K data and M parity shards
//	x-lode-placement: hrw | zone-spread | capacity-weighted | pinned:v1,v2,...
func bucketConfig(r *http.Request) (ledger.BucketConfig, *apiError) {
	var cfg ledger.BucketConfig
	bad := func(format string, args ...any) *apiError {
		return errf(http.StatusBadRequest, "InvalidArgument", format, args...)
	}
	if v := r.Header.Get("X-Lode-Replication"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 16 {
			return cfg, bad("x-lode-replication must be between 1 and 16")
		}
		cfg.Replication = n
	}
	if v := strings.ToLower(r.Header.Get("X-Lode-Storage-Class")); v != "" && v != "replicated" {
		spec, ok := strings.CutPrefix(v, "ec:")
		ks, ms, ok2 := strings.Cut(spec, "+")
		k, e1 := strconv.Atoi(ks)
		m, e2 := strconv.Atoi(ms)
		if !ok || !ok2 || e1 != nil || e2 != nil || ec.Validate(k, m) != nil {
			return cfg, bad("x-lode-storage-class must be 'replicated' or 'ec:K+M' with K,M >= 1 and K+M <= %d", ec.MaxShards)
		}
		if cfg.Replication > 0 {
			return cfg, bad("x-lode-replication and an erasure-coded storage class are mutually exclusive")
		}
		cfg.ECData, cfg.ECParity = k, m
	}
	if v := r.Header.Get("X-Lode-Placement"); v != "" {
		name, list, _ := strings.Cut(v, ":")
		if list != "" {
			for _, p := range strings.Split(list, ",") {
				if p = strings.TrimSpace(p); p != "" {
					cfg.Pinned = append(cfg.Pinned, p)
				}
			}
		}
		if _, err := survey.ByName(name, cfg.Pinned); err != nil {
			return cfg, bad("%s", err.Error())
		}
		cfg.Placement = name
	}
	return cfg, nil
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
