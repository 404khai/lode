// Package adit implements the S3-compatible HTTP gateway.
package adit

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/survey"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

// Options configure a Server.
type Options struct {
	Logger      *slog.Logger
	Registerer  prometheus.Registerer
	Ledger      *ledger.Ledger
	Cluster     *cluster.Client
	Events      *events.Bus
	Replication int    // default replicas per nugget
	WriteQuorum int    // replicas that must be durable before a write succeeds; 0 = majority
	Placement   string // default Survey strategy
	Region      string
	Credentials map[string]string // access key -> secret key
	Now         func() time.Time
}

// Server is the Adit HTTP handler.
type Server struct {
	log         *slog.Logger
	ledger      *ledger.Ledger
	cluster     *cluster.Client
	bus         *events.Bus
	replication int
	quorum      int
	placement   string
	region      string
	credentials map[string]string
	now         func() time.Time

	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	bytesIn  prometheus.Counter
	bytesOut prometheus.Counter
}

// New builds a Server registering its metrics on opts.Registerer.
func New(opts Options) *Server {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if opts.Replication <= 0 {
		opts.Replication = 3
	}
	f := promauto.With(opts.Registerer)
	return &Server{
		log:         opts.Logger,
		ledger:      opts.Ledger,
		cluster:     opts.Cluster,
		bus:         opts.Events,
		replication: opts.Replication,
		quorum:      opts.WriteQuorum,
		placement:   opts.Placement,
		region:      opts.Region,
		credentials: opts.Credentials,
		now:         opts.Now,
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "lode", Subsystem: "adit", Name: "requests_total",
			Help: "S3 requests handled, by operation and status code.",
		}, []string{"op", "code"}),
		latency: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "lode", Subsystem: "adit", Name: "request_duration_seconds",
			Help: "S3 request latency by operation.", Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
		bytesIn: f.NewCounter(prometheus.CounterOpts{
			Namespace: "lode", Subsystem: "adit", Name: "bytes_received_total", Help: "Object bytes accepted.",
		}),
		bytesOut: f.NewCounter(prometheus.CounterOpts{
			Namespace: "lode", Subsystem: "adit", Name: "bytes_sent_total", Help: "Object bytes served.",
		}),
	}
}

// request carries per-request state through a handler.
type request struct {
	w      *statusWriter
	r      *http.Request
	id     string
	op     string
	bucket string
	key    string
	auth   *authContext
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

func newRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func (s *Server) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	start := s.now()
	req := &request{w: &statusWriter{ResponseWriter: rw}, r: r, id: newRequestID(), op: "Unknown"}
	req.w.Header().Set("x-amz-request-id", req.id)
	req.w.Header().Set("Server", "Lode")

	defer func() {
		code := req.w.code
		if code == 0 {
			code = http.StatusOK
		}
		s.requests.WithLabelValues(req.op, strconv.Itoa(code)).Inc()
		s.latency.WithLabelValues(req.op).Observe(s.now().Sub(start).Seconds())
		s.log.Debug("request", "op", req.op, "method", r.Method, "bucket", req.bucket, "key", req.key, "code", code, "request_id", req.id)
	}()

	path := strings.TrimPrefix(r.URL.Path, "/")
	req.bucket, req.key, _ = strings.Cut(path, "/")

	auth, aerr := s.authenticate(r)
	if aerr != nil {
		s.writeError(req, aerr)
		return
	}
	req.auth = auth

	if err := s.route(req); err != nil {
		s.writeError(req, err)
	}
}

// subresources Adit does not implement yet; requests for them get 501
// rather than being mistaken for plain object or bucket operations.
var unsupportedSubresources = []string{
	"cors", "encryption", "logging", "notification", "object-lock", "ownershipControls",
	"policy", "policyStatus", "publicAccessBlock", "replication", "requestPayment", "restore", "retention",
	"legal-hold", "select", "torrent", "website",
	"accelerate", "analytics", "intelligent-tiering", "inventory", "metrics", "attributes",
}

func (s *Server) route(req *request) error {
	r := req.r
	q := r.URL.Query()
	for _, sub := range unsupportedSubresources {
		if q.Has(sub) {
			req.op = "Unsupported"
			return errNotImplemented
		}
	}
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		if aerr := rejectSSE(r); aerr != nil {
			req.op = "Unsupported"
			return aerr
		}
	}
	if r.Header.Get("X-Amz-Server-Side-Encryption-Customer-Algorithm") != "" {
		req.op = "Unsupported"
		return errf(http.StatusNotImplemented, "NotImplemented", "Server-side encryption is not supported by Lode yet.")
	}
	if req.bucket != "" && req.key == "" && q.Has("lifecycle") {
		return s.lifecycle(req)
	}
	if req.bucket != "" && (q.Has("tagging") || q.Has("acl")) {
		if req.key == "" {
			if q.Has("tagging") {
				return s.bucketTagging(req)
			}
			return s.acl(req, false)
		}
		if q.Has("tagging") {
			return s.objectTagging(req)
		}
		return s.acl(req, true)
	}

	switch {
	case req.bucket == "":
		if r.Method != http.MethodGet {
			return errMethodNotAllowed
		}
		req.op = "ListBuckets"
		return s.listBuckets(req)

	case req.key == "":
		if !validBucketName(req.bucket) && r.Method == http.MethodPut {
			return errInvalidBucketName
		}
		switch r.Method {
		case http.MethodPut:
			if q.Has("versioning") {
				req.op = "PutBucketVersioning"
				return s.putBucketVersioning(req)
			}
			req.op = "CreateBucket"
			return s.createBucket(req)
		case http.MethodDelete:
			req.op = "DeleteBucket"
			return s.deleteBucket(req)
		case http.MethodHead:
			req.op = "HeadBucket"
			return s.headBucket(req)
		case http.MethodGet:
			switch {
			case q.Has("location"):
				req.op = "GetBucketLocation"
				return s.getBucketLocation(req)
			case q.Has("versioning"):
				req.op = "GetBucketVersioning"
				return s.getBucketVersioning(req)
			case q.Has("versions"):
				req.op = "ListObjectVersions"
				return s.listObjectVersions(req)
			case q.Has("uploads"):
				req.op = "ListMultipartUploads"
				return s.listUploads(req)
			case q.Get("list-type") == "2":
				req.op = "ListObjectsV2"
				return s.listObjects(req, true)
			default:
				req.op = "ListObjects"
				return s.listObjects(req, false)
			}
		case http.MethodPost:
			if q.Has("delete") {
				req.op = "DeleteObjects"
				return s.deleteObjects(req)
			}
		}
		return errMethodNotAllowed

	default:
		if len(req.key) > 1024 {
			return errKeyTooLong
		}
		uploadID := q.Get("uploadId")
		switch r.Method {
		case http.MethodPost:
			switch {
			case q.Has("uploads"):
				req.op = "CreateMultipartUpload"
				return s.createUpload(req)
			case uploadID != "":
				req.op = "CompleteMultipartUpload"
				return s.completeUpload(req, uploadID)
			}
		case http.MethodPut:
			copySource := r.Header.Get("X-Amz-Copy-Source") != ""
			switch {
			case uploadID != "" && copySource:
				req.op = "UploadPartCopy"
				return s.uploadPartCopy(req, uploadID)
			case uploadID != "":
				req.op = "UploadPart"
				return s.uploadPart(req, uploadID)
			case copySource:
				req.op = "CopyObject"
				return s.copyObject(req)
			}
			req.op = "PutObject"
			return s.putObject(req)
		case http.MethodGet:
			if uploadID != "" {
				req.op = "ListParts"
				return s.listParts(req, uploadID)
			}
			req.op = "GetObject"
			return s.getObject(req, false)
		case http.MethodHead:
			req.op = "HeadObject"
			return s.getObject(req, true)
		case http.MethodDelete:
			if uploadID != "" {
				req.op = "AbortMultipartUpload"
				return s.abortUpload(req, uploadID)
			}
			req.op = "DeleteObject"
			return s.deleteObject(req)
		}
		return errMethodNotAllowed
	}
}

// toAPIError maps ledger, vein and internal errors to S3 errors.
func (s *Server) toAPIError(req *request, err error) *apiError {
	var ae *apiError
	switch {
	case errors.As(err, &ae):
		return ae
	case errors.Is(err, ledger.ErrNoSuchBucket):
		return errNoSuchBucket
	case errors.Is(err, ledger.ErrNoSuchKey):
		return errNoSuchKey
	case errors.Is(err, ledger.ErrBucketExists):
		return errBucketOwned
	case errors.Is(err, ledger.ErrBucketNotEmpty):
		return errBucketNotEmpty
	case errors.Is(err, ledger.ErrNoSuchVersion):
		return errNoSuchVersion
	case errors.Is(err, ledger.ErrNoSuchUpload):
		return errNoSuchUpload
	case errors.Is(err, ledger.ErrInvalidPart):
		return errInvalidPart
	case errors.Is(err, ledger.ErrInvalidPartOrder):
		return errInvalidPartOrder
	case errors.Is(err, ledger.ErrEntityTooSmall):
		return errEntityTooSmall
	case errors.Is(err, ledger.ErrPreconditionFailed):
		return errPreconditionFailed
	case errors.Is(err, ledger.ErrCopyFromDeleteMarker):
		return errMethodNotAllowed
	case errors.Is(err, ledger.ErrQuotaExceeded):
		s.emit("quota.exceeded", map[string]any{"bucket": req.bucket, "key": req.key, "access_key": req.auth.AccessKey, "op": req.op})
		return errQuotaExceeded
	case errors.Is(err, ledger.ErrNoLifecycle):
		return errf(http.StatusNotFound, "NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist")
	case errors.Is(err, cluster.ErrInsufficientReplicas), errors.Is(err, cluster.ErrUnavailable):
		s.log.Warn("storage unavailable", "op", req.op, "bucket", req.bucket, "key", req.key, "request_id", req.id, "err", err)
		return errServiceUnavailable
	}
	s.log.Error("internal error", "op", req.op, "bucket", req.bucket, "key", req.key, "request_id", req.id, "err", err)
	return errInternal
}

func (s *Server) writeError(req *request, err error) {
	ae := s.toAPIError(req, err)
	if req.r.Method == http.MethodHead {
		req.w.WriteHeader(ae.Status)
		return
	}
	s.writeXML(req, ae.Status, S3Error{Code: ae.Code, Message: ae.Message, Resource: req.r.URL.Path, RequestID: req.id})
}

func (s *Server) writeXML(req *request, status int, v any) {
	req.w.Header().Set("Content-Type", "application/xml")
	req.w.WriteHeader(status)
	_, _ = req.w.Write([]byte(xml.Header))
	if err := xml.NewEncoder(req.w).Encode(v); err != nil {
		s.log.Warn("encode response", "err", err, "request_id", req.id)
	}
}

func (s *Server) emit(typ string, attrs map[string]any) {
	s.bus.Publish(events.Event{Type: typ, Component: "adit", Attrs: attrs})
}

// strategy returns the bucket's placement strategy.
func (s *Server) strategy(b ledger.Bucket) (survey.Strategy, error) {
	name := b.Placement
	if name == "" {
		name = s.placement
	}
	return survey.ByName(name, b.Pinned)
}

// writePolicy returns the replica count and write quorum for a bucket.
func (s *Server) writePolicy(b ledger.Bucket) (replicas, quorum int) {
	replicas = s.replication
	if b.Replication > 0 {
		replicas = b.Replication
	}
	quorum = s.quorum
	if quorum <= 0 {
		quorum = replicas/2 + 1
	}
	return replicas, min(quorum, replicas)
}

func validBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '.' {
			return false
		}
	}
	first, last := name[0], name[len(name)-1]
	return first != '-' && first != '.' && last != '-' && last != '.' && !strings.Contains(name, "..")
}
