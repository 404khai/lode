// Package adit implements the S3-compatible HTTP gateway.
package adit

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// S3Error is the XML error body returned on the S3 surface.
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

// Server is the Adit HTTP handler.
type Server struct {
	log      *slog.Logger
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

// New builds a Server registering its metrics on reg.
func New(log *slog.Logger, reg prometheus.Registerer) *Server {
	f := promauto.With(reg)
	return &Server{
		log: log,
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "lode", Subsystem: "adit", Name: "requests_total",
			Help: "S3 requests handled, by method and status code.",
		}, []string{"method", "code"}),
		latency: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "lode", Subsystem: "adit", Name: "request_duration_seconds",
			Help: "S3 request latency.", Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := strconv.FormatInt(start.UnixNano(), 36)
	code := http.StatusNotImplemented

	// Phase 0: no S3 operations exist yet.
	writeError(w, code, S3Error{
		Code:      "NotImplemented",
		Message:   "A header or operation you provided implies functionality that is not implemented.",
		Resource:  r.URL.Path,
		RequestID: reqID,
	})

	s.requests.WithLabelValues(r.Method, strconv.Itoa(code)).Inc()
	s.latency.WithLabelValues(r.Method).Observe(time.Since(start).Seconds())
	s.log.Debug("request", "method", r.Method, "path", r.URL.Path, "code", code, "request_id", reqID)
}

func writeError(w http.ResponseWriter, code int, e S3Error) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", e.RequestID)
	w.WriteHeader(code)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(e)
}
