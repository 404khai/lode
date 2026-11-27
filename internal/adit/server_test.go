package adit

import (
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNotImplemented(t *testing.T) {
	reg := prometheus.NewRegistry()
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), reg)
	ts := httptest.NewServer(s)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/bucket/key")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var e S3Error
	if err := xml.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	if e.Code != "NotImplemented" || e.Resource != "/bucket/key" || e.RequestID == "" {
		t.Fatalf("unexpected error body: %+v", e)
	}
	if got := testutil.ToFloat64(s.requests.WithLabelValues("GET", "501")); got != 1 {
		t.Fatalf("requests_total = %v", got)
	}
}
