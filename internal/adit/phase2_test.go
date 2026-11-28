package adit

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/tailings"
)

func TestMultipartUpload(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/mpu", nil, nil), 200, "")

	var init initiateResult
	if err := xml.Unmarshal(h.expect(h.do("POST", "/mpu/big.bin?uploads", nil, map[string]string{"Content-Type": "video/x"}), 200, ""), &init); err != nil {
		t.Fatal(err)
	}
	id := url.QueryEscape(init.UploadID)
	p1 := randomBytes(ledger.MinPartSize + 123)
	p2 := []byte("the end")
	var etags []string
	for i, p := range [][]byte{p1, p2} {
		resp := h.do("PUT", fmt.Sprintf("/mpu/big.bin?partNumber=%d&uploadId=%s", i+1, id), p, nil)
		h.expect(resp, 200, "")
		etags = append(etags, resp.Header.Get("ETag"))
	}
	h.expect(h.do("PUT", "/mpu/big.bin?partNumber=0&uploadId="+id, p2, nil), 400, "InvalidArgument")
	h.expect(h.do("PUT", "/mpu/big.bin?partNumber=1&uploadId=nope", p2, nil), 404, "NoSuchUpload")

	var parts listPartsResult
	if err := xml.Unmarshal(h.expect(h.do("GET", "/mpu/big.bin?uploadId="+id, nil, nil), 200, ""), &parts); err != nil || len(parts.Parts) != 2 {
		t.Fatalf("list parts = %+v %v", parts, err)
	}
	var ups listUploadsResult
	if err := xml.Unmarshal(h.expect(h.do("GET", "/mpu?uploads", nil, nil), 200, ""), &ups); err != nil || len(ups.Uploads) != 1 {
		t.Fatalf("list uploads = %+v %v", ups, err)
	}

	body := func(list ...int) []byte {
		var b strings.Builder
		b.WriteString("<CompleteMultipartUpload>")
		for _, n := range list {
			fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", n, etags[n-1])
		}
		b.WriteString("</CompleteMultipartUpload>")
		return []byte(b.String())
	}
	h.expect(h.do("POST", "/mpu/big.bin?uploadId="+id, body(2, 1), nil), 400, "InvalidPartOrder")
	var done completeResult
	if err := xml.Unmarshal(h.expect(h.do("POST", "/mpu/big.bin?uploadId="+id, body(1, 2), nil), 200, ""), &done); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(done.ETag, `-2"`) {
		t.Fatalf("multipart etag = %s", done.ETag)
	}

	resp := h.do("GET", "/mpu/big.bin", nil, nil)
	got := h.expect(resp, 200, "")
	if !bytes.Equal(got, append(append([]byte(nil), p1...), p2...)) {
		t.Fatal("completed object has wrong bytes")
	}
	if resp.Header.Get("ETag") != done.ETag || resp.Header.Get("Content-Type") != "video/x" || resp.Header.Get("X-Amz-Mp-Parts-Count") != "2" {
		t.Fatalf("headers: %v", resp.Header)
	}
	h.expect(h.do("POST", "/mpu/big.bin?uploadId="+id, body(1, 2), nil), 200, "") // retried complete is idempotent

	// Abort.
	if err := xml.Unmarshal(h.expect(h.do("POST", "/mpu/gone?uploads", nil, nil), 200, ""), &init); err != nil {
		t.Fatal(err)
	}
	h.expect(h.do("PUT", "/mpu/gone?partNumber=1&uploadId="+url.QueryEscape(init.UploadID), []byte("x"), nil), 200, "")
	h.expect(h.do("DELETE", "/mpu/gone?uploadId="+url.QueryEscape(init.UploadID), nil, nil), 204, "")
	h.expect(h.do("DELETE", "/mpu/gone?uploadId="+url.QueryEscape(init.UploadID), nil, nil), 404, "NoSuchUpload")
}

func TestVersioningHTTP(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/ver", nil, nil), 200, "")
	var cfg versioningConfiguration
	if err := xml.Unmarshal(h.expect(h.do("GET", "/ver?versioning", nil, nil), 200, ""), &cfg); err != nil || cfg.Status != "" {
		t.Fatalf("initial versioning = %+v %v", cfg, err)
	}
	h.expect(h.do("PUT", "/ver?versioning", []byte(`<VersioningConfiguration><Status>Bogus</Status></VersioningConfiguration>`), nil), 400, "IllegalVersioningConfigurationException")
	h.expect(h.do("PUT", "/ver?versioning", []byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`), nil), 200, "")

	r1 := h.do("PUT", "/ver/k", []byte("one"), nil)
	h.expect(r1, 200, "")
	r2 := h.do("PUT", "/ver/k", []byte("two"), nil)
	h.expect(r2, 200, "")
	v1, v2 := r1.Header.Get("X-Amz-Version-Id"), r2.Header.Get("X-Amz-Version-Id")
	if v1 == "" || v1 == v2 {
		t.Fatalf("version ids %q %q", v1, v2)
	}
	if b := h.expect(h.do("GET", "/ver/k?versionId="+v1, nil, nil), 200, ""); string(b) != "one" {
		t.Fatalf("old version = %q", b)
	}
	h.expect(h.do("GET", "/ver/k?versionId=nope", nil, nil), 404, "NoSuchVersion")

	del := h.do("DELETE", "/ver/k", nil, nil)
	h.expect(del, 204, "")
	marker := del.Header.Get("X-Amz-Version-Id")
	if del.Header.Get("X-Amz-Delete-Marker") != "true" || marker == "" {
		t.Fatalf("delete marker headers: %v", del.Header)
	}
	resp := h.do("GET", "/ver/k", nil, nil)
	h.expect(resp, 404, "NoSuchKey")
	if resp.Header.Get("X-Amz-Delete-Marker") != "true" {
		t.Fatal("404 on delete marker must carry x-amz-delete-marker")
	}
	h.expect(h.do("GET", "/ver/k?versionId="+marker, nil, nil), 405, "MethodNotAllowed")

	var lv listVersionsResult
	raw := h.expect(h.do("GET", "/ver?versions", nil, nil), 200, "")
	if err := xml.Unmarshal(raw, &lv); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("<DeleteMarker><Key>k</Key><VersionId>"+marker+"</VersionId><IsLatest>true</IsLatest>")) ||
		bytes.Count(raw, []byte("<Version>")) != 2 {
		t.Fatalf("list versions:\n%s", raw)
	}
	var page listVersionsResult
	raw = h.expect(h.do("GET", "/ver?versions&max-keys=1", nil, nil), 200, "")
	if err := xml.Unmarshal(raw, &page); err != nil || !page.IsTruncated || page.NextVersionIDMarker != marker {
		t.Fatalf("paged versions = %+v %v", page, err)
	}

	h.expect(h.do("DELETE", "/ver/k?versionId="+marker, nil, nil), 204, "")
	if b := h.expect(h.do("GET", "/ver/k", nil, nil), 200, ""); string(b) != "two" {
		t.Fatalf("after removing marker = %q", b)
	}
	if ls := h.expect(h.do("GET", "/ver?list-type=2", nil, nil), 200, ""); !bytes.Contains(ls, []byte("<Key>k</Key>")) {
		t.Fatal("restored key not listed")
	}
	h.expect(h.do("DELETE", "/ver", nil, nil), 409, "BucketNotEmpty")
}

// End to end: deleted data is reclaimed by Tailings, live data survives.
func TestTailingsReclaimsOnlyUnreferenced(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/gcb", nil, nil), 200, "")
	keep := randomBytes(3 << 20)
	drop := randomBytes(3<<20 + 1)
	h.expect(h.do("PUT", "/gcb/keep", keep, nil), 200, "")
	h.expect(h.do("PUT", "/gcb/drop", drop, nil), 200, "")
	h.expect(h.do("DELETE", "/gcb/drop", nil, nil), 204, "")

	w := tailings.New(tailings.Options{Ledger: h.ledger, Veins: h.cluster, Grace: time.Hour})
	if st, err := w.RunOnce(context.Background()); err != nil || st.NuggetsReclaimed != 0 {
		t.Fatalf("pass inside grace window: %+v %v", st, err)
	}
	w = tailings.New(tailings.Options{Ledger: h.ledger, Veins: h.cluster, Grace: -time.Minute})
	st, err := w.RunOnce(context.Background())
	if err != nil || st.NuggetsReclaimed != 1 {
		t.Fatalf("pass: %+v %v", st, err)
	}
	if _, err := h.vein.Compact(0.01); err != nil {
		t.Fatal(err)
	}
	if b := h.expect(h.do("GET", "/gcb/keep", nil, nil), 200, ""); !bytes.Equal(b, keep) {
		t.Fatal("live object damaged by Tailings")
	}
	// Re-uploading reclaimed content must work.
	h.expect(h.do("PUT", "/gcb/again", drop, nil), 200, "")
	if b := h.expect(h.do("GET", "/gcb/again", nil, nil), 200, ""); !bytes.Equal(b, drop) {
		t.Fatal("re-uploaded content after reclaim is wrong")
	}
}

func TestConditionalWritesAndPartNumber(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/cond", nil, nil), 200, "")
	r := h.do("PUT", "/cond/k", []byte("a"), map[string]string{"If-None-Match": "*"})
	h.expect(r, 200, "")
	etag := r.Header.Get("ETag")
	h.expect(h.do("PUT", "/cond/k", []byte("b"), map[string]string{"If-None-Match": "*"}), 412, "PreconditionFailed")
	h.expect(h.do("PUT", "/cond/k", []byte("b"), map[string]string{"If-Match": `"bad"`}), 412, "PreconditionFailed")
	h.expect(h.do("PUT", "/cond/k", []byte("b"), map[string]string{"If-Match": etag}), 200, "")
	h.expect(h.do("PUT", "/cond/missing", []byte("b"), map[string]string{"If-Match": "*"}), 404, "NoSuchKey")
	if b := h.expect(h.do("GET", "/cond/k", nil, nil), 200, ""); string(b) != "b" {
		t.Fatalf("got %q", b)
	}

	var init initiateResult
	_ = xml.Unmarshal(h.expect(h.do("POST", "/cond/mp?uploads", nil, nil), 200, ""), &init)
	id := url.QueryEscape(init.UploadID)
	p1, p2 := randomBytes(ledger.MinPartSize), []byte("tail")
	var list strings.Builder
	list.WriteString("<CompleteMultipartUpload>")
	for i, p := range [][]byte{p1, p2} {
		resp := h.do("PUT", fmt.Sprintf("/cond/mp?partNumber=%d&uploadId=%s", i+1, id), p, nil)
		h.expect(resp, 200, "")
		fmt.Fprintf(&list, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, resp.Header.Get("ETag"))
	}
	list.WriteString("</CompleteMultipartUpload>")
	h.expect(h.do("POST", "/cond/mp?uploadId="+id, []byte(list.String()), map[string]string{"If-Match": "*"}), 404, "NoSuchKey")
	h.expect(h.do("POST", "/cond/mp?uploadId="+id, []byte(list.String()), nil), 200, "")

	resp := h.do("GET", "/cond/mp?partNumber=2", nil, nil)
	if b := h.expect(resp, 206, ""); string(b) != "tail" || resp.Header.Get("X-Amz-Mp-Parts-Count") != "2" {
		t.Fatalf("part 2 = %q %v", b, resp.Header)
	}
	h.expect(h.do("GET", "/cond/mp?partNumber=3", nil, nil), 400, "InvalidPart")
	if b := h.expect(h.do("GET", "/cond/k?partNumber=1", nil, nil), 206, ""); string(b) != "b" {
		t.Fatalf("plain object part 1 = %q", b)
	}
	resp = h.do("PUT", "/cond/meta", []byte("x"), map[string]string{"X-Amz-Meta-Foo": "bar"})
	h.expect(resp, 200, "")
	resp = h.do("GET", "/cond/meta", nil, nil)
	h.expect(resp, 200, "")
	// Go's client canonicalises header names; s3-tests checks the wire casing.
	if resp.Header.Get("X-Amz-Meta-Foo") != "bar" {
		t.Fatalf("metadata = %v", resp.Header)
	}
}
