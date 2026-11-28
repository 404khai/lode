package adit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"net/url"
	"strings"
	"testing"

	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
)

func TestCopyObjectSharesNuggets(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.expect(h.do("PUT", "/src", nil, nil), 200, "")
	h.expect(h.do("PUT", "/dst", nil, nil), 200, "")
	data := randomBytes(2 * nugget.MaxSize)
	h.expect(h.do("PUT", "/src/a b", data, map[string]string{"Content-Type": "text/x", "X-Amz-Meta-K": "v", "X-Amz-Tagging": "t1=one"}), 200, "")
	before := h.referenced()
	var res copyObjectResult
	raw := h.expect(h.do("PUT", "/dst/copy", nil, map[string]string{"X-Amz-Copy-Source": "/src/a%20b"}), 200, "")
	if err := xml.Unmarshal(raw, &res); err != nil || res.ETag == "" {
		t.Fatalf("copy result %s", raw)
	}
	if h.referenced() != before {
		t.Fatal("copy should not create new nuggets")
	}
	resp := h.do("GET", "/dst/copy", nil, nil)
	if got := h.expect(resp, 200, ""); !bytes.Equal(got, data) || resp.Header.Get("Content-Type") != "text/x" ||
		resp.Header.Get("X-Amz-Meta-K") != "v" || resp.Header.Get("X-Amz-Tagging-Count") != "1" {
		t.Fatalf("copy contents/headers wrong: %v", resp.Header)
	}
	// REPLACE directives.
	h.expect(h.do("PUT", "/dst/copy2", nil, map[string]string{
		"X-Amz-Copy-Source": "src/a%20b", "X-Amz-Metadata-Directive": "REPLACE", "Content-Type": "image/y",
		"X-Amz-Tagging-Directive": "REPLACE", "X-Amz-Tagging": "a=1&b=2",
	}), 200, "")
	resp = h.do("HEAD", "/dst/copy2", nil, nil)
	h.expect(resp, 200, "")
	if resp.Header.Get("Content-Type") != "image/y" || resp.Header.Get("X-Amz-Meta-K") != "" || resp.Header.Get("X-Amz-Tagging-Count") != "2" {
		t.Fatalf("replace directives ignored: %v", resp.Header)
	}
	h.expect(h.do("PUT", "/src/a%20b", nil, map[string]string{"X-Amz-Copy-Source": "/src/a%20b"}), 400, "InvalidRequest")
	h.expect(h.do("PUT", "/dst/x", nil, map[string]string{"X-Amz-Copy-Source": "/src/missing"}), 404, "NoSuchKey")
	h.expect(h.do("PUT", "/dst/x", nil, map[string]string{"X-Amz-Copy-Source": "/src/a%20b", "X-Amz-Copy-Source-If-Match": `"nope"`}), 412, "PreconditionFailed")

	// Deleting the source keeps the copy's data alive.
	h.expect(h.do("DELETE", "/src/a%20b", nil, nil), 204, "")
	if got := h.expect(h.do("GET", "/dst/copy", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("copy lost data after source delete")
	}
	_ = ctx
}

func TestUploadPartCopy(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/pcopy", nil, nil), 200, "")
	data := randomBytes(ledger.MinPartSize + 777)
	h.expect(h.do("PUT", "/pcopy/src", data, nil), 200, "")
	var init initiateResult
	_ = xml.Unmarshal(h.expect(h.do("POST", "/pcopy/dst?uploads", nil, nil), 200, ""), &init)
	id := url.QueryEscape(init.UploadID)
	var p1, p2 copyPartResult
	_ = xml.Unmarshal(h.expect(h.do("PUT", "/pcopy/dst?partNumber=1&uploadId="+id, nil, map[string]string{
		"X-Amz-Copy-Source": "/pcopy/src", "X-Amz-Copy-Source-Range": fmt.Sprintf("bytes=0-%d", ledger.MinPartSize-1)}), 200, ""), &p1)
	_ = xml.Unmarshal(h.expect(h.do("PUT", "/pcopy/dst?partNumber=2&uploadId="+id, nil, map[string]string{
		"X-Amz-Copy-Source": "/pcopy/src", "X-Amz-Copy-Source-Range": fmt.Sprintf("bytes=%d-%d", ledger.MinPartSize, len(data)-1)}), 200, ""), &p2)
	h.expect(h.do("PUT", "/pcopy/dst?partNumber=3&uploadId="+id, nil, map[string]string{
		"X-Amz-Copy-Source": "/pcopy/src", "X-Amz-Copy-Source-Range": fmt.Sprintf("bytes=0-%d", len(data))}), 400, "InvalidRange")
	body := fmt.Sprintf("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>", p1.ETag, p2.ETag)
	h.expect(h.do("POST", "/pcopy/dst?uploadId="+id, []byte(body), nil), 200, "")
	if got := h.expect(h.do("GET", "/pcopy/dst", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("part-copied object differs from source")
	}
}

func TestTaggingAndACL(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/tags", nil, nil), 200, "")
	h.expect(h.do("PUT", "/tags/k", []byte("x"), nil), 200, "")
	put := []byte(`<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag><Tag><Key>team</Key><Value>ore</Value></Tag></TagSet></Tagging>`)
	h.expect(h.do("PUT", "/tags/k?tagging", put, nil), 200, "")
	var got tagging
	_ = xml.Unmarshal(h.expect(h.do("GET", "/tags/k?tagging", nil, nil), 200, ""), &got)
	if len(got.TagSet) != 2 || got.TagSet[0].Key != "env" {
		t.Fatalf("tags = %+v", got)
	}
	dup := []byte(`<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag><Tag><Key>a</Key><Value>2</Value></Tag></TagSet></Tagging>`)
	h.expect(h.do("PUT", "/tags/k?tagging", dup, nil), 400, "InvalidTag")
	h.expect(h.do("DELETE", "/tags/k?tagging", nil, nil), 204, "")
	var cleared tagging
	_ = xml.Unmarshal(h.expect(h.do("GET", "/tags/k?tagging", nil, nil), 200, ""), &cleared)
	if len(cleared.TagSet) != 0 {
		t.Fatal("tags not deleted")
	}
	h.expect(h.do("GET", "/tags?tagging", nil, nil), 404, "NoSuchTagSet")
	h.expect(h.do("PUT", "/tags?tagging", put, nil), 204, "")
	h.expect(h.do("GET", "/tags?tagging", nil, nil), 200, "")

	var acl accessControlPolicy
	_ = xml.Unmarshal(h.expect(h.do("GET", "/tags/k?acl", nil, nil), 200, ""), &acl)
	if len(acl.Grants) != 1 || acl.Grants[0].Permission != "FULL_CONTROL" {
		t.Fatalf("acl = %+v", acl)
	}
	h.expect(h.do("PUT", "/tags?acl", nil, map[string]string{"X-Amz-Acl": "private"}), 200, "")
	h.expect(h.do("PUT", "/tags?acl", nil, map[string]string{"X-Amz-Acl": "public-read"}), 501, "NotImplemented")
	h.expect(h.do("PUT", "/tags/pub", []byte("x"), map[string]string{"X-Amz-Acl": "public-read"}), 501, "NotImplemented")
	h.expect(h.do("PUT", "/tags/enc", []byte("x"), map[string]string{"X-Amz-Server-Side-Encryption": "AES256"}), 501, "NotImplemented")
}

func crc32b64(b []byte) string {
	return base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(b)))
}

func TestMultipartChecksums(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/cksum", nil, nil), 200, "")
	p1, p2 := randomBytes(ledger.MinPartSize), []byte("tail bytes")
	upload := func(key string, hdr map[string]string, partHdr func([]byte) map[string]string) (completeResult, int) {
		var init initiateResult
		_ = xml.Unmarshal(h.expect(h.do("POST", "/cksum/"+key+"?uploads", nil, hdr), 200, ""), &init)
		id := url.QueryEscape(init.UploadID)
		var b strings.Builder
		b.WriteString("<CompleteMultipartUpload>")
		for i, p := range [][]byte{p1, p2} {
			r := h.do("PUT", fmt.Sprintf("/cksum/%s?partNumber=%d&uploadId=%s", key, i+1, id), p, partHdr(p))
			h.expect(r, 200, "")
			fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", i+1, r.Header.Get("ETag"))
		}
		b.WriteString("</CompleteMultipartUpload>")
		resp := h.do("POST", "/cksum/"+key+"?uploadId="+id, []byte(b.String()), nil)
		var out completeResult
		raw := readAll(t, resp)
		_ = xml.Unmarshal(raw, &out)
		return out, resp.StatusCode
	}

	// FULL_OBJECT CRC32: the CRC of the whole object.
	full, code := upload("full", map[string]string{"X-Amz-Checksum-Algorithm": "CRC32", "X-Amz-Checksum-Type": "FULL_OBJECT"},
		func(p []byte) map[string]string { return map[string]string{"X-Amz-Checksum-Crc32": crc32b64(p)} })
	whole := append(append([]byte(nil), p1...), p2...)
	if code != 200 || full.ChecksumCRC32 != crc32b64(whole) || full.ChecksumType != "FULL_OBJECT" {
		t.Fatalf("full-object crc32 = %+v (%d), want %s", full, code, crc32b64(whole))
	}
	resp := h.do("HEAD", "/cksum/full", nil, map[string]string{"X-Amz-Checksum-Mode": "ENABLED"})
	h.expect(resp, 200, "")
	if resp.Header.Get("X-Amz-Checksum-Crc32") != crc32b64(whole) || resp.Header.Get("X-Amz-Checksum-Type") != "FULL_OBJECT" {
		t.Fatalf("stored checksum headers: %v", resp.Header)
	}

	// COMPOSITE SHA256 with checksums computed server-side when parts omit them.
	comp, code := upload("comp", map[string]string{"X-Amz-Checksum-Algorithm": "SHA256"},
		func([]byte) map[string]string { return nil })
	h1, h2 := sha256.Sum256(p1), sha256.Sum256(p2)
	outer := sha256.Sum256(append(h1[:], h2[:]...))
	if want := base64.StdEncoding.EncodeToString(outer[:]) + "-2"; code != 200 || comp.ChecksumSHA256 != want || comp.ChecksumType != "COMPOSITE" {
		t.Fatalf("composite sha256 = %+v (%d), want %s", comp, code, want)
	}
	h.expect(h.do("POST", "/cksum/bad?uploads", nil, map[string]string{"X-Amz-Checksum-Algorithm": "SHA256", "X-Amz-Checksum-Type": "FULL_OBJECT"}), 400, "InvalidRequest")
}
