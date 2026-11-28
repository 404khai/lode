package adit

import (
	"bytes"
	"context"
	"crypto/md5" // #nosec G501
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	lodev1 "github.com/404khai/lode/gen/lode/v1"
	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/vein"
)

const (
	testAK = "lodetest"
	testSK = "lodetestsecret"
)

type harness struct {
	t       *testing.T
	ts      *httptest.Server
	vein    *vein.Store // the first vein
	veins   []*testVein
	cluster *cluster.Client
	ledger  *ledger.Ledger
	dir     string // the first vein's data directory
}

// testVein is an in-process vein served over real gRPC on localhost.
type testVein struct {
	id    string
	dir   string
	store *vein.Store
	gs    *grpc.Server
	addr  string
}

func (v *testVein) stop() { v.gs.Stop() }

// restart serves the same store again on the same address.
func (v *testVein) restart(t *testing.T) {
	t.Helper()
	lis, err := net.Listen("tcp", v.addr)
	if err != nil {
		t.Fatal(err)
	}
	v.gs = grpc.NewServer()
	lodev1.RegisterVeinServiceServer(v.gs, vein.NewServer(v.store, v.id))
	go func() { _ = v.gs.Serve(lis) }()
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return newClusterHarness(t, 1, 1)
}

// newClusterHarness starts n veins (alternating zones a/b) and an Adit
// writing `replication` copies of each nugget.
func newClusterHarness(t *testing.T, n, replication int) *harness {
	t.Helper()
	dsn := os.Getenv("LODE_TEST_DSN")
	if dsn == "" {
		t.Skip("LODE_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("adit_%d", time.Now().UnixNano())
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, _ := pgxpool.ParseConfig(dsn)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	led := ledger.New(pool)
	var veins []*testVein
	for i := range n {
		dir := t.TempDir()
		store, err := vein.Open(vein.Options{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		v := &testVein{id: fmt.Sprintf("v%d", i+1), dir: dir, store: store, addr: lis.Addr().String()}
		v.gs = grpc.NewServer()
		lodev1.RegisterVeinServiceServer(v.gs, vein.NewServer(store, v.id))
		go func() { _ = v.gs.Serve(lis) }()
		if err := led.RegisterVein(ctx, ledger.VeinInfo{ID: v.id, Addr: v.addr, Zone: []string{"a", "b"}[i%2], Host: v.id}); err != nil {
			t.Fatal(err)
		}
		veins = append(veins, v)
	}
	cl := cluster.New(cluster.Options{Ledger: led, Registerer: reg, Logger: testLogger(), StaleAfter: time.Hour, Backoff: 200 * time.Millisecond, RPCTimeout: 30 * time.Second})
	if err := cl.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	srv := New(Options{
		Logger:      testLogger(),
		Registerer:  reg,
		Ledger:      led,
		Cluster:     cl,
		Replication: replication,
		Credentials: map[string]string{testAK: testSK},
	})
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		cl.Close()
		for _, v := range veins {
			v.gs.Stop()
			_ = v.store.Close()
		}
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	return &harness{t: t, ts: ts, vein: veins[0].store, veins: veins, cluster: cl, ledger: led, dir: veins[0].dir}
}

// sign adds a SigV4 Authorization header. payloadHash defaults to the body's SHA-256.
func sign(r *http.Request, body []byte, payloadHash string) {
	now := time.Now().UTC()
	amzDate := now.Format(amzDateFormat)
	if payloadHash == "" {
		payloadHash = sha256Hex(body)
	}
	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signed := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	for name := range r.Header {
		l := strings.ToLower(name)
		if strings.HasPrefix(l, "x-amz-") && l != "x-amz-date" && l != "x-amz-content-sha256" || l == "content-md5" || l == "range" {
			signed = append(signed, l)
		}
	}
	sort.Strings(signed)
	scope := amzDate[:8] + "/us-east-1/s3/aws4_request"
	canon := canonicalRequest(r, canonicalQuery(parseRawQuery(r.URL.RawQuery), ""), signed, payloadHash)
	sig := hex.EncodeToString(hmacSHA256(signingKey(testSK, amzDate[:8], "us-east-1", "s3"), stringToSign(amzDate, scope, canon)))
	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s,SignedHeaders=%s,Signature=%s",
		testAK, scope, strings.Join(signed, ";"), sig))
}

func (h *harness) do(method, path string, body []byte, hdr map[string]string) *http.Response {
	h.t.Helper()
	r, err := http.NewRequest(method, h.ts.URL+path, bytes.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	r.Host = r.URL.Host
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	sign(r, body, "")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

// testLogger discards logs unless LODE_TEST_LOG is set.
func testLogger() *slog.Logger {
	if os.Getenv("LODE_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(uint64(n), 7))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// referenced counts nuggets the Ledger holds references to.
func (h *harness) referenced() int {
	var n int
	if err := h.ledger.Pool().QueryRow(context.Background(), "SELECT count(*) FROM nuggets WHERE refs > 0").Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (h *harness) expect(resp *http.Response, status int, code string) []byte {
	h.t.Helper()
	b := readAll(h.t, resp)
	if resp.StatusCode != status {
		h.t.Fatalf("status = %d, want %d; body: %s", resp.StatusCode, status, b)
	}
	if code != "" {
		var e S3Error
		if err := xml.Unmarshal(b, &e); err != nil || e.Code != code {
			h.t.Fatalf("error code = %q, want %q (%s)", e.Code, code, b)
		}
	}
	return b
}

func TestBucketOperations(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/Bad_Name", nil, nil), 400, "InvalidBucketName")
	h.expect(h.do("PUT", "/photos", nil, nil), 200, "")
	h.expect(h.do("PUT", "/photos", nil, nil), 200, "") // us-east-1 semantics
	h.expect(h.do("HEAD", "/photos", nil, nil), 200, "")
	h.expect(h.do("HEAD", "/nope", nil, nil), 404, "")

	var list listAllMyBucketsResult
	if err := xml.Unmarshal(h.expect(h.do("GET", "/", nil, nil), 200, ""), &list); err != nil || len(list.Buckets) != 1 || list.Buckets[0].Name != "photos" {
		t.Fatalf("list buckets = %+v %v", list, err)
	}
	h.expect(h.do("PUT", "/photos/a", []byte("x"), nil), 200, "")
	h.expect(h.do("DELETE", "/photos", nil, nil), 409, "BucketNotEmpty")
	h.expect(h.do("DELETE", "/photos/a", nil, nil), 204, "")
	h.expect(h.do("DELETE", "/photos", nil, nil), 204, "")
	h.expect(h.do("DELETE", "/photos", nil, nil), 404, "NoSuchBucket")
	h.expect(h.do("GET", "/photos?tagging", nil, nil), 501, "NotImplemented")
}

func TestPutGetObject(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/bkt1", nil, nil), 200, "")
	h.expect(h.do("PUT", "/nobucket/k", []byte("x"), nil), 404, "NoSuchBucket")

	// Spans three nuggets so ranges cross nugget boundaries.
	data := make([]byte, 2*nugget.Size+12345)
	for i := range data {
		data[i] = byte(i * 7)
	}
	sum := md5.Sum(data) // #nosec G401
	resp := h.do("PUT", "/bkt1/dir/file.bin", data, map[string]string{
		"Content-Type":         "application/x-test",
		"Content-MD5":          base64.StdEncoding.EncodeToString(sum[:]),
		"X-Amz-Meta-Origin":    "unit-test",
		"X-Amz-Checksum-Crc32": "AAAAAA==", // deliberately wrong
	})
	if resp.StatusCode != 400 {
		t.Fatalf("bad checksum accepted: %d", resp.StatusCode)
	}
	_ = readAll(t, resp)
	if n := h.referenced(); n != 0 {
		t.Fatalf("failed put left %d referenced nuggets", n)
	}

	c := crc32.ChecksumIEEE(data)
	crc := base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, c))
	resp = h.do("PUT", "/bkt1/dir/file.bin", data, map[string]string{
		"Content-Type":         "application/x-test",
		"Content-MD5":          base64.StdEncoding.EncodeToString(sum[:]),
		"X-Amz-Meta-Origin":    "unit-test",
		"X-Amz-Checksum-Crc32": crc,
	})
	h.expect(resp, 200, "")
	wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
	if resp.Header.Get("ETag") != wantETag {
		t.Fatalf("etag = %s", resp.Header.Get("ETag"))
	}

	resp = h.do("GET", "/bkt1/dir/file.bin", nil, map[string]string{"X-Amz-Checksum-Mode": "ENABLED"})
	got := h.expect(resp, 200, "")
	if !bytes.Equal(got, data) {
		t.Fatal("GET returned different bytes")
	}
	for k, want := range map[string]string{
		"Content-Type": "application/x-test", "X-Amz-Meta-Origin": "unit-test", "ETag": wantETag, "X-Amz-Checksum-Crc32": crc,
	} {
		if v := resp.Header.Get(k); v != want {
			t.Errorf("%s = %q, want %q", k, v, want)
		}
	}

	start, end := int64(nugget.Size-10), int64(2*nugget.Size+5)
	resp = h.do("GET", "/bkt1/dir/file.bin", nil, map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", start, end), "X-Amz-Checksum-Mode": "ENABLED"})
	got = h.expect(resp, 206, "")
	if resp.Header.Get("X-Amz-Checksum-Crc32") != "" {
		t.Fatal("full-object checksum returned on a ranged GET")
	}
	if !bytes.Equal(got, data[start:end+1]) {
		t.Fatal("range across nuggets returned wrong bytes")
	}
	if cr := resp.Header.Get("Content-Range"); cr != fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)) {
		t.Fatalf("content-range = %s", cr)
	}
	h.expect(h.do("GET", "/bkt1/dir/file.bin", nil, map[string]string{"Range": "bytes=999999999-"}), 416, "InvalidRange")
	h.expect(h.do("GET", "/bkt1/dir/file.bin", nil, map[string]string{"If-None-Match": wantETag}), 304, "")
	h.expect(h.do("GET", "/bkt1/dir/file.bin", nil, map[string]string{"If-Match": `"nope"`}), 412, "PreconditionFailed")

	resp = h.do("HEAD", "/bkt1/dir/file.bin", nil, nil)
	if b := h.expect(resp, 200, ""); len(b) != 0 || resp.ContentLength != int64(len(data)) {
		t.Fatalf("HEAD: body %d bytes, content-length %d", len(b), resp.ContentLength)
	}
	h.expect(h.do("GET", "/bkt1/missing", nil, nil), 404, "NoSuchKey")
	h.expect(h.do("PUT", "/bkt1/x", []byte("abc"), map[string]string{"Content-MD5": base64.StdEncoding.EncodeToString(sum[:])}), 400, "BadDigest")

	// Empty object.
	h.expect(h.do("PUT", "/bkt1/empty", nil, nil), 200, "")
	if b := h.expect(h.do("GET", "/bkt1/empty", nil, nil), 200, ""); len(b) != 0 {
		t.Fatal("empty object not empty")
	}
}

func TestDedupAndOverwriteReleasesNuggets(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/bkt", nil, nil), 200, "")
	data := []byte("same bytes")
	id := nugget.Sum(data)
	h.expect(h.do("PUT", "/bkt/one", data, nil), 200, "")
	h.expect(h.do("PUT", "/bkt/two", data, nil), 200, "")
	if n, _ := h.ledger.NuggetRefs(context.Background(), id); n != 2 {
		t.Fatalf("refs = %d, want 2", n)
	}
	h.expect(h.do("PUT", "/bkt/one", []byte("new bytes"), nil), 200, "")
	if n, _ := h.ledger.NuggetRefs(context.Background(), id); n != 1 {
		t.Fatalf("overwrite should release old nugget: refs = %d", n)
	}
	if got := h.expect(h.do("GET", "/bkt/two", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("shared nugget lost after overwrite of another key")
	}
	h.expect(h.do("DELETE", "/bkt/two", nil, nil), 204, "")
	if n, _ := h.ledger.NuggetRefs(context.Background(), id); n != 0 {
		t.Fatalf("refs after delete = %d", n)
	}
	h.expect(h.do("DELETE", "/bkt/two", nil, nil), 204, "")
}

func TestCorruptNuggetNeverServed(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/bkt", nil, nil), 200, "")
	data := bytes.Repeat([]byte("lode"), 1000)
	h.expect(h.do("PUT", "/bkt/k", data, nil), 200, "")

	segs, _ := filepath.Glob(filepath.Join(h.dir, "segments", "*.seg"))
	f, err := os.OpenFile(segs[0], os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("X"), 10); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	resp := h.do("GET", "/bkt/k", nil, nil)
	b := h.expect(resp, 500, "InternalError")
	if bytes.Contains(b, []byte("lode")) {
		t.Fatal("corrupt bytes leaked into the response")
	}
}

func TestListObjects(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/bkt", nil, nil), 200, "")
	keys := []string{"a.txt", "photos/2024/1.jpg", "photos/2024/2.jpg", "photos/2025/1.jpg", "photos/cover.jpg", "z z.txt"}
	for _, k := range keys {
		h.expect(h.do("PUT", "/bkt/"+strings.ReplaceAll(k, " ", "%20"), []byte(k), nil), 200, "")
	}

	list := func(query string) listBucketResult {
		t.Helper()
		var out listBucketResult
		if err := xml.Unmarshal(h.expect(h.do("GET", "/bkt?"+query, nil, nil), 200, ""), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	names := func(r listBucketResult) (ks, ps []string) {
		for _, c := range r.Contents {
			ks = append(ks, c.Key)
		}
		for _, p := range r.CommonPrefixes {
			ps = append(ps, p.Prefix)
		}
		return
	}

	r := list("list-type=2")
	if ks, _ := names(r); len(ks) != len(keys) || *r.KeyCount != len(keys) || r.IsTruncated {
		t.Fatalf("full list = %v", ks)
	}
	r = list("list-type=2&delimiter=/")
	if ks, ps := names(r); fmt.Sprint(ks) != "[a.txt z z.txt]" || fmt.Sprint(ps) != "[photos/]" {
		t.Fatalf("delimiter list = %v %v", ks, ps)
	}
	r = list("list-type=2&delimiter=/&prefix=photos/")
	if ks, ps := names(r); fmt.Sprint(ks) != "[photos/cover.jpg]" || fmt.Sprint(ps) != "[photos/2024/ photos/2025/]" {
		t.Fatalf("prefix list = %v %v", ks, ps)
	}
	r = list("list-type=2&encoding-type=url")
	if r.Contents[len(r.Contents)-1].Key != "z%20z.txt" {
		t.Fatalf("url encoding: %s", r.Contents[len(r.Contents)-1].Key)
	}

	// Paginate two at a time with a delimiter; common prefixes must not repeat.
	var all []string
	token := ""
	for page := 0; page < 10; page++ {
		q := "list-type=2&delimiter=/&prefix=photos/&max-keys=1"
		if token != "" {
			q += "&continuation-token=" + token
		}
		r = list(q)
		ks, ps := names(r)
		all = append(all, append(ps, ks...)...)
		if !r.IsTruncated {
			break
		}
		token = r.NextContinuationToken
	}
	if fmt.Sprint(all) != "[photos/2024/ photos/2025/ photos/cover.jpg]" {
		t.Fatalf("paginated = %v", all)
	}

	// ListObjects v1 with marker.
	r = list("marker=photos/cover.jpg")
	if ks, _ := names(r); fmt.Sprint(ks) != "[z z.txt]" {
		t.Fatalf("v1 marker = %v", ks)
	}

	// Multi-object delete.
	del := []byte(`<Delete><Object><Key>a.txt</Key></Object><Object><Key>z z.txt</Key></Object><Object><Key>nope</Key></Object></Delete>`)
	var dr deleteResult
	if err := xml.Unmarshal(h.expect(h.do("POST", "/bkt?delete", del, nil), 200, ""), &dr); err != nil || len(dr.Deleted) != 3 {
		t.Fatalf("delete objects = %+v %v", dr, err)
	}
	if ks, _ := names(list("list-type=2")); len(ks) != 4 {
		t.Fatalf("after delete = %v", ks)
	}
}

func TestStreamingUnsignedTrailer(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/bkt", nil, nil), 200, "")
	data := []byte("hello")
	c := crc32.ChecksumIEEE(data)
	crc := base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, c))
	for _, tc := range []struct {
		crc    string
		status int
	}{{crc, 200}, {"AAAAAA==", 400}} {
		body := []byte("5\r\nhello\r\n0\r\nx-amz-checksum-crc32:" + tc.crc + "\r\n\r\n")
		r, _ := http.NewRequest("PUT", h.ts.URL+"/bkt/streamed", bytes.NewReader(body))
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("X-Amz-Decoded-Content-Length", "5")
		r.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
		sign(r, body, streamingUnsignedTrailer)
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		h.expect(resp, tc.status, "")
	}
	resp := h.do("GET", "/bkt/streamed", nil, nil)
	if got := h.expect(resp, 200, ""); string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
	if resp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("aws-chunked leaked into stored content-encoding: %q", resp.Header.Get("Content-Encoding"))
	}
}

func TestAuthRequired(t *testing.T) {
	h := newHarness(t)
	resp, err := http.Get(h.ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	h.expect(resp, 403, "AccessDenied")

	r, _ := http.NewRequest("GET", h.ts.URL+"/", nil)
	sign(r, nil, "")
	r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), testAK, "someoneelse", 1))
	resp, _ = http.DefaultClient.Do(r)
	h.expect(resp, 403, "InvalidAccessKeyId")

	r, _ = http.NewRequest("PUT", h.ts.URL+"/bkt2", bytes.NewReader([]byte("body")))
	sign(r, []byte("different"), "")
	resp, _ = http.DefaultClient.Do(r)
	h.expect(resp, 400, "XAmzContentSHA256Mismatch")
}

// Corruption in a later nugget is only found after the response has started;
// the connection must be aborted so the client never accepts wrong bytes.
func TestCorruptionMidStreamAborts(t *testing.T) {
	h := newHarness(t)
	h.expect(h.do("PUT", "/bkt", nil, nil), 200, "")
	data := randomBytes(2*nugget.MaxSize + 1000)
	h.expect(h.do("PUT", "/bkt/big", data, nil), 200, "")

	segs, _ := filepath.Glob(filepath.Join(h.dir, "segments", "*.seg"))
	f, err := os.OpenFile(segs[0], os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff, 0xff}, int64(len(data)-100)); err != nil { // inside the last nugget
		t.Fatal(err)
	}
	_ = f.Close()

	resp := h.do("GET", "/bkt/big", nil, nil)
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatalf("read of corrupt object succeeded with %d bytes", len(got))
	}
	if int64(len(got)) >= int64(len(data)) || !bytes.Equal(got, data[:len(got)]) {
		t.Fatal("client received bytes that differ from the original object")
	}
}
