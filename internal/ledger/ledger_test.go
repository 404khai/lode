package ledger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/404khai/lode/internal/nugget"
)

// TestLedger runs against the Postgres in LODE_TEST_DSN, using a throwaway
// schema so tests never touch real data.
func newTestLedger(t *testing.T) *Ledger {
	t.Helper()
	dsn := os.Getenv("LODE_TEST_DSN")
	if dsn == "" {
		t.Skip("LODE_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate is not idempotent: %v", err)
	}
	return New(pool)
}

func version(data ...string) Version {
	v := Version{ETag: "etag", ChunkSize: nugget.Size, Headers: map[string]string{"content-type": "text/plain"}}
	for _, d := range data {
		v.Nuggets = append(v.Nuggets, NuggetRef{ID: nugget.Sum([]byte(d)), Length: int64(len(d))})
		v.Size += int64(len(d))
	}
	return v
}

func refs(t *testing.T, l *Ledger, data string) int64 {
	t.Helper()
	n, err := l.NuggetRefs(context.Background(), nugget.Sum([]byte(data)))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestBuckets(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	if err := l.CreateBucket(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if err := l.CreateBucket(ctx, "b1"); !errors.Is(err, ErrBucketExists) {
		t.Fatalf("want ErrBucketExists, got %v", err)
	}
	if _, err := l.PutObject(ctx, "b1", "k", version("x"), Condition{}); err != nil {
		t.Fatal(err)
	}
	if err := l.DeleteBucket(ctx, "b1"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("want ErrBucketNotEmpty, got %v", err)
	}
	if _, err := l.DeleteObject(ctx, "b1", "k", ""); err != nil {
		t.Fatal(err)
	}
	up, err := l.CreateUpload(ctx, "b1", "u", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.PutPart(ctx, "b1", "u", up, part(1, "pending")); err != nil {
		t.Fatal(err)
	}
	// Incomplete uploads are discarded with the bucket, as in S3.
	if err := l.DeleteBucket(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if refs(t, l, "pending") != 0 {
		t.Fatal("discarded upload kept its refs")
	}
	if err := l.DeleteBucket(ctx, "b1"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("want ErrNoSuchBucket, got %v", err)
	}
}

func TestObjectLifecycleAndRefs(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	if _, err := l.PutObject(ctx, "missing", "k", version("x"), Condition{}); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("want ErrNoSuchBucket, got %v", err)
	}
	_ = l.CreateBucket(ctx, "b")

	res, err := l.PutObject(ctx, "b", "k", version("a", "b", "a"), Condition{})
	if err != nil || res.VersionID != NullVersion || res.Versioning != "" {
		t.Fatalf("put: %+v %v", res, err)
	}
	if refs(t, l, "a") != 2 || refs(t, l, "b") != 1 {
		t.Fatalf("refs a=%d b=%d", refs(t, l, "a"), refs(t, l, "b"))
	}
	if _, err := l.PutObject(ctx, "b", "other", version("a"), Condition{}); err != nil {
		t.Fatal(err)
	}
	o, err := l.GetObject(ctx, "b", "k", "")
	if err != nil || o.Size != 3 || len(o.Nuggets) != 3 {
		t.Fatalf("get: %+v %v", o, err)
	}
	// Overwrite in an unversioned bucket replaces the null version.
	if _, err := l.PutObject(ctx, "b", "k", version("c"), Condition{}); err != nil {
		t.Fatal(err)
	}
	if refs(t, l, "a") != 1 || refs(t, l, "b") != 0 || refs(t, l, "c") != 1 {
		t.Fatalf("after overwrite refs a=%d b=%d c=%d", refs(t, l, "a"), refs(t, l, "b"), refs(t, l, "c"))
	}
	if _, err := l.DeleteObject(ctx, "b", "k", ""); err != nil {
		t.Fatal(err)
	}
	if refs(t, l, "c") != 0 {
		t.Fatal("delete did not drop refs")
	}
	if _, err := l.GetObject(ctx, "b", "k", ""); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("want ErrNoSuchKey, got %v", err)
	}
	if _, err := l.GetObject(ctx, "nope", "k", ""); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("want ErrNoSuchBucket, got %v", err)
	}
	if res, err := l.DeleteObject(ctx, "b", "k", ""); err != nil || res.VersionID != "" {
		t.Fatalf("delete missing: %+v %v", res, err)
	}
}

func TestVersioning(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	_ = l.CreateBucket(ctx, "v")
	if _, err := l.PutObject(ctx, "v", "k", version("pre"), Condition{}); err != nil {
		t.Fatal(err)
	}
	if err := l.SetVersioning(ctx, "v", VersioningEnabled); err != nil {
		t.Fatal(err)
	}
	r1, _ := l.PutObject(ctx, "v", "k", version("one"), Condition{})
	r2, _ := l.PutObject(ctx, "v", "k", version("two"), Condition{})
	if r1.VersionID == NullVersion || r1.VersionID == r2.VersionID {
		t.Fatalf("version ids %q %q", r1.VersionID, r2.VersionID)
	}
	if o, _ := l.GetObject(ctx, "v", "k", ""); o.VersionID != r2.VersionID {
		t.Fatalf("current = %s", o.VersionID)
	}
	if o, err := l.GetObject(ctx, "v", "k", NullVersion); err != nil || o.Size != 3 {
		t.Fatalf("null version kept: %+v %v", o, err)
	}
	if _, err := l.GetObject(ctx, "v", "k", "nope"); !errors.Is(err, ErrNoSuchVersion) {
		t.Fatalf("want ErrNoSuchVersion, got %v", err)
	}

	del, err := l.DeleteObject(ctx, "v", "k", "")
	if err != nil || !del.DeleteMarker || del.VersionID == "" {
		t.Fatalf("delete marker: %+v %v", del, err)
	}
	if o, err := l.GetObject(ctx, "v", "k", ""); !errors.Is(err, ErrNoSuchKey) || o == nil || !o.IsDeleteMarker {
		t.Fatalf("current should be a delete marker: %+v %v", o, err)
	}
	if ls, _ := l.ListObjects(ctx, "v", "", "", 10); len(ls) != 0 {
		t.Fatalf("deleted key still listed: %+v", ls)
	}
	if refs(t, l, "two") != 1 {
		t.Fatal("delete marker must not drop older versions' refs")
	}
	vs, err := l.ListVersions(ctx, "v", "", "", "", 10)
	if err != nil || len(vs) != 4 || !vs[0].IsDeleteMarker || !vs[0].IsLatest || vs[3].VersionID != NullVersion {
		t.Fatalf("versions = %+v %v", vs, err)
	}
	page, _ := l.ListVersions(ctx, "v", "", "k", vs[1].VersionID, 10)
	if len(page) != 2 || page[0].VersionID != vs[2].VersionID {
		t.Fatalf("version marker page = %+v", page)
	}

	// Removing the marker restores the previous version.
	if res, err := l.DeleteObject(ctx, "v", "k", del.VersionID); err != nil || !res.DeleteMarker {
		t.Fatalf("delete marker by id: %+v %v", res, err)
	}
	if o, err := l.GetObject(ctx, "v", "k", ""); err != nil || o.VersionID != r2.VersionID {
		t.Fatalf("after removing marker: %+v %v", o, err)
	}
	for _, id := range []string{r2.VersionID, r1.VersionID, NullVersion} {
		if _, err := l.DeleteObject(ctx, "v", "k", id); err != nil {
			t.Fatal(err)
		}
	}
	if vs, _ := l.ListVersions(ctx, "v", "", "", "", 10); len(vs) != 0 {
		t.Fatalf("versions left: %+v", vs)
	}
	if refs(t, l, "one")+refs(t, l, "two")+refs(t, l, "pre") != 0 {
		t.Fatal("refs leaked after deleting every version")
	}

	// Suspended: writes replace the null version.
	_ = l.SetVersioning(ctx, "v", VersioningSuspended)
	a, _ := l.PutObject(ctx, "v", "s", version("s1"), Condition{})
	b, _ := l.PutObject(ctx, "v", "s", version("s2"), Condition{})
	if a.VersionID != NullVersion || b.VersionID != NullVersion || refs(t, l, "s1") != 0 {
		t.Fatalf("suspended: %+v %+v refs=%d", a, b, refs(t, l, "s1"))
	}
}

func TestListObjects(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	_ = l.CreateBucket(ctx, "b")
	for _, k := range []string{"a/1", "a/2", "b", "B", "a/é"} {
		if _, err := l.PutObject(ctx, "b", k, version(k), Condition{}); err != nil {
			t.Fatal(err)
		}
	}
	all, err := l.ListObjects(ctx, "b", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"B", "a/1", "a/2", "a/é", "b"} // byte order
	if len(all) != len(want) {
		t.Fatalf("got %d keys", len(all))
	}
	for i := range want {
		if all[i].Key != want[i] {
			t.Fatalf("order: got %s at %d, want %s", all[i].Key, i, want[i])
		}
	}
	page, _ := l.ListObjects(ctx, "b", "a/", "a/1", 1)
	if len(page) != 1 || page[0].Key != "a/2" {
		t.Fatalf("page = %+v", page)
	}
}

func part(n int, data ...string) Part {
	v := version(data...)
	return Part{Number: n, Size: v.Size, ETag: fmt.Sprintf(`"%032x"`, n), Nuggets: v.Nuggets}
}

func TestMultipart(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	_ = l.CreateBucket(ctx, "m")
	if _, err := l.CreateUpload(ctx, "nope", "k", nil, ""); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("want ErrNoSuchBucket, got %v", err)
	}
	up, err := l.CreateUpload(ctx, "m", "k", map[string]string{"content-type": "text/x"}, "crc32")
	if err != nil {
		t.Fatal(err)
	}
	big := string(make([]byte, MinPartSize))
	if err := l.PutPart(ctx, "m", "k", up, part(1, "replaced")); err != nil {
		t.Fatal(err)
	}
	if err := l.PutPart(ctx, "m", "k", up, part(1, big)); err != nil {
		t.Fatal(err)
	}
	if refs(t, l, "replaced") != 0 {
		t.Fatal("replaced part kept its refs")
	}
	_ = l.PutPart(ctx, "m", "k", up, part(2, "tail"))
	_ = l.PutPart(ctx, "m", "k", up, part(3, "unused"))
	if err := l.PutPart(ctx, "m", "other", up, part(1, "x")); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("wrong key: want ErrNoSuchUpload, got %v", err)
	}

	_, parts, err := l.ListParts(ctx, "m", "k", up, 1, 10)
	if err != nil || len(parts) != 2 || parts[0].Number != 2 {
		t.Fatalf("list parts = %+v %v", parts, err)
	}
	if ups, _ := l.ListUploads(ctx, "m", "", "", "", 10); len(ups) != 1 || ups[0].UploadID != up {
		t.Fatalf("uploads = %+v", ups)
	}

	cp := func(n int) CompletePart { return CompletePart{Number: n, ETag: fmt.Sprintf("%032x", n)} }
	if _, _, err := l.CompleteUpload(ctx, "m", "k", up, []CompletePart{cp(2), cp(1)}, Condition{}); !errors.Is(err, ErrInvalidPartOrder) {
		t.Fatalf("want ErrInvalidPartOrder, got %v", err)
	}
	if _, _, err := l.CompleteUpload(ctx, "m", "k", up, []CompletePart{cp(1), {Number: 2, ETag: "bad"}}, Condition{}); !errors.Is(err, ErrInvalidPart) {
		t.Fatalf("want ErrInvalidPart, got %v", err)
	}
	if _, _, err := l.CompleteUpload(ctx, "m", "k", up, []CompletePart{cp(2), cp(3)}, Condition{}); !errors.Is(err, ErrEntityTooSmall) {
		t.Fatalf("want ErrEntityTooSmall, got %v", err)
	}
	etag, _, err := l.CompleteUpload(ctx, "m", "k", up, []CompletePart{cp(1), cp(2)}, Condition{})
	if err != nil || etag[len(etag)-3:] != `-2"` {
		t.Fatalf("complete: %s %v", etag, err)
	}
	o, err := l.GetObject(ctx, "m", "k", "")
	if err != nil || o.Size != int64(MinPartSize+4) || o.PartsCount != 2 || o.Headers["content-type"] != "text/x" || len(o.Nuggets) != 2 {
		t.Fatalf("object = %+v %v", o, err)
	}
	if refs(t, l, big) != 1 || refs(t, l, "tail") != 1 || refs(t, l, "unused") != 0 {
		t.Fatalf("refs after complete big=%d tail=%d unused=%d", refs(t, l, big), refs(t, l, "tail"), refs(t, l, "unused"))
	}
	if again, _, err := l.CompleteUpload(ctx, "m", "k", up, []CompletePart{cp(1), cp(2)}, Condition{}); err != nil || again != etag {
		t.Fatalf("retried complete: %s %v", again, err)
	}
	if err := l.PutPart(ctx, "m", "k", up, part(9, "late")); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("part after complete: %v", err)
	}
	if ups, _ := l.ListUploads(ctx, "m", "", "", "", 10); len(ups) != 0 {
		t.Fatalf("completed upload still listed: %+v", ups)
	}

	up2, _ := l.CreateUpload(ctx, "m", "k2", nil, "")
	_ = l.PutPart(ctx, "m", "k2", up2, part(1, "abort-me"))
	if err := l.AbortUpload(ctx, "m", "k2", up2); err != nil {
		t.Fatal(err)
	}
	if refs(t, l, "abort-me") != 0 {
		t.Fatal("abort kept part refs")
	}
	if err := l.AbortUpload(ctx, "m", "k2", up2); !errors.Is(err, ErrNoSuchUpload) {
		t.Fatalf("double abort: %v", err)
	}
}

// A referenced or recently touched nugget must never be reclaimed (rule 5).
func TestReclaimSafety(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	_ = l.CreateBucket(ctx, "g")
	_, _ = l.PutObject(ctx, "g", "live", version("live"), Condition{})
	_, _ = l.PutObject(ctx, "g", "gone", version("gone"), Condition{})
	_, _ = l.DeleteObject(ctx, "g", "gone", "")
	_ = l.TouchNuggets(ctx, []NuggetRef{{ID: nugget.Sum([]byte("inflight")), Length: 8}})

	future := time.Now().Add(time.Hour)
	ids, err := l.ReclaimCandidates(ctx, future, 10)
	if err != nil || len(ids) != 2 {
		t.Fatalf("candidates = %v %v", ids, err)
	}
	removed := 0
	remove := func() error { removed++; return nil }
	if ok, err := l.ReclaimNugget(ctx, nugget.Sum([]byte("live")), future, remove); ok || err != nil || removed != 0 {
		t.Fatalf("referenced nugget reclaimed: %v %v", ok, err)
	}
	// Grace window: nothing was touched before an hour ago.
	if ok, _ := l.ReclaimNugget(ctx, nugget.Sum([]byte("gone")), time.Now().Add(-time.Hour), remove); ok || removed != 0 {
		t.Fatal("nugget reclaimed inside the grace window")
	}
	if ok, err := l.ReclaimNugget(ctx, nugget.Sum([]byte("gone")), future, remove); !ok || err != nil || removed != 1 {
		t.Fatalf("unreferenced nugget not reclaimed: %v %v", ok, err)
	}
	failing := func() error { return errors.New("disk") }
	if ok, err := l.ReclaimNugget(ctx, nugget.Sum([]byte("inflight")), future, failing); ok || err == nil {
		t.Fatal("failed removal must keep the row")
	}
	if ids, _ := l.ReclaimCandidates(ctx, future, 10); len(ids) != 1 {
		t.Fatalf("row lost after failed removal: %v", ids)
	}
}
