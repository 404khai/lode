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

func TestBuckets(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	if err := l.CreateBucket(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if err := l.CreateBucket(ctx, "b1"); !errors.Is(err, ErrBucketExists) {
		t.Fatalf("want ErrBucketExists, got %v", err)
	}
	if _, err := l.PutObject(ctx, "b1", "k", version("x")); err != nil {
		t.Fatal(err)
	}
	if err := l.DeleteBucket(ctx, "b1"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("want ErrBucketNotEmpty, got %v", err)
	}
	if _, err := l.DeleteObject(ctx, "b1", "k"); err != nil {
		t.Fatal(err)
	}
	if err := l.DeleteBucket(ctx, "b1"); err != nil {
		t.Fatal(err)
	}
	if err := l.DeleteBucket(ctx, "b1"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("want ErrNoSuchBucket, got %v", err)
	}
	if bs, err := l.ListBuckets(ctx); err != nil || len(bs) != 0 {
		t.Fatalf("buckets = %v %v", bs, err)
	}
}

func TestObjectLifecycle(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	if _, err := l.PutObject(ctx, "missing", "k", version("x")); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("want ErrNoSuchBucket, got %v", err)
	}
	_ = l.CreateBucket(ctx, "b")

	released, err := l.PutObject(ctx, "b", "k", version("a", "b"))
	if err != nil || len(released) != 0 {
		t.Fatalf("first put: %v %v", released, err)
	}
	o, err := l.GetObject(ctx, "b", "k")
	if err != nil {
		t.Fatal(err)
	}
	if o.Size != 2 || len(o.Nuggets) != 2 || o.Nuggets[0].ID != nugget.Sum([]byte("a")) || o.Headers["content-type"] != "text/plain" {
		t.Fatalf("unexpected object %+v", o)
	}

	released, err = l.PutObject(ctx, "b", "k", version("c"))
	if err != nil || len(released) != 2 {
		t.Fatalf("overwrite should release old nuggets: %v %v", released, err)
	}
	if o, _ = l.GetObject(ctx, "b", "k"); o.Size != 1 {
		t.Fatalf("overwrite not visible: %+v", o)
	}

	released, err = l.DeleteObject(ctx, "b", "k")
	if err != nil || len(released) != 1 || released[0] != nugget.Sum([]byte("c")) {
		t.Fatalf("delete: %v %v", released, err)
	}
	if _, err := l.GetObject(ctx, "b", "k"); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("want ErrNoSuchKey, got %v", err)
	}
	if _, err := l.GetObject(ctx, "nope", "k"); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("want ErrNoSuchBucket, got %v", err)
	}
	if released, err = l.DeleteObject(ctx, "b", "k"); err != nil || released != nil {
		t.Fatalf("delete missing: %v %v", released, err)
	}
}

func TestListObjects(t *testing.T) {
	l := newTestLedger(t)
	ctx := context.Background()
	_ = l.CreateBucket(ctx, "b")
	for _, k := range []string{"a/1", "a/2", "b", "B", "a/é"} {
		if _, err := l.PutObject(ctx, "b", k, version(k)); err != nil {
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
