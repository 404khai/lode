package vein

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/404khai/lode/internal/nugget"
)

func open(t *testing.T, dir string, segSize int64) *Store {
	t.Helper()
	s, err := Open(Options{Dir: dir, SegmentSize: segSize})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutGet(t *testing.T) {
	s := open(t, t.TempDir(), 0)
	defer func() { _ = s.Close() }()
	data := []byte("the quick brown fox")
	id := nugget.Sum(data)
	if err := s.Put(id, data); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(id)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("get = %q, %v", got, err)
	}
	if _, err := s.Get(nugget.Sum([]byte("nope"))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if err := s.Put(id, []byte("wrong")); err == nil {
		t.Fatal("put with mismatched id must fail")
	}
}

// A nugget referenced by another object must survive the release of one
// reference (AGENTS.md rule 5).
func TestReferencedNuggetSurvivesRelease(t *testing.T) {
	s := open(t, t.TempDir(), 0)
	defer func() { _ = s.Close() }()
	data := []byte("shared")
	id := nugget.Sum(data)
	for range 2 {
		if err := s.Put(id, data); err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := s.Refs(id); n != 2 {
		t.Fatalf("refs = %d", n)
	}
	if err := s.Release(id); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(id); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("referenced nugget lost: %q %v", got, err)
	}
	if err := s.Release(id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after last release, got %v", err)
	}
	if err := s.Release(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("release of absent nugget: %v", err)
	}
}

func TestCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 0)
	defer func() { _ = s.Close() }()
	data := bytes.Repeat([]byte("x"), 1024)
	id := nugget.Sum(data)
	if err := s.Put(id, data); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(s.segPath(1), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("y"), 100); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	got, err := s.Get(id)
	if !errors.Is(err, ErrCorrupt) || got != nil {
		t.Fatalf("want ErrCorrupt and no bytes, got %d bytes, %v", len(got), err)
	}
}

func TestRotationAndReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 64)
	var ids []nugget.ID
	for i := range 5 {
		d := bytes.Repeat([]byte{byte('a' + i)}, 40)
		id := nugget.Sum(d)
		ids = append(ids, id)
		if err := s.Put(id, d); err != nil {
			t.Fatal(err)
		}
	}
	if s.activeN != 5 {
		t.Fatalf("expected 5 segments, active=%d", s.activeN)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir, 64)
	defer func() { _ = s.Close() }()
	for i, id := range ids {
		got, err := s.Get(id)
		if err != nil || got[0] != byte('a'+i) {
			t.Fatalf("nugget %d after reopen: %v", i, err)
		}
	}
	d := []byte("after reopen")
	if err := s.Put(nugget.Sum(d), d); err != nil {
		t.Fatal(err)
	}
}
