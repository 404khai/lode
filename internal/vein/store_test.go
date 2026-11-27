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

func TestPutIsIdempotentAndDelete(t *testing.T) {
	s := open(t, t.TempDir(), 0)
	defer func() { _ = s.Close() }()
	data := []byte("shared")
	id := nugget.Sum(data)
	for range 2 {
		if err := s.Put(id, data); err != nil {
			t.Fatal(err)
		}
	}
	if s.size != int64(len(data)) {
		t.Fatalf("duplicate put wrote bytes: size=%d", s.size)
	}
	if ok, _ := s.Has(id); !ok {
		t.Fatal("missing after put")
	}
	if err := s.Delete(id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
	if err := s.Delete(id); err != nil {
		t.Fatalf("delete of absent nugget: %v", err)
	}
}

// Compaction must never lose a live nugget (AGENTS.md rule 5).
func TestCompactKeepsLiveNuggets(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 100)
	defer func() { _ = s.Close() }()
	var live, dead []nugget.ID
	for i := range 6 {
		d := bytes.Repeat([]byte{byte('a' + i)}, 60) // one nugget per segment
		id := nugget.Sum(d)
		if err := s.Put(id, d); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			dead = append(dead, id)
		} else {
			live = append(live, id)
		}
	}
	// A segment holding one dead and one live nugget.
	mixedLive, mixedDead := bytes.Repeat([]byte("L"), 40), bytes.Repeat([]byte("D"), 40)
	_ = s.useSegment(s.activeN + 1)
	for _, d := range [][]byte{mixedLive, mixedDead} {
		if err := s.Put(nugget.Sum(d), d); err != nil {
			t.Fatal(err)
		}
	}
	live = append(live, nugget.Sum(mixedLive))
	dead = append(dead, nugget.Sum(mixedDead))
	_ = s.useSegment(s.activeN + 1)

	for _, id := range dead {
		if err := s.Delete(id); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := s.segments()
	st, err := s.Compact(0.4)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := s.segments()
	if st.SegmentsRemoved != 4 || len(after) >= len(before) || st.BytesReclaimed != 3*60+40 {
		t.Fatalf("stats = %+v, segments %d -> %d", st, len(before), len(after))
	}
	for _, id := range live {
		if _, err := s.Get(id); err != nil {
			t.Fatalf("live nugget lost by compaction: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir, 100)
	for _, id := range live {
		if _, err := s.Get(id); err != nil {
			t.Fatalf("live nugget lost after reopen: %v", err)
		}
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
