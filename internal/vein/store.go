// Package vein implements a storage node's local nugget store: append-only
// segment files on disk indexed by a Pebble database.
//
// On-disk layout under the data directory:
//
//	index/            Pebble DB: "n/"+id -> segment(u32) offset(u64) length(u32) refs(u32)
//	segments/NNNNNNNN.seg  raw nugget bytes, appended and never rewritten
//
// A nugget's bytes are fsynced before its index entry is written, so an index
// entry always points at durable data. Bytes orphaned by a crash between the
// two steps are reclaimed by segment compaction (Tailings).
package vein

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/nugget"
)

var (
	// ErrNotFound means the vein holds no nugget with that ID.
	ErrNotFound = errors.New("vein: nugget not found")
	// ErrCorrupt means the stored bytes no longer hash to the nugget ID.
	ErrCorrupt = errors.New("vein: nugget corrupt")
)

const (
	defaultSegmentSize = 1 << 30
	entrySize          = 20
	keyPrefix          = "n/"
)

type entry struct {
	segment uint32
	offset  uint64
	length  uint32
	refs    uint32
}

func (e entry) encode() []byte {
	b := make([]byte, entrySize)
	binary.BigEndian.PutUint32(b[0:], e.segment)
	binary.BigEndian.PutUint64(b[4:], e.offset)
	binary.BigEndian.PutUint32(b[12:], e.length)
	binary.BigEndian.PutUint32(b[16:], e.refs)
	return b
}

func decodeEntry(b []byte) (entry, error) {
	if len(b) != entrySize {
		return entry{}, fmt.Errorf("vein: bad index entry length %d", len(b))
	}
	return entry{
		segment: binary.BigEndian.Uint32(b[0:]),
		offset:  binary.BigEndian.Uint64(b[4:]),
		length:  binary.BigEndian.Uint32(b[12:]),
		refs:    binary.BigEndian.Uint32(b[16:]),
	}, nil
}

func indexKey(id nugget.ID) []byte { return append([]byte(keyPrefix), id[:]...) }

// Options configure a Store.
type Options struct {
	Dir         string
	SegmentSize int64 // rotate the active segment past this size; 0 = 1 GiB
	Logger      *slog.Logger
	Registerer  prometheus.Registerer
	Events      *events.Bus
}

// Store is a single-disk nugget store. It is safe for concurrent use.
type Store struct {
	dir     string
	segSize int64
	log     *slog.Logger
	bus     *events.Bus
	db      *pebble.DB

	mu      sync.Mutex // serialises writes and segment rotation
	active  *os.File
	activeN uint32
	size    int64

	rmu     sync.RWMutex
	readers map[uint32]*os.File

	puts, dedups, gets, corrupt, releases prometheus.Counter
	bytesWritten                          prometheus.Counter
}

// Open opens or creates a store in opts.Dir.
func Open(opts Options) (*Store, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Registerer == nil {
		opts.Registerer = prometheus.NewRegistry()
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = defaultSegmentSize
	}
	segDir := filepath.Join(opts.Dir, "segments")
	if err := os.MkdirAll(segDir, 0o750); err != nil {
		return nil, fmt.Errorf("vein: %w", err)
	}
	db, err := pebble.Open(filepath.Join(opts.Dir, "index"), &pebble.Options{Logger: pebbleLogger{opts.Logger}})
	if err != nil {
		return nil, fmt.Errorf("vein: open index: %w", err)
	}
	f := promauto.With(opts.Registerer)
	counter := func(name, help string) prometheus.Counter {
		return f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "vein", Name: name, Help: help})
	}
	s := &Store{
		dir:          opts.Dir,
		segSize:      opts.SegmentSize,
		log:          opts.Logger,
		bus:          opts.Events,
		db:           db,
		readers:      make(map[uint32]*os.File),
		puts:         counter("nugget_puts_total", "Nuggets written to disk."),
		dedups:       counter("nugget_dedup_total", "Puts satisfied by an existing nugget."),
		gets:         counter("nugget_gets_total", "Nugget reads."),
		corrupt:      counter("nugget_corrupt_total", "Reads that failed hash verification."),
		releases:     counter("nugget_releases_total", "Nugget references released."),
		bytesWritten: counter("bytes_written_total", "Nugget bytes appended to segments."),
	}
	if err := s.openActive(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) segPath(n uint32) string {
	return filepath.Join(s.dir, "segments", fmt.Sprintf("%08d.seg", n))
}

func (s *Store) openActive() error {
	names, err := filepath.Glob(filepath.Join(s.dir, "segments", "*.seg"))
	if err != nil {
		return err
	}
	var last uint32
	nums := make([]int, 0, len(names))
	for _, name := range names {
		n, err := strconv.Atoi(strings.TrimSuffix(filepath.Base(name), ".seg"))
		if err == nil {
			nums = append(nums, n)
		}
	}
	sort.Ints(nums)
	if len(nums) > 0 {
		last = uint32(nums[len(nums)-1]) // #nosec G115 -- segment numbers are written by us
	} else {
		last = 1
	}
	return s.useSegment(last)
}

func (s *Store) useSegment(n uint32) error {
	f, err := os.OpenFile(s.segPath(n), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("vein: open segment: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if s.active != nil {
		if err := s.active.Close(); err != nil {
			s.log.Warn("close segment", "err", err)
		}
	}
	s.active, s.activeN, s.size = f, n, st.Size()
	return nil
}

func (s *Store) lookup(id nugget.ID) (entry, error) {
	v, closer, err := s.db.Get(indexKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return entry{}, ErrNotFound
	}
	if err != nil {
		return entry{}, err
	}
	defer func() { _ = closer.Close() }()
	return decodeEntry(v)
}

// Put stores data under id, or adds a reference if it is already stored.
// data must hash to id.
func (s *Store) Put(id nugget.ID, data []byte) error {
	if nugget.Sum(data) != id {
		return fmt.Errorf("vein: data does not match nugget id %s", id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	e, err := s.lookup(id)
	switch {
	case err == nil:
		e.refs++
		s.dedups.Inc()
		return s.db.Set(indexKey(id), e.encode(), pebble.Sync)
	case !errors.Is(err, ErrNotFound):
		return err
	}

	if s.size > 0 && s.size+int64(len(data)) > s.segSize {
		if err := s.useSegment(s.activeN + 1); err != nil {
			return err
		}
	}
	off := s.size
	if _, err := s.active.Write(data); err != nil {
		return fmt.Errorf("vein: append: %w", err)
	}
	if err := s.active.Sync(); err != nil {
		return fmt.Errorf("vein: fsync: %w", err)
	}
	s.size += int64(len(data))
	e = entry{segment: s.activeN, offset: uint64(off), length: uint32(len(data)), refs: 1} // #nosec G115 -- nuggets are bounded
	if err := s.db.Set(indexKey(id), e.encode(), pebble.Sync); err != nil {
		return fmt.Errorf("vein: index: %w", err)
	}
	s.puts.Inc()
	s.bytesWritten.Add(float64(len(data)))
	return nil
}

func (s *Store) reader(n uint32) (*os.File, error) {
	s.rmu.RLock()
	f := s.readers[n]
	s.rmu.RUnlock()
	if f != nil {
		return f, nil
	}
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if f = s.readers[n]; f != nil {
		return f, nil
	}
	f, err := os.Open(s.segPath(n))
	if err != nil {
		return nil, fmt.Errorf("vein: open segment: %w", err)
	}
	s.readers[n] = f
	return f, nil
}

// Get returns the nugget's bytes after verifying them against id. It never
// returns bytes that fail verification.
func (s *Store) Get(id nugget.ID) ([]byte, error) {
	e, err := s.lookup(id)
	if err != nil {
		return nil, err
	}
	s.gets.Inc()
	f, err := s.reader(e.segment)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, e.length)
	if _, err := f.ReadAt(buf, int64(e.offset)); err != nil { // #nosec G115 -- offsets come from our index
		if errors.Is(err, io.EOF) {
			return nil, s.markCorrupt(id, "short read")
		}
		return nil, fmt.Errorf("vein: read: %w", err)
	}
	if nugget.Sum(buf) != id {
		return nil, s.markCorrupt(id, "hash mismatch")
	}
	return buf, nil
}

func (s *Store) markCorrupt(id nugget.ID, reason string) error {
	s.corrupt.Inc()
	s.log.Error("nugget corrupt", "nugget", id.String(), "reason", reason)
	s.bus.Publish(events.Event{Type: "nugget.corrupt", Component: "vein", Attrs: map[string]any{"nugget": id.String(), "reason": reason}})
	return fmt.Errorf("%w: %s (%s)", ErrCorrupt, id, reason)
}

// Release drops one reference to id. When the last reference goes the index
// entry is removed; the bytes stay in the segment until compaction.
func (s *Store) Release(id nugget.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.lookup(id)
	if err != nil {
		return err
	}
	s.releases.Inc()
	if e.refs > 1 {
		e.refs--
		return s.db.Set(indexKey(id), e.encode(), pebble.Sync)
	}
	return s.db.Delete(indexKey(id), pebble.Sync)
}

// Refs reports the reference count of id (0 if absent).
func (s *Store) Refs(id nugget.ID) (uint32, error) {
	e, err := s.lookup(id)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	return e.refs, err
}

// Close flushes and closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rmu.Lock()
	for _, f := range s.readers {
		_ = f.Close()
	}
	s.readers = nil
	s.rmu.Unlock()
	errs := []error{s.db.Close()}
	if s.active != nil {
		errs = append(errs, s.active.Close())
	}
	return errors.Join(errs...)
}

// pebbleLogger routes Pebble's logging through slog.
type pebbleLogger struct{ log *slog.Logger }

func (l pebbleLogger) Infof(format string, args ...any)  { l.log.Debug(fmt.Sprintf(format, args...)) }
func (l pebbleLogger) Errorf(format string, args ...any) { l.log.Error(fmt.Sprintf(format, args...)) }
func (l pebbleLogger) Fatalf(format string, args ...any) {
	l.log.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}
