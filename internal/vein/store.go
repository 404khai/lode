// Package vein implements a storage node's local nugget store: append-only
// segment files on disk indexed by a Pebble database.
//
// On-disk layout under the data directory (ADR 0001):
//
//	index/            Pebble DB: "n/"+id -> segment(u32) offset(u64) length(u32) refs(u32)
//	segments/NNNNNNNN.seg  raw nugget bytes, appended and never rewritten
//
// A nugget's bytes are fsynced before its index entry is written, so an index
// entry always points at durable data. The Ledger owns reference counts
// (ADR 0002): the vein only records presence, and nuggets are removed by
// Tailings. Space held by removed or crash-orphaned bytes is reclaimed by
// Compact, which rewrites live nuggets out of mostly-dead segments.
package vein

import (
	"context"
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
	"sync/atomic"
	"time"

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
	// ErrClosed means the store has been closed.
	ErrClosed = errors.New("vein: store closed")
)

const (
	defaultSegmentSize = 1 << 30
	entrySize          = 20
	keyPrefix          = "n/"
)

// entry is an index value. refs is always 1 since format v1 moved reference
// counting to the Ledger; the field is kept for format compatibility.
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

	// life guards the store's lifetime: every operation holds it shared and
	// Close holds it exclusively, so nothing touches the index after Close.
	life   sync.RWMutex
	closed bool

	mu      sync.Mutex // serialises writes, deletes, rotation and compaction
	active  *os.File
	activeN uint32
	size    int64

	rmu     sync.RWMutex
	readers map[uint32]*os.File

	puts, dedups, gets, corrupt, deletes prometheus.Counter
	bytesWritten, bytesReclaimed         prometheus.Counter
	ioErrors                             prometheus.Counter

	ioErrorCount, corruptCount atomic.Int64
}

// Health reports cumulative I/O errors and corrupt nuggets seen by this store.
func (s *Store) Health() (ioErrors, corrupt int64) {
	return s.ioErrorCount.Load(), s.corruptCount.Load()
}

func (s *Store) ioError(err error) error {
	s.ioErrors.Inc()
	s.ioErrorCount.Add(1)
	return err
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
		dir:            opts.Dir,
		segSize:        opts.SegmentSize,
		log:            opts.Logger,
		bus:            opts.Events,
		db:             db,
		readers:        make(map[uint32]*os.File),
		puts:           counter("nugget_puts_total", "Nuggets written to disk."),
		dedups:         counter("nugget_dedup_total", "Puts satisfied by an existing nugget."),
		gets:           counter("nugget_gets_total", "Nugget reads."),
		corrupt:        counter("nugget_corrupt_total", "Reads that failed hash verification."),
		deletes:        counter("nugget_deletes_total", "Nuggets removed from the index."),
		bytesWritten:   counter("bytes_written_total", "Nugget bytes appended to segments."),
		bytesReclaimed: counter("bytes_reclaimed_total", "Segment bytes freed by compaction."),
		ioErrors:       counter("io_errors_total", "Disk read/write/fsync errors."),
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

func (s *Store) segments() ([]uint32, error) {
	names, err := filepath.Glob(filepath.Join(s.dir, "segments", "*.seg"))
	if err != nil {
		return nil, err
	}
	var nums []uint32
	for _, name := range names {
		n, err := strconv.ParseUint(strings.TrimSuffix(filepath.Base(name), ".seg"), 10, 32)
		if err == nil {
			nums = append(nums, uint32(n))
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

func (s *Store) openActive() error {
	nums, err := s.segments()
	if err != nil {
		return err
	}
	last := uint32(1)
	if len(nums) > 0 {
		last = nums[len(nums)-1]
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

// enter starts an operation; the returned func ends it.
func (s *Store) enter() (func(), error) {
	s.life.RLock()
	if s.closed {
		s.life.RUnlock()
		return nil, ErrClosed
	}
	return s.life.RUnlock, nil
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

// Put stores data under id. Storing a nugget that is already present is a
// no-op. data must hash to id.
func (s *Store) Put(id nugget.ID, data []byte) error {
	leave, err := s.enter()
	if err != nil {
		return err
	}
	defer leave()
	if nugget.Sum(data) != id {
		return fmt.Errorf("vein: data does not match nugget id %s", id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err = s.lookup(id)
	switch {
	case err == nil:
		s.dedups.Inc()
		return nil
	case !errors.Is(err, ErrNotFound):
		return err
	}
	e, err := s.appendLocked(data)
	if err != nil {
		return err
	}
	if err := s.db.Set(indexKey(id), e.encode(), pebble.Sync); err != nil {
		return fmt.Errorf("vein: index: %w", err)
	}
	s.puts.Inc()
	return nil
}

// appendLocked appends data to the active segment and fsyncs it. s.mu must be held.
func (s *Store) appendLocked(data []byte) (entry, error) {
	if s.size > 0 && s.size+int64(len(data)) > s.segSize {
		if err := s.useSegment(s.activeN + 1); err != nil {
			return entry{}, err
		}
	}
	off := s.size
	if _, err := s.active.Write(data); err != nil {
		return entry{}, s.ioError(fmt.Errorf("vein: append: %w", err))
	}
	if err := s.active.Sync(); err != nil {
		return entry{}, s.ioError(fmt.Errorf("vein: fsync: %w", err))
	}
	s.size += int64(len(data))
	s.bytesWritten.Add(float64(len(data)))
	return entry{segment: s.activeN, offset: uint64(off), length: uint32(len(data)), refs: 1}, nil // #nosec G115 -- nuggets are bounded
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
	leave, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer leave()
	s.gets.Inc()
	for attempt := 0; ; attempt++ {
		buf, err := s.read(id)
		// Compaction may have moved the nugget between lookup and read.
		if attempt == 0 && (errors.Is(err, os.ErrNotExist) || errors.Is(err, os.ErrClosed)) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if nugget.Sum(buf) != id {
			return nil, s.markCorrupt(id, "hash mismatch")
		}
		return buf, nil
	}
}

func (s *Store) read(id nugget.ID) ([]byte, error) {
	e, err := s.lookup(id)
	if err != nil {
		return nil, err
	}
	f, err := s.reader(e.segment)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, e.length)
	if _, err := f.ReadAt(buf, int64(e.offset)); err != nil { // #nosec G115 -- offsets come from our index
		if errors.Is(err, io.EOF) {
			return nil, s.markCorrupt(id, "short read")
		}
		return nil, s.ioError(fmt.Errorf("vein: read: %w", err))
	}
	return buf, nil
}

func (s *Store) markCorrupt(id nugget.ID, reason string) error {
	s.corrupt.Inc()
	s.corruptCount.Add(1)
	s.log.Error("nugget corrupt", "nugget", id.String(), "reason", reason)
	s.bus.Publish(events.Event{Type: "nugget.corrupt", Component: "vein", Attrs: map[string]any{"nugget": id.String(), "reason": reason}})
	return fmt.Errorf("%w: %s (%s)", ErrCorrupt, id, reason)
}

// Delete removes id from the index. Deleting an absent nugget is not an
// error. Only Tailings calls this, after the Ledger confirms the nugget is
// unreferenced. The bytes stay in the segment until Compact.
func (s *Store) Delete(id nugget.ID) error {
	leave, err := s.enter()
	if err != nil {
		return err
	}
	defer leave()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.lookup(id); errors.Is(err, ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	s.deletes.Inc()
	return s.db.Delete(indexKey(id), pebble.Sync)
}

// Has reports whether id is stored.
func (s *Store) Has(id nugget.ID) (bool, error) {
	leave, err := s.enter()
	if err != nil {
		return false, err
	}
	defer leave()
	_, err = s.lookup(id)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// CompactStats reports what a compaction did.
type CompactStats struct {
	SegmentsRemoved int
	BytesReclaimed  int64
	NuggetsMoved    int
}

// Compact rewrites the live nuggets of every sealed segment whose dead
// fraction is at least minDead (0..1) into the active segment, then removes
// the old segment. Live nuggets are copied, fsynced and re-indexed before
// their old segment is deleted; a segment holding a corrupt live nugget is
// left untouched so Assay and Smelter can deal with it.
func (s *Store) Compact(minDead float64) (CompactStats, error) {
	var st CompactStats
	leave, err := s.enter()
	if err != nil {
		return st, err
	}
	defer leave()
	s.mu.Lock()
	defer s.mu.Unlock()

	live := map[uint32][]nugget.ID{}
	liveBytes := map[uint32]int64{}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(keyPrefix), UpperBound: []byte("n0")})
	if err != nil {
		return st, err
	}
	for it.First(); it.Valid(); it.Next() {
		e, derr := decodeEntry(it.Value())
		if derr != nil {
			_ = it.Close()
			return st, derr
		}
		id, ierr := nugget.FromBytes(it.Key()[len(keyPrefix):])
		if ierr != nil {
			_ = it.Close()
			return st, ierr
		}
		live[e.segment] = append(live[e.segment], id)
		liveBytes[e.segment] += int64(e.length)
	}
	if err := it.Close(); err != nil {
		return st, err
	}

	nums, err := s.segments()
	if err != nil {
		return st, err
	}
	for _, n := range nums {
		if n == s.activeN {
			continue
		}
		fi, err := os.Stat(s.segPath(n))
		if err != nil {
			return st, err
		}
		size := fi.Size()
		if size == 0 || float64(size-liveBytes[n])/float64(size) < minDead {
			continue
		}
		ok, err := s.evacuateLocked(n, live[n])
		if err != nil {
			return st, err
		}
		if !ok {
			continue
		}
		s.rmu.Lock()
		if f := s.readers[n]; f != nil {
			_ = f.Close()
			delete(s.readers, n)
		}
		s.rmu.Unlock()
		if err := os.Remove(s.segPath(n)); err != nil {
			return st, err
		}
		reclaimed := size - liveBytes[n]
		st.SegmentsRemoved++
		st.NuggetsMoved += len(live[n])
		st.BytesReclaimed += reclaimed
		s.bytesReclaimed.Add(float64(reclaimed))
	}
	return st, nil
}

// evacuateLocked copies segment n's live nuggets to the active segment. ok is
// false (and nothing changes) if any of them is corrupt.
func (s *Store) evacuateLocked(n uint32, ids []nugget.ID) (bool, error) {
	data := make([][]byte, len(ids))
	for i, id := range ids {
		b, err := s.read(id)
		if errors.Is(err, ErrCorrupt) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if nugget.Sum(b) != id {
			s.log.Warn("compaction skipped segment with corrupt nugget", "segment", n, "nugget", id.String())
			return false, nil
		}
		data[i] = b
	}
	batch := s.db.NewBatch()
	defer func() { _ = batch.Close() }()
	for i, id := range ids {
		e, err := s.appendLocked(data[i])
		if err != nil {
			return false, err
		}
		if err := batch.Set(indexKey(id), e.encode(), nil); err != nil {
			return false, err
		}
	}
	return true, batch.Commit(pebble.Sync)
}

// Close flushes and closes the store. Operations after Close return ErrClosed.
func (s *Store) Close() error {
	s.life.Lock()
	defer s.life.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
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

// Scrub reads and verifies every stored nugget, calling corrupt for each one
// that fails verification. limit, if set, paces the scan to that many bytes
// per second. It stops early when ctx is done.
func (s *Store) Scrub(ctx context.Context, limit int64, corrupt func(nugget.ID)) (scanned int64, err error) {
	leave, err := s.enter()
	if err != nil {
		return 0, err
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(keyPrefix), UpperBound: []byte("n0")})
	if err != nil {
		return 0, err
	}
	var ids []nugget.ID
	for it.First(); it.Valid(); it.Next() {
		if id, ierr := nugget.FromBytes(it.Key()[len(keyPrefix):]); ierr == nil {
			ids = append(ids, id)
		}
	}
	cerr := it.Close()
	leave() // Get below re-enters per nugget, so Close is never blocked for a whole pass
	if cerr != nil {
		return 0, cerr
	}
	start := time.Now()
	for _, id := range ids {
		if ctx.Err() != nil {
			return scanned, ctx.Err()
		}
		data, err := s.Get(id)
		switch {
		case errors.Is(err, ErrCorrupt):
			corrupt(id)
		case errors.Is(err, ErrNotFound):
			continue // deleted since the scan started
		case err != nil:
			return scanned, err
		}
		scanned += int64(len(data))
		if limit > 0 {
			if ahead := time.Duration(float64(scanned)/float64(limit)*float64(time.Second)) - time.Since(start); ahead > 0 {
				select {
				case <-ctx.Done():
					return scanned, ctx.Err()
				case <-time.After(ahead):
				}
			}
		}
	}
	return scanned, nil
}

// pebbleLogger routes Pebble's logging through slog.
type pebbleLogger struct{ log *slog.Logger }

func (l pebbleLogger) Infof(format string, args ...any)  { l.log.Debug(fmt.Sprintf(format, args...)) }
func (l pebbleLogger) Errorf(format string, args ...any) { l.log.Error(fmt.Sprintf(format, args...)) }
func (l pebbleLogger) Fatalf(format string, args ...any) {
	l.log.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}
