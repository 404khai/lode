// Package tailings is Lode's garbage collector. Each pass it aborts expired
// multipart uploads and removes, from every vein holding a replica, nuggets
// the Ledger reports unreferenced for longer than the grace window. Veins
// compact their own segments. Passes are idempotent; only the process
// holding the Tailings leader lock runs them.
package tailings

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
)

// Remover deletes nugget replicas from veins.
type Remover interface {
	Delete(ctx context.Context, vein string, id nugget.ID) error
}

// LeaderLock is the advisory lock key held by the active Tailings worker.
const LeaderLock = 0x6c6f6465_7461696c // "lodetail"

const batchSize = 500

type Options struct {
	Ledger     *ledger.Ledger
	Veins      Remover
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Events     *events.Bus

	Grace        time.Duration // unreferenced nuggets older than this are reclaimed
	Interval     time.Duration // time between passes
	UploadExpiry time.Duration // multipart uploads older than this are aborted; 0 disables
	Now          func() time.Time
}

// Stats summarises one pass.
type Stats struct {
	UploadsAborted   int
	NuggetsReclaimed int
	NuggetsDeferred  int // unreferenced but a replica's vein could not be reached
}

type Worker struct {
	opts                       Options
	log                        *slog.Logger
	passes, reclaimed, aborted prometheus.Counter
	deferred                   prometheus.Counter
	lastPass                   prometheus.Gauge
}

func New(opts Options) *Worker {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Registerer == nil {
		opts.Registerer = prometheus.NewRegistry()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Minute
	}
	f := promauto.With(opts.Registerer)
	counter := func(name, help string) prometheus.Counter {
		return f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "tailings", Name: name, Help: help})
	}
	return &Worker{
		opts:      opts,
		log:       opts.Logger,
		passes:    counter("passes_total", "Completed GC passes."),
		reclaimed: counter("nuggets_reclaimed_total", "Unreferenced nuggets removed."),
		aborted:   counter("uploads_aborted_total", "Expired multipart uploads aborted."),
		deferred:  counter("nuggets_deferred_total", "Reclaims postponed because a replica's vein was unreachable."),
		lastPass: f.NewGauge(prometheus.GaugeOpts{Namespace: "lode", Subsystem: "tailings", Name: "last_pass_timestamp_seconds",
			Help: "Unix time of the last completed pass."}),
	}
}

// Run executes passes until ctx is cancelled, whenever this process holds
// the leader lock.
func (w *Worker) Run(ctx context.Context) {
	t := time.NewTicker(w.opts.Interval)
	defer t.Stop()
	for {
		release, ok, err := w.opts.Ledger.TryLeader(ctx, LeaderLock)
		switch {
		case err != nil:
			w.log.Warn("tailings leader election", "err", err)
		case ok:
			if _, err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
				w.log.Error("tailings pass", "err", err)
			}
			release()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunOnce executes a single pass.
func (w *Worker) RunOnce(ctx context.Context) (Stats, error) {
	var st Stats
	now := w.opts.Now()

	if w.opts.UploadExpiry > 0 {
		for {
			ups, err := w.opts.Ledger.ExpiredUploads(ctx, now.Add(-w.opts.UploadExpiry), batchSize)
			if err != nil {
				return st, err
			}
			for _, u := range ups {
				if err := w.opts.Ledger.AbortUpload(ctx, u.Bucket, u.Key, u.UploadID); err != nil {
					return st, err
				}
				st.UploadsAborted++
				w.aborted.Inc()
				w.emit("upload.expired", map[string]any{"bucket": u.Bucket, "key": u.Key, "upload_id": u.UploadID})
			}
			if len(ups) < batchSize {
				break
			}
		}
	}

	if _, err := w.opts.Ledger.PurgeCompletedUploads(ctx, now.Add(-time.Hour)); err != nil {
		return st, err
	}

	cutoff := now.Add(-w.opts.Grace)
	for {
		ids, err := w.opts.Ledger.ReclaimCandidates(ctx, cutoff, batchSize)
		if err != nil {
			return st, err
		}
		progress := 0
		for _, id := range ids {
			ok, err := w.opts.Ledger.ReclaimNugget(ctx, id, cutoff, func(veins []string) error {
				for _, v := range veins {
					if err := w.opts.Veins.Delete(ctx, v, id); err != nil {
						return &deferredError{err}
					}
				}
				return nil
			})
			var de *deferredError
			if errors.As(err, &de) {
				st.NuggetsDeferred++
				w.deferred.Inc()
				w.log.Debug("reclaim deferred", "nugget", id.String(), "err", de.err)
				continue
			}
			if err != nil {
				return st, err
			}
			if ok {
				progress++
				st.NuggetsReclaimed++
				w.reclaimed.Inc()
			}
		}
		if len(ids) < batchSize || progress == 0 {
			break
		}
	}

	w.passes.Inc()
	w.lastPass.Set(float64(w.opts.Now().Unix()))
	if st != (Stats{}) {
		w.log.Info("tailings pass", "uploads_aborted", st.UploadsAborted, "nuggets_reclaimed", st.NuggetsReclaimed,
			"nuggets_deferred", st.NuggetsDeferred)
		w.emit("gc.pass", map[string]any{"uploads_aborted": st.UploadsAborted, "nuggets_reclaimed": st.NuggetsReclaimed,
			"nuggets_deferred": st.NuggetsDeferred})
	}
	return st, nil
}

func (w *Worker) emit(typ string, attrs map[string]any) {
	w.opts.Events.Publish(events.Event{Type: typ, Component: "tailings", Attrs: attrs})
}

// deferredError marks a reclaim that failed because a vein was unreachable;
// the nugget is retried on a later pass.
type deferredError struct{ err error }

func (e *deferredError) Error() string { return e.err.Error() }
func (e *deferredError) Unwrap() error { return e.err }
