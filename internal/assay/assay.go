// Package assay is the integrity scrubber that runs inside each vein. It
// continuously re-reads and re-hashes every stored nugget at a bounded rate
// and queues a repair for any copy that fails verification.
package assay

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/vein"
)

type Options struct {
	Store      *vein.Store
	Ledger     *ledger.Ledger
	VeinID     string
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Events     *events.Bus

	BytesPerSecond int64         // scrub rate; 0 = unlimited
	Pause          time.Duration // wait between full passes
}

type Assay struct {
	opts           Options
	log            *slog.Logger
	passes, found  prometheus.Counter
	scanned        prometheus.Counter
	lastPassFinish prometheus.Gauge
}

func New(opts Options) *Assay {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Registerer == nil {
		opts.Registerer = prometheus.NewRegistry()
	}
	if opts.Pause <= 0 {
		opts.Pause = time.Minute
	}
	f := promauto.With(opts.Registerer)
	c := func(name, help string) prometheus.Counter {
		return f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "assay", Name: name, Help: help})
	}
	return &Assay{
		opts: opts, log: opts.Logger,
		passes:  c("passes_total", "Completed scrub passes."),
		found:   c("corrupt_found_total", "Corrupt copies found and queued for repair."),
		scanned: c("bytes_scanned_total", "Bytes verified."),
		lastPassFinish: f.NewGauge(prometheus.GaugeOpts{Namespace: "lode", Subsystem: "assay", Name: "last_pass_timestamp_seconds",
			Help: "Unix time the last full pass finished."}),
	}
}

// Run scrubs until ctx is done.
func (a *Assay) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if _, err := a.Pass(ctx); err != nil && ctx.Err() == nil {
			a.log.Error("assay pass", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(a.opts.Pause):
		}
	}
}

// Pass scrubs every nugget once and returns how many were corrupt.
func (a *Assay) Pass(ctx context.Context) (corrupt int, err error) {
	scanned, err := a.opts.Store.Scrub(ctx, a.opts.BytesPerSecond, func(id nugget.ID) {
		corrupt++
		a.found.Inc()
		a.log.Warn("assay found corrupt copy", "nugget", id.String())
		a.opts.Events.Publish(events.Event{Type: "assay.corrupt", Component: "assay", Attrs: map[string]any{"nugget": id.String(), "vein": a.opts.VeinID}})
		if err := a.opts.Ledger.EnqueueRepair(ctx, id, ledger.ReasonCorrupt, a.opts.VeinID); err != nil {
			a.log.Error("queue repair", "nugget", id.String(), "err", err)
		}
	})
	a.scanned.Add(float64(scanned))
	if err == nil {
		a.passes.Inc()
		a.lastPassFinish.SetToCurrentTime()
	}
	return corrupt, err
}
