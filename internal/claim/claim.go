// Package claim enforces quotas. Enforcement happens in the Ledger, inside
// every write transaction; this package runs the background job that
// reconciles usage counters against the versions actually stored.
package claim

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/ledger"
)

// LeaderLock is the advisory lock key held by the active reconciler.
const LeaderLock = 0x6c6f6465_636c616d // "lodeclam"

type Reconciler struct {
	ledger   *ledger.Ledger
	log      *slog.Logger
	interval time.Duration
	fixed    prometheus.Counter
}

func NewReconciler(l *ledger.Ledger, log *slog.Logger, reg prometheus.Registerer, interval time.Duration) *Reconciler {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	return &Reconciler{ledger: l, log: log, interval: interval,
		fixed: promauto.With(reg).NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "claim",
			Name: "counters_corrected_total", Help: "Usage counters corrected by reconciliation."})}
}

// Run reconciles periodically while holding the leader lock.
func (r *Reconciler) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		release, ok, err := r.ledger.TryLeader(ctx, LeaderLock)
		if err != nil || !ok {
			continue
		}
		n, err := r.ledger.ReconcileUsage(ctx)
		release()
		if err != nil {
			r.log.Error("usage reconciliation", "err", err)
			continue
		}
		if n > 0 {
			r.fixed.Add(float64(n))
			r.log.Warn("usage counters corrected", "counters", n)
		}
	}
}
