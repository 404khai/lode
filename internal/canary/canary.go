// Package canary is Lode's failure detector. It watches vein heartbeats in
// the Ledger and runs a phi-accrual detector per vein: phi grows with the
// time since the last heartbeat relative to the vein's own heartbeat
// history. A vein whose phi crosses the suspect threshold stops receiving
// writes; one that stays silent past DownAfter is marked down and every
// nugget it holds is queued for repair. Heartbeats resuming bring a suspect
// or down vein back to healthy. Draining veins are handed to Smelter.
package canary

import (
	"context"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
)

// LeaderLock is the advisory lock key held by the active Canary.
const LeaderLock = 0x6c6f6465_63616e61 // "lodecana"

type Options struct {
	Ledger     *ledger.Ledger
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Events     *events.Bus

	Interval         time.Duration // how often to evaluate veins
	SuspectPhi       float64       // phi above which a vein is suspect
	DownAfter        time.Duration // silence after which a vein is down
	MinStdDev        time.Duration // floor on heartbeat jitter, avoids hair triggers
	ExpectedInterval time.Duration // assumed heartbeat interval before history exists
	SweepEvery       time.Duration // how often to queue under-replicated nuggets as a safety net
	Now              func() time.Time
}

// detector is the phi-accrual state for one vein.
type detector struct {
	last      time.Time // last heartbeat timestamp seen
	intervals []float64 // recent inter-arrival times, seconds
}

const window = 100

func (d *detector) observe(hb time.Time) {
	if !d.last.IsZero() && hb.After(d.last) {
		d.intervals = append(d.intervals, hb.Sub(d.last).Seconds())
		if len(d.intervals) > window {
			d.intervals = d.intervals[1:]
		}
	}
	if hb.After(d.last) {
		d.last = hb
	}
}

// phi is -log10 of the probability that a heartbeat arrives later than
// elapsed, under a normal model of the observed intervals.
func (d *detector) phi(now time.Time, expected, minStd time.Duration) float64 {
	mean, std := expected.Seconds(), minStd.Seconds()
	if n := len(d.intervals); n >= 2 {
		var sum, sq float64
		for _, x := range d.intervals {
			sum += x
		}
		mean = sum / float64(n)
		for _, x := range d.intervals {
			sq += (x - mean) * (x - mean)
		}
		std = math.Max(math.Sqrt(sq/float64(n)), minStd.Seconds())
	}
	elapsed := now.Sub(d.last).Seconds()
	y := (elapsed - mean) / std
	// Logistic approximation of the normal CDF tail (Hayashibara et al.).
	e := math.Exp(-y * (1.5976 + 0.070566*y*y))
	if elapsed > mean {
		return -math.Log10(e / (1 + e))
	}
	return -math.Log10(1 - 1/(1+e))
}

type Canary struct {
	opts      Options
	log       *slog.Logger
	mu        sync.Mutex
	detectors map[string]*detector
	lastSweep time.Time

	phiGauge    *prometheus.GaugeVec
	stateGauge  *prometheus.GaugeVec
	transitions *prometheus.CounterVec
	underRep    prometheus.Gauge
	queued      prometheus.Gauge
	lost        prometheus.Gauge
}

func New(opts Options) *Canary {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Registerer == nil {
		opts.Registerer = prometheus.NewRegistry()
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Second
	}
	if opts.SuspectPhi <= 0 {
		opts.SuspectPhi = 8
	}
	if opts.DownAfter <= 0 {
		opts.DownAfter = 30 * time.Second
	}
	if opts.MinStdDev <= 0 {
		opts.MinStdDev = 500 * time.Millisecond
	}
	if opts.ExpectedInterval <= 0 {
		opts.ExpectedInterval = 2 * time.Second
	}
	if opts.SweepEvery <= 0 {
		opts.SweepEvery = time.Minute
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	f := promauto.With(opts.Registerer)
	return &Canary{
		opts: opts, log: opts.Logger, detectors: map[string]*detector{},
		phiGauge: f.NewGaugeVec(prometheus.GaugeOpts{Namespace: "lode", Subsystem: "canary", Name: "phi",
			Help: "Phi-accrual suspicion per vein."}, []string{"vein"}),
		stateGauge: f.NewGaugeVec(prometheus.GaugeOpts{Namespace: "lode", Subsystem: "canary", Name: "vein_state",
			Help: "1 for each vein's current state."}, []string{"vein", "state"}),
		transitions: f.NewCounterVec(prometheus.CounterOpts{Namespace: "lode", Subsystem: "canary", Name: "transitions_total",
			Help: "Vein state transitions."}, []string{"from", "to"}),
		underRep: f.NewGauge(prometheus.GaugeOpts{Namespace: "lode", Subsystem: "repair", Name: "under_replicated_nuggets",
			Help: "Referenced nuggets with fewer copies on healthy veins than wanted."}),
		queued: f.NewGauge(prometheus.GaugeOpts{Namespace: "lode", Subsystem: "repair", Name: "jobs_open",
			Help: "Repair jobs queued or leased."}),
		lost: f.NewGauge(prometheus.GaugeOpts{Namespace: "lode", Subsystem: "repair", Name: "jobs_lost",
			Help: "Repair jobs that found no readable copy."}),
	}
}

// Run evaluates veins until ctx is done while holding the leader lock.
func (c *Canary) Run(ctx context.Context) {
	t := time.NewTicker(c.opts.Interval)
	defer t.Stop()
	var release func()
	defer func() {
		if release != nil {
			release()
		}
	}()
	for {
		if release == nil {
			r, ok, err := c.opts.Ledger.TryLeader(ctx, LeaderLock)
			if err != nil && ctx.Err() == nil {
				c.log.Warn("canary leader election", "err", err)
			}
			if ok {
				release = r
				c.log.Info("canary is leader")
			}
		}
		if release != nil {
			if err := c.Tick(ctx); err != nil && ctx.Err() == nil {
				c.log.Error("canary tick", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick evaluates every vein once.
func (c *Canary) Tick(ctx context.Context) error {
	veins, err := c.opts.Ledger.ListVeins(ctx)
	if err != nil {
		return err
	}
	now := c.opts.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	// Heartbeats travel through the Ledger. When most veins go quiet at the
	// same moment the likely culprit is that shared path (or this process),
	// not the veins, so new suspicions are held until it recovers.
	type eval struct {
		phi    float64
		silent time.Duration
	}
	evals := make(map[string]eval, len(veins))
	healthy, wouldSuspect := 0, 0
	for _, v := range veins {
		d := c.detectors[v.ID]
		if d == nil {
			d = &detector{}
			c.detectors[v.ID] = d
		}
		d.observe(v.LastHeartbeat)
		e := eval{d.phi(now, c.opts.ExpectedInterval, c.opts.MinStdDev), now.Sub(v.LastHeartbeat)}
		evals[v.ID] = e
		if v.State == ledger.VeinHealthy {
			healthy++
			if c.suspicious(e.phi, e.silent) {
				wouldSuspect++
			}
		}
	}
	correlated := wouldSuspect >= 2 && wouldSuspect*2 > healthy
	if correlated {
		c.log.Warn("heartbeats delayed across the cluster; holding suspicion", "veins", healthy, "silent", wouldSuspect)
	}
	for _, v := range veins {
		phi, silent := evals[v.ID].phi, evals[v.ID].silent
		c.phiGauge.WithLabelValues(v.ID).Set(math.Min(phi, 1000))

		next := v.State
		switch v.State {
		case ledger.VeinHealthy:
			if silent >= c.opts.DownAfter && !correlated {
				next = ledger.VeinDown
			} else if c.suspicious(phi, silent) && !correlated {
				next = ledger.VeinSuspect
			}
		case ledger.VeinSuspect:
			if silent >= c.opts.DownAfter {
				next = ledger.VeinDown
			} else if phi < c.opts.SuspectPhi/2 {
				next = ledger.VeinHealthy
			}
		case ledger.VeinDown:
			if phi < c.opts.SuspectPhi/2 && silent < c.opts.DownAfter {
				next = ledger.VeinHealthy
			}
		case ledger.VeinDraining:
			if err := c.drain(ctx, v); err != nil {
				return err
			}
		}
		if next != v.State {
			if err := c.transition(ctx, v, next, phi, silent); err != nil {
				return err
			}
		}
		for _, s := range []string{ledger.VeinHealthy, ledger.VeinSuspect, ledger.VeinDown, ledger.VeinDraining} {
			val := 0.0
			if s == next {
				val = 1
			}
			c.stateGauge.WithLabelValues(v.ID, s).Set(val)
		}
	}
	if now.Sub(c.lastSweep) >= c.opts.SweepEvery {
		c.lastSweep = now
		if n, err := c.opts.Ledger.EnqueueUnderReplicated(ctx, 10000); err != nil {
			return err
		} else if n > 0 {
			c.log.Info("queued under-replicated nuggets", "count", n)
		}
	}
	st, err := c.opts.Ledger.RepairStats(ctx)
	if err != nil {
		return err
	}
	c.underRep.Set(float64(st.UnderReplicated))
	c.queued.Set(float64(st.Queued + st.Leased))
	c.lost.Set(float64(st.Lost))
	return nil
}

// suspicious reports whether a healthy vein should stop taking writes: phi
// over the threshold and silent for at least three heartbeat intervals.
func (c *Canary) suspicious(phi float64, silent time.Duration) bool {
	return phi >= c.opts.SuspectPhi && silent >= 3*c.opts.ExpectedInterval
}

func (c *Canary) transition(ctx context.Context, v ledger.VeinInfo, to string, phi float64, silent time.Duration) error {
	if _, err := c.opts.Ledger.SetVeinState(ctx, v.ID, to); err != nil {
		return err
	}
	c.transitions.WithLabelValues(v.State, to).Inc()
	c.log.Warn("vein state changed", "vein", v.ID, "from", v.State, "to", to, "phi", phi, "silent", silent.Round(time.Millisecond))
	c.opts.Events.Publish(events.Event{Type: "vein.state", Component: "canary", Attrs: map[string]any{
		"vein": v.ID, "from": v.State, "to": to, "phi": math.Min(phi, 1000), "silent_seconds": silent.Seconds(),
	}})
	if to == ledger.VeinDown {
		n, err := c.opts.Ledger.EnqueueVeinRepairs(ctx, v.ID, ledger.ReasonVeinDown)
		if err != nil {
			return err
		}
		c.log.Warn("queued repairs for down vein", "vein", v.ID, "jobs", n)
		c.opts.Events.Publish(events.Event{Type: "repair.queued", Component: "canary", Attrs: map[string]any{
			"vein": v.ID, "reason": ledger.ReasonVeinDown, "jobs": n,
		}})
	}
	return nil
}

// drain queues copies off a draining vein and retires it once the Ledger
// records no replicas on it.
func (c *Canary) drain(ctx context.Context, v ledger.VeinInfo) error {
	left, err := c.opts.Ledger.VeinReplicaCount(ctx, v.ID)
	if err != nil {
		return err
	}
	if left == 0 {
		if err := c.opts.Ledger.RemoveVein(ctx, v.ID, false); err != nil {
			return err
		}
		c.log.Info("vein drained and removed", "vein", v.ID)
		c.opts.Events.Publish(events.Event{Type: "vein.state", Component: "canary", Attrs: map[string]any{
			"vein": v.ID, "from": ledger.VeinDraining, "to": ledger.VeinRemoved,
		}})
		return nil
	}
	_, err = c.opts.Ledger.EnqueueVeinRepairs(ctx, v.ID, ledger.ReasonDrain)
	return err
}
