// Package smelter repairs redundancy. Workers lease jobs from the Ledger's
// repair queue (most urgent first), read a verified copy of the nugget,
// write new copies to veins chosen by Survey until the nugget has its wanted
// number of copies on healthy veins, and drop copies that are corrupt or on
// draining veins. Jobs are idempotent: re-running one that already
// succeeded finds nothing to do.
package smelter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/survey"
)

type Options struct {
	Ledger     *ledger.Ledger
	Cluster    *cluster.Client
	Logger     *slog.Logger
	Registerer prometheus.Registerer
	Events     *events.Bus

	Owner              string        // lease owner, unique per process
	Workers            int           // concurrent repairs
	BandwidthBytes     int64         // repair write budget per second across workers; 0 = unlimited
	Lease              time.Duration // job lease length
	DefaultReplication int           // for nuggets that predate per-nugget replication
	MaxAttempts        int           // after this many failed reads a job is marked lost
	Poll               time.Duration // idle wait between queue polls
}

type Smelter struct {
	opts    Options
	log     *slog.Logger
	limiter *limiter

	repaired *prometheus.CounterVec
	copies   prometheus.Counter
	bytes    prometheus.Counter
}

func New(opts Options) *Smelter {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Registerer == nil {
		opts.Registerer = prometheus.NewRegistry()
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Lease <= 0 {
		opts.Lease = time.Minute
	}
	if opts.DefaultReplication <= 0 {
		opts.DefaultReplication = 3
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 20
	}
	if opts.Poll <= 0 {
		opts.Poll = 500 * time.Millisecond
	}
	f := promauto.With(opts.Registerer)
	return &Smelter{
		opts: opts, log: opts.Logger, limiter: newLimiter(opts.BandwidthBytes),
		repaired: f.NewCounterVec(prometheus.CounterOpts{Namespace: "lode", Subsystem: "smelter", Name: "jobs_total",
			Help: "Repair jobs finished, by reason and outcome."}, []string{"reason", "outcome"}),
		copies: f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "smelter", Name: "copies_written_total",
			Help: "Replica copies written by repair."}),
		bytes: f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "smelter", Name: "bytes_written_total",
			Help: "Bytes written by repair."}),
	}
}

// Run processes jobs until ctx is done.
func (s *Smelter) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := range s.opts.Workers {
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			for ctx.Err() == nil {
				did, err := s.RunOne(ctx, owner)
				if err != nil && ctx.Err() == nil {
					s.log.Error("smelter", "err", err)
				}
				if !did {
					select {
					case <-ctx.Done():
					case <-time.After(s.opts.Poll):
					}
				}
			}
		}(fmt.Sprintf("%s/%d", s.opts.Owner, i))
	}
	wg.Wait()
}

// RunOne leases and processes a single job; did is false when the queue
// had nothing runnable.
func (s *Smelter) RunOne(ctx context.Context, owner string) (did bool, err error) {
	job, err := s.opts.Ledger.LeaseRepair(ctx, owner, s.opts.Lease)
	if err != nil || job == nil {
		return false, err
	}
	outcome, msg, perr := s.process(ctx, job)
	switch {
	case perr != nil && job.Attempts >= s.opts.MaxAttempts:
		outcome, msg = "lost", perr.Error()
		s.log.Error("repair gave up", "nugget", job.Nugget.String(), "reason", job.Reason, "attempts", job.Attempts, "err", perr)
		s.opts.Events.Publish(events.Event{Type: "nugget.lost", Component: "smelter",
			Attrs: map[string]any{"nugget": job.Nugget.String(), "error": perr.Error()}})
		err = s.opts.Ledger.FinishRepair(ctx, job.ID, owner, "lost", msg)
	case perr != nil:
		outcome = "retry"
		backoff := min(time.Duration(job.Attempts)*time.Second, time.Minute)
		s.log.Warn("repair retry", "nugget", job.Nugget.String(), "reason", job.Reason, "attempt", job.Attempts, "err", perr)
		err = s.opts.Ledger.RetryRepair(ctx, job.ID, owner, perr.Error(), backoff)
	default:
		err = s.opts.Ledger.FinishRepair(ctx, job.ID, owner, "done", msg)
	}
	s.repaired.WithLabelValues(job.Reason, outcome).Inc()
	return true, err
}

var errNotEnoughVeins = errors.New("smelter: not enough healthy veins to place copies")

func (s *Smelter) process(ctx context.Context, job *ledger.RepairJob) (outcome, msg string, err error) {
	st, ok, err := s.opts.Ledger.Nugget(ctx, job.Nugget)
	if err != nil {
		return "", "", err
	}
	if !ok || st.Refs == 0 {
		return "unreferenced", "", nil
	}
	if st.ECData > 0 {
		return s.processEC(ctx, job, st)
	}
	want := st.Replication
	if want <= 0 {
		want = s.opts.DefaultReplication
	}

	states := map[string]string{}
	for _, v := range s.opts.Cluster.Veins() {
		states[v.ID] = v.State
	}
	bad := map[string]bool{}
	for _, v := range job.BadVeins {
		bad[v] = true
	}
	avail := map[string]bool{}
	for _, n := range s.opts.Cluster.Available() {
		avail[n.ID] = true
	}
	var good, draining, sources []string
	for _, v := range st.Replicas {
		if bad[v] {
			continue
		}
		sources = append(sources, v)
		switch {
		case states[v] == ledger.VeinHealthy && avail[v]:
			good = append(good, v)
		case states[v] == ledger.VeinDraining:
			draining = append(draining, v)
		}
	}

	written := 0
	holdingNew := map[string]bool{} // veins given a fresh copy by this job
	if len(good) < want {
		if len(sources) == 0 {
			return "", "", fmt.Errorf("smelter: no copy of %s left to read", job.Nugget)
		}
		data, err := s.opts.Cluster.Read(ctx, ledger.NuggetRef{ID: job.Nugget, Length: st.Length, Replicas: sources})
		if err != nil {
			return "", "", err
		}
		holding := map[string]bool{}
		for _, v := range st.Replicas {
			holding[v] = !bad[v]
		}
		// A vein whose copy is corrupt gets a fresh one: drop the bad copy
		// first so the rewrite is not mistaken for a duplicate.
		for v := range bad {
			if err := s.opts.Cluster.Delete(ctx, v, job.Nugget); err != nil {
				s.log.Debug("delete corrupt copy", "vein", v, "nugget", job.Nugget.String(), "err", err)
			}
		}
		nodes := s.opts.Cluster.Available()
		for _, n := range survey.Place(job.Nugget, len(nodes), nodes) {
			if len(good)+written >= want {
				break
			}
			if holding[n.ID] {
				continue
			}
			if err := s.limiter.wait(ctx, int64(len(data))); err != nil {
				return "", "", err
			}
			if err := s.opts.Cluster.Put(ctx, n.ID, job.Nugget, data); err != nil {
				s.log.Warn("repair copy failed", "vein", n.ID, "nugget", job.Nugget.String(), "err", err)
				continue
			}
			if err := s.opts.Ledger.AddReplicas(ctx, job.Nugget, []string{n.ID}); err != nil {
				return "", "", err
			}
			holding[n.ID] = true
			holdingNew[n.ID] = true
			written++
			s.copies.Inc()
			s.bytes.Add(float64(len(data)))
		}
		if len(good)+written < want {
			return "", "", fmt.Errorf("%w: %d of %d copies", errNotEnoughVeins, len(good)+written, want)
		}
	}
	// Full redundancy now exists without them: forget corrupt copies (kept
	// until now so the bytes survive if nothing could be repaired) and
	// release copies held by draining veins.
	for v := range bad {
		if holdingNew[v] {
			continue // rewritten with good bytes above
		}
		if err := s.opts.Ledger.RemoveReplica(ctx, job.Nugget, v); err != nil {
			return "", "", err
		}
		if err := s.opts.Cluster.Delete(ctx, v, job.Nugget); err != nil {
			s.log.Debug("delete corrupt copy", "vein", v, "nugget", job.Nugget.String(), "err", err)
		}
	}
	for _, v := range draining {
		if err := s.opts.Ledger.RemoveReplica(ctx, job.Nugget, v); err != nil {
			return "", "", err
		}
		if err := s.opts.Cluster.Delete(ctx, v, job.Nugget); err != nil {
			s.log.Debug("delete drained copy", "vein", v, "nugget", job.Nugget.String(), "err", err)
		}
	}
	if written > 0 || len(bad) > 0 || len(draining) > 0 {
		s.opts.Events.Publish(events.Event{Type: "repair.done", Component: "smelter", Attrs: map[string]any{
			"nugget": job.Nugget.String(), "reason": job.Reason, "copies_written": written,
			"dropped_corrupt": len(bad), "released_draining": len(draining),
		}})
	}
	return "repaired", "", nil
}

// limiter is a simple shared byte-rate limiter.
type limiter struct {
	rate int64
	mu   sync.Mutex
	next time.Time
}

func newLimiter(rate int64) *limiter { return &limiter{rate: rate} }

func (l *limiter) wait(ctx context.Context, n int64) error {
	if l.rate <= 0 {
		return nil
	}
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	at := l.next
	l.next = l.next.Add(time.Duration(float64(n) / float64(l.rate) * float64(time.Second)))
	l.mu.Unlock()
	if d := time.Until(at); d > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
		}
	}
	return nil
}
