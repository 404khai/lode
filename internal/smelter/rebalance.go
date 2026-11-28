package smelter

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/404khai/lode/internal/cluster"
	"github.com/404khai/lode/internal/ec"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/survey"
)

// RebalanceLock is the advisory lock key held by the active rebalancer.
const RebalanceLock = 0x6c6f6465_7265626c // "lodorebl"

// RebalanceOptions configure a Rebalancer.
type RebalanceOptions struct {
	Ledger             *ledger.Ledger
	Cluster            *cluster.Client
	Logger             *slog.Logger
	Registerer         prometheus.Registerer
	Events             *events.Bus
	Strategy           survey.Strategy // must be Stable
	DefaultReplication int
	BandwidthBytes     int64
	Interval           time.Duration // how often to check whether the vein set changed
}

// Rebalancer moves copies and shards to where the placement strategy now
// wants them after veins join or leave. With rendezvous placement only the
// nuggets that rank a new vein in their top set move, about 1/N of the data.
// A copy is removed only after its replacement is durable and recorded.
type Rebalancer struct {
	opts    RebalanceOptions
	log     *slog.Logger
	limiter *limiter
	moved   prometheus.Counter
	passes  prometheus.Counter
	lastSet string
}

// RebalanceStats summarises a pass.
type RebalanceStats struct {
	Checked int
	Moved   int // copies or shards moved to a new vein
}

func NewRebalancer(opts RebalanceOptions) *Rebalancer {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.Registerer == nil {
		opts.Registerer = prometheus.NewRegistry()
	}
	if opts.Strategy == nil {
		opts.Strategy, _ = survey.ByName("", nil)
	}
	if opts.DefaultReplication <= 0 {
		opts.DefaultReplication = 3
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Minute
	}
	f := promauto.With(opts.Registerer)
	return &Rebalancer{
		opts: opts, log: opts.Logger, limiter: newLimiter(opts.BandwidthBytes),
		moved: f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "rebalance", Name: "moved_total",
			Help: "Copies and shards moved by the rebalancer."}),
		passes: f.NewCounter(prometheus.CounterOpts{Namespace: "lode", Subsystem: "rebalance", Name: "passes_total",
			Help: "Completed rebalance passes."}),
	}
}

func veinSet(nodes []survey.Node) string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// Run rebalances whenever the set of available veins changes and stays
// stable for a full interval, while holding the rebalance leader lock.
func (r *Rebalancer) Run(ctx context.Context) {
	if !r.opts.Strategy.Stable() {
		r.log.Info("rebalancer disabled: placement strategy is not stable", "strategy", r.opts.Strategy.Name())
		return
	}
	t := time.NewTicker(r.opts.Interval)
	defer t.Stop()
	var seen string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		set := veinSet(r.opts.Cluster.Available())
		if set != seen { // wait one more interval for the set to settle
			seen = set
			continue
		}
		if set == r.lastSet {
			continue
		}
		release, ok, err := r.opts.Ledger.TryLeader(ctx, RebalanceLock)
		if err != nil || !ok {
			continue
		}
		st, err := r.RunOnce(ctx)
		release()
		if err != nil {
			r.log.Error("rebalance", "err", err)
			continue
		}
		r.lastSet = set
		r.log.Info("rebalance pass", "checked", st.Checked, "moved", st.Moved, "veins", set)
	}
}

// RunOnce checks every referenced nugget once.
func (r *Rebalancer) RunOnce(ctx context.Context) (RebalanceStats, error) {
	var st RebalanceStats
	nodes := r.opts.Cluster.Available()
	avail := map[string]bool{}
	for _, n := range nodes {
		avail[n.ID] = true
	}
	var after nugget.ID
	for {
		page, err := r.opts.Ledger.ListNuggets(ctx, after, 500)
		if err != nil {
			return st, err
		}
		for _, n := range page {
			after = n.ID
			st.Checked++
			var moved int
			if n.ECData > 0 {
				moved, err = r.moveShards(ctx, n, nodes, avail)
			} else {
				moved, err = r.moveReplicas(ctx, n, nodes, avail)
			}
			if err != nil {
				r.log.Warn("rebalance nugget", "nugget", n.ID.String(), "err", err)
				continue
			}
			st.Moved += moved
		}
		if len(page) < 500 {
			break
		}
	}
	r.passes.Inc()
	if st.Moved > 0 {
		r.opts.Events.Publish(events.Event{Type: "rebalance.pass", Component: "rebalancer",
			Attrs: map[string]any{"checked": st.Checked, "moved": st.Moved}})
	}
	return st, nil
}

func (r *Rebalancer) moveReplicas(ctx context.Context, n ledger.NuggetEntry, nodes []survey.Node, avail map[string]bool) (int, error) {
	want := n.Replication
	if want <= 0 {
		want = r.opts.DefaultReplication
	}
	current := map[string]bool{}
	healthy := 0
	for _, v := range n.Replicas {
		current[v] = true
		if avail[v] {
			healthy++
		}
	}
	if healthy < want {
		return 0, nil // under-replicated: repair's job, not ours
	}
	desired := map[string]bool{}
	var missing []string
	for _, d := range r.opts.Strategy.Place(n.ID, want, nodes) {
		desired[d.ID] = true
		if !current[d.ID] {
			missing = append(missing, d.ID)
		}
	}
	if len(missing) == 0 {
		return 0, nil
	}
	data, err := r.opts.Cluster.Read(ctx, ledger.NuggetRef{ID: n.ID, Length: n.Length, Replicas: n.Replicas})
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, v := range missing {
		if err := r.limiter.wait(ctx, int64(len(data))); err != nil {
			return moved, err
		}
		if err := r.opts.Cluster.Put(ctx, v, n.ID, data); err != nil {
			return moved, err
		}
		if err := r.opts.Ledger.AddReplicas(ctx, n.ID, []string{v}); err != nil {
			return moved, err
		}
		moved++
		r.moved.Inc()
	}
	// Every desired vein now holds a copy: drop the ones placement no longer wants.
	for _, v := range n.Replicas {
		if desired[v] || !avail[v] {
			continue
		}
		if err := r.opts.Ledger.RemoveReplica(ctx, n.ID, v); err != nil {
			return moved, err
		}
		_ = r.opts.Cluster.Delete(ctx, v, n.ID)
	}
	return moved, nil
}

func (r *Rebalancer) moveShards(ctx context.Context, n ledger.NuggetEntry, nodes []survey.Node, avail map[string]bool) (int, error) {
	k, m := n.ECData, n.ECParity
	held := map[int]bool{}
	busy := map[string]bool{}
	for _, s := range n.Shards {
		busy[s.Vein] = true
		if avail[s.Vein] {
			held[s.Index] = true
		}
	}
	if len(held) < k+m {
		return 0, nil // degraded: repair first
	}
	desired := map[string]bool{}
	var free []string
	for _, d := range r.opts.Strategy.Place(n.ID, k+m, nodes) {
		desired[d.ID] = true
		if !busy[d.ID] {
			free = append(free, d.ID)
		}
	}
	moved := 0
	for _, s := range n.Shards {
		if desired[s.Vein] || !avail[s.Vein] || len(free) == 0 {
			continue
		}
		target := free[0]
		blob, err := r.opts.Cluster.Get(ctx, s.Vein, s.ID, -1)
		if err != nil {
			return moved, err
		}
		if _, err := ec.ParseBlob(blob, n.ID, s.Index); err != nil {
			return moved, err
		}
		if err := r.limiter.wait(ctx, int64(len(blob))); err != nil {
			return moved, err
		}
		if err := r.opts.Cluster.Put(ctx, target, s.ID, blob); err != nil {
			return moved, err
		}
		if err := r.opts.Ledger.AddShards(ctx, n.ID, []ledger.ShardLoc{{Index: s.Index, ID: s.ID, Vein: target}}); err != nil {
			return moved, err
		}
		if err := r.opts.Ledger.RemoveShard(ctx, n.ID, s.Index, s.Vein); err != nil {
			return moved, err
		}
		_ = r.opts.Cluster.Delete(ctx, s.Vein, s.ID)
		free = free[1:]
		moved++
		r.moved.Inc()
	}
	return moved, nil
}
