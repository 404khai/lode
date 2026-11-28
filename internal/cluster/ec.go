package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/404khai/lode/internal/ec"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/survey"
)

// StoreEC writes the shards of id that have no copy on an available vein,
// each on a different vein, and succeeds once at least quorum distinct shards
// are durable (default k+1). It returns the shards newly written.
func (c *Client) StoreEC(ctx context.Context, id nugget.ID, data []byte, existing []ledger.ShardLoc, k, m, quorum int,
	placer survey.Strategy) ([]ledger.ShardLoc, error) {
	if quorum <= 0 {
		quorum = min(k+1, k+m)
	}
	avail := c.Available()
	isAvail := map[string]bool{}
	for _, n := range avail {
		isAvail[n.ID] = true
	}
	held := map[int]bool{}    // shard indices with a copy on an available vein
	busy := map[string]bool{} // veins already holding some shard of id
	for _, s := range existing {
		busy[s.Vein] = true
		if isAvail[s.Vein] {
			held[s.Index] = true
		}
	}
	if len(held) == k+m {
		c.writes.WithLabelValues("deduplicated").Inc()
		return nil, nil
	}
	shards, err := ec.Encode(id, data, k, m)
	if err != nil {
		return nil, err
	}
	var targets []survey.Node
	for _, n := range placer.Place(id, len(avail), avail) {
		if !busy[n.ID] {
			targets = append(targets, n)
		}
	}
	type job struct {
		shard ec.Shard
		vein  string
	}
	var jobs []job
	t := 0
	for _, sh := range shards {
		if held[sh.Index] {
			continue
		}
		if t == len(targets) {
			break // fewer veins than shards: the rest is left for repair
		}
		jobs = append(jobs, job{sh, targets[t].ID})
		t++
	}
	results := make(chan struct{}, len(jobs))
	var mu sync.Mutex
	var written []ledger.ShardLoc
	var errs []error
	for _, j := range jobs {
		go func(j job) {
			err := c.Put(ctx, j.vein, j.shard.ID, j.shard.Blob)
			mu.Lock()
			if err != nil {
				errs = append(errs, fmt.Errorf("shard %d on %s: %w", j.shard.Index, j.vein, err))
			} else {
				written = append(written, ledger.ShardLoc{Index: j.shard.Index, ID: j.shard.ID, Vein: j.vein})
			}
			mu.Unlock()
			results <- struct{}{}
		}(j)
	}
	for range jobs {
		<-results
	}
	total := len(held) + len(written)
	switch {
	case total < quorum:
		c.writes.WithLabelValues("failed").Inc()
		return written, fmt.Errorf("%w: %d of %d shards stored (need %d)%s", ErrInsufficientReplicas, total, k+m, quorum, joinErrs(errs))
	case total < k+m:
		c.writes.WithLabelValues("degraded").Inc()
		c.opts.Events.Publish(events.Event{Type: "nugget.under_replicated", Component: "adit",
			Attrs: map[string]any{"nugget": id.String(), "shards": total, "want": k + m}})
		if c.opts.OnUnderReplicated != nil {
			c.opts.OnUnderReplicated(ctx, id)
		}
	default:
		c.writes.WithLabelValues("full").Inc()
	}
	return written, nil
}

// shardResult is the outcome of fetching one shard index.
type shardResult struct {
	index   int
	data    []byte
	corrupt int
	err     error
}

// fetchShard reads shard idx of ref from the best of its locations.
func (c *Client) fetchShard(ctx context.Context, ref ledger.NuggetRef, idx int, locs []ledger.ShardLoc, wait bool) shardResult {
	byVein := map[string]ledger.ShardLoc{}
	veins := make([]string, 0, len(locs))
	for _, l := range locs {
		byVein[l.Vein] = l
		veins = append(veins, l.Vein)
	}
	res := shardResult{index: idx}
	var errs []error
	for _, v := range c.readOrder(veins) {
		loc := byVein[v]
		b, err := c.get(ctx, v, loc.ID, -1, wait)
		if err == nil {
			data, perr := ec.ParseBlob(b, ref.ID, idx)
			if perr == nil {
				res.data = data
				return res
			}
			err = status.Error(codes.DataLoss, perr.Error())
		}
		if status.Code(err) == codes.DataLoss {
			res.corrupt++
			c.opts.Events.Publish(events.Event{Type: "nugget.corrupt", Component: "adit",
				Attrs: map[string]any{"nugget": ref.ID.String(), "shard": idx, "vein": v}})
			if c.opts.OnCorrupt != nil {
				c.opts.OnCorrupt(ctx, loc.ID, v)
			}
		}
		errs = append(errs, fmt.Errorf("%s: %w", v, err))
		if ctx.Err() != nil {
			break
		}
	}
	res.err = fmt.Errorf("shard %d: %w", idx, errors.Join(errs...))
	if len(errs) == 0 {
		res.err = fmt.Errorf("shard %d: no location", idx)
	}
	return res
}

// ReadShards fetches shards of ref until want of them are in hand (all of
// them when want <= 0), data shards first. The result is indexed by shard
// number with nil for missing ones.
func (c *Client) ReadShards(ctx context.Context, ref ledger.NuggetRef, want int) ([][]byte, int, error) {
	k, m := ref.ECData, ref.ECParity
	if want <= 0 {
		want = k + m
	}
	locs := make([][]ledger.ShardLoc, k+m)
	for _, s := range ref.Shards {
		if s.Index >= 0 && s.Index < k+m {
			locs[s.Index] = append(locs[s.Index], s)
		}
	}
	out := make([][]byte, k+m)
	have, corrupt := 0, 0
	var errs []error
	var failed []int
	fetchWith := func(indices []int, wait bool) {
		ch := make(chan shardResult, len(indices))
		for _, i := range indices {
			go func(i int) { ch <- c.fetchShard(ctx, ref, i, locs[i], wait) }(i)
		}
		for range indices {
			r := <-ch
			corrupt += r.corrupt
			if r.err != nil {
				errs = append(errs, r.err)
				if r.corrupt == 0 {
					failed = append(failed, r.index)
				}
				continue
			}
			out[r.index] = r.data
			have++
		}
	}
	fetch := func(indices []int) { fetchWith(indices, false) }
	// Data shards first: when they are all readable no decoding is needed.
	first := make([]int, 0, k)
	for i := 0; i < k; i++ {
		first = append(first, i)
	}
	fetch(first)
	for next := k; have < want && next < k+m && ctx.Err() == nil; {
		batch := []int{}
		for ; next < k+m && len(batch) < want-have; next++ {
			batch = append(batch, next)
		}
		if have < k && len(batch) > 0 {
			c.failovers.Inc()
		}
		fetch(batch)
	}
	// Every shard may have failed fast on a vein that is just reconnecting:
	// give the unreachable ones one patient retry.
	if have < want && len(failed) > 0 && ctx.Err() == nil {
		retry := failed
		failed, errs = nil, nil
		fetchWith(retry, true)
	}
	if have < want {
		return out, corrupt, errors.Join(errs...)
	}
	return out, corrupt, nil
}

// readEC reconstructs ref from any k of its shards.
func (c *Client) readEC(ctx context.Context, ref ledger.NuggetRef) ([]byte, error) {
	shards, corrupt, err := c.ReadShards(ctx, ref, ref.ECData)
	if err != nil {
		if corrupt > 0 {
			return nil, fmt.Errorf("%w: nugget %s: %w", ErrCorrupt, ref.ID, err)
		}
		return nil, fmt.Errorf("%w: nugget %s: %w", ErrUnavailable, ref.ID, err)
	}
	data, err := ec.Decode(ref.ID, ref.Length, ref.ECData, ref.ECParity, shards)
	if err != nil {
		return nil, fmt.Errorf("%w: nugget %s: %w", ErrCorrupt, ref.ID, err)
	}
	return data, nil
}
