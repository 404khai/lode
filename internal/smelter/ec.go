package smelter

import (
	"context"
	"fmt"

	"github.com/404khai/lode/internal/ec"
	"github.com/404khai/lode/internal/events"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/survey"
)

// processEC restores an erasure-coded nugget to k+m shards on distinct
// healthy veins: it reads any k shards, reconstructs the missing ones,
// writes them to veins holding no other shard of the nugget, and only then
// drops shards that are corrupt or on draining veins.
func (s *Smelter) processEC(ctx context.Context, job *ledger.RepairJob, st ledger.NuggetState) (string, string, error) {
	k, m := st.ECData, st.ECParity
	states := map[string]string{}
	for _, v := range s.opts.Cluster.Veins() {
		states[v.ID] = v.State
	}
	avail := map[string]bool{}
	for _, n := range s.opts.Cluster.Available() {
		avail[n.ID] = true
	}
	bad := map[string]bool{}
	for _, v := range job.BadVeins {
		bad[v] = true
	}
	goodIdx := map[int]bool{}
	var readable, release []ledger.ShardLoc
	busy := map[string]bool{}
	for _, sh := range st.Shards {
		busy[sh.Vein] = true
		if bad[sh.Vein] {
			release = append(release, sh)
			continue
		}
		readable = append(readable, sh)
		switch {
		case states[sh.Vein] == ledger.VeinHealthy && avail[sh.Vein]:
			goodIdx[sh.Index] = true
		case states[sh.Vein] == ledger.VeinDraining:
			release = append(release, sh)
		}
	}

	written := 0
	rebuilt := map[ledger.ShardLoc]bool{} // (vein, index) given a fresh shard, keyed with ID zeroed
	if len(goodIdx) < k+m {
		ref := ledger.NuggetRef{ID: job.Nugget, Length: st.Length, ECData: k, ECParity: m, Shards: readable}
		shards, _, err := s.opts.Cluster.ReadShards(ctx, ref, k)
		if err != nil {
			return "", "", fmt.Errorf("smelter: reading shards of %s: %w", job.Nugget, err)
		}
		if err := ec.Reconstruct(k, m, shards); err != nil {
			return "", "", err
		}
		// Shards on bad or draining veins are being replaced, so those veins
		// may take a fresh shard (a different index is fine too).
		for _, r := range release {
			if !goodIdx[r.Index] {
				delete(busy, r.Vein)
			}
		}
		nodes := s.opts.Cluster.Available()
		targets := survey.Place(job.Nugget, len(nodes), nodes)
		t := 0
		for idx := 0; idx < k+m; idx++ {
			if goodIdx[idx] {
				continue
			}
			for t < len(targets) && busy[targets[t].ID] {
				t++
			}
			if t == len(targets) {
				break
			}
			vein := targets[t].ID
			t++
			sh := ec.Blob(job.Nugget, idx, shards[idx])
			if bad[vein] {
				_ = s.opts.Cluster.Delete(ctx, vein, sh.ID) // a corrupt copy of this exact blob
			}
			if err := s.limiter.wait(ctx, int64(len(sh.Blob))); err != nil {
				return "", "", err
			}
			if err := s.opts.Cluster.Put(ctx, vein, sh.ID, sh.Blob); err != nil {
				s.log.Warn("shard rebuild failed", "vein", vein, "nugget", job.Nugget.String(), "shard", idx, "err", err)
				continue
			}
			if err := s.opts.Ledger.AddShards(ctx, job.Nugget, []ledger.ShardLoc{{Index: idx, ID: sh.ID, Vein: vein}}); err != nil {
				return "", "", err
			}
			busy[vein] = true
			goodIdx[idx] = true
			rebuilt[ledger.ShardLoc{Index: idx, Vein: vein}] = true
			written++
			s.copies.Inc()
			s.bytes.Add(float64(len(sh.Blob)))
		}
		if len(goodIdx) < k+m {
			return "", "", fmt.Errorf("%w: %d of %d shards", errNotEnoughVeins, len(goodIdx), k+m)
		}
	}
	// Every index now has a healthy shard: forget the ones being replaced.
	for _, r := range release {
		if rebuilt[ledger.ShardLoc{Index: r.Index, Vein: r.Vein}] {
			continue // replaced in place by a fresh copy of the same shard
		}
		if err := s.opts.Ledger.RemoveShard(ctx, job.Nugget, r.Index, r.Vein); err != nil {
			return "", "", err
		}
		// Shard IDs encode nugget and index, so a rebuilt shard never shares
		// an ID with the one released here.
		if err := s.opts.Cluster.Delete(ctx, r.Vein, r.ID); err != nil {
			s.log.Debug("delete replaced shard", "vein", r.Vein, "err", err)
		}
	}
	if written > 0 || len(release) > 0 {
		s.opts.Events.Publish(events.Event{Type: "repair.done", Component: "smelter", Attrs: map[string]any{
			"nugget": job.Nugget.String(), "reason": job.Reason, "shards_rebuilt": written, "shards_released": len(release),
		}})
	}
	return "repaired", "", nil
}
