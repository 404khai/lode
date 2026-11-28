package adit

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	lodev1 "github.com/404khai/lode/gen/lode/v1"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/smelter"
	"github.com/404khai/lode/internal/survey"
	"github.com/404khai/lode/internal/vein"
)

// addVein starts and registers one more vein.
func (h *harness) addVein(zone string) *testVein {
	h.t.Helper()
	ctx := context.Background()
	store, err := vein.Open(vein.Options{Dir: h.t.TempDir()})
	if err != nil {
		h.t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatal(err)
	}
	v := &testVein{id: fmt.Sprintf("v%d", len(h.veins)+1), store: store, addr: lis.Addr().String()}
	v.gs = grpc.NewServer(vein.IdentityInterceptors(v.id)...)
	lodev1.RegisterVeinServiceServer(v.gs, vein.NewServer(store, v.id))
	go func() { _ = v.gs.Serve(lis) }()
	h.t.Cleanup(func() { v.gs.Stop(); _ = store.Close() })
	if err := h.ledger.RegisterVein(ctx, ledger.VeinInfo{ID: v.id, Addr: v.addr, Zone: zone, Host: v.id}); err != nil {
		h.t.Fatal(err)
	}
	h.veins = append(h.veins, v)
	if err := h.cluster.Refresh(ctx); err != nil {
		h.t.Fatal(err)
	}
	return v
}

func (h *harness) shardSets(bucket, key string) [][]ledger.ShardLoc {
	h.t.Helper()
	o, err := h.ledger.GetObject(context.Background(), bucket, key, "")
	if err != nil {
		h.t.Fatal(err)
	}
	var out [][]ledger.ShardLoc
	for _, n := range o.Nuggets {
		if n.ECData != 4 || n.ECParity != 2 {
			h.t.Fatalf("nugget layout %d+%d", n.ECData, n.ECParity)
		}
		out = append(out, n.Shards)
	}
	return out
}

// Phase 5 exit criterion: a 6-vein cluster with 4+2 survives two
// simultaneous vein losses.
func TestErasureCodingSurvivesTwoVeinLosses(t *testing.T) {
	h := newClusterHarness(t, 6, 3)
	h.expect(h.do("PUT", "/ecb", nil, map[string]string{"X-Lode-Storage-Class": "ec:4+2"}), 200, "")
	h.expect(h.do("PUT", "/ecbad", nil, map[string]string{"X-Lode-Storage-Class": "ec:0+2"}), 400, "InvalidArgument")
	data := randomBytes(3*nugget.MaxSize + 12345)
	h.expect(h.do("PUT", "/ecb/obj", data, nil), 200, "")
	small := []byte("tiny object, still striped")
	h.expect(h.do("PUT", "/ecb/small", small, nil), 200, "")

	for i, shards := range h.shardSets("ecb", "obj") {
		veins, idx := map[string]bool{}, map[int]bool{}
		for _, s := range shards {
			veins[s.Vein], idx[s.Index] = true, true
		}
		if len(shards) != 6 || len(veins) != 6 || len(idx) != 6 {
			t.Fatalf("nugget %d: %d shards on %d veins", i, len(shards), len(veins))
		}
	}
	// Storage overhead is 1.5x, not 3x.
	var stored int64
	for _, v := range h.veins {
		_, used, _ := v.store.Usage()
		stored += used
	}
	if ratio := float64(stored) / float64(len(data)+len(small)); ratio > 1.6 {
		t.Fatalf("4+2 stored %.2fx the data", ratio)
	}

	for _, pair := range [][2]int{{0, 1}, {2, 5}, {3, 4}} {
		h.veins[pair[0]].stop()
		h.veins[pair[1]].stop()
		for key, want := range map[string][]byte{"obj": data, "small": small} {
			if got := h.expect(h.do("GET", "/ecb/"+key, nil, nil), 200, ""); !bytes.Equal(got, want) {
				t.Fatalf("%s wrong with v%d and v%d down", key, pair[0]+1, pair[1]+1)
			}
		}
		// Ranged read through reconstruction.
		resp := h.do("GET", "/ecb/obj", nil, map[string]string{"Range": "bytes=8388000-8389000"})
		if got := h.expect(resp, 206, ""); !bytes.Equal(got, data[8388000:8389001]) {
			t.Fatal("degraded ranged read wrong")
		}
		// Four veins left: k+1 = 5 shards cannot be written.
		h.expect(h.do("PUT", "/ecb/new", randomBytes(1000+pair[0]), nil), 503, "ServiceUnavailable") // unique content per attempt
		h.veins[pair[0]].restart(t)
		h.veins[pair[1]].restart(t)
	}
}

func TestSmelterRebuildsLostShards(t *testing.T) {
	h := newClusterHarness(t, 7, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/ecr", nil, map[string]string{"X-Lode-Storage-Class": "ec:4+2"}), 200, "")
	data := randomBytes(2 * nugget.MaxSize)
	h.expect(h.do("PUT", "/ecr/obj", data, nil), 200, "")

	// Kill a vein that holds shards and let repair rebuild them elsewhere.
	victim := h.shardSets("ecr", "obj")[0][0].Vein
	var vi int
	if _, err := fmt.Sscanf(victim, "v%d", &vi); err != nil {
		t.Fatal(err)
	}
	h.veins[vi-1].stop()
	_, _ = h.ledger.SetVeinState(ctx, victim, ledger.VeinDown)
	if _, err := h.ledger.EnqueueVeinRepairs(ctx, victim, ledger.ReasonVeinDown); err != nil {
		t.Fatal(err)
	}
	if st := h.stats(); st.UnderReplicated == 0 {
		t.Fatal("losing a shard should count as under-replicated")
	}
	h.drainQueue()
	if st := h.stats(); st.UnderReplicated != 0 || st.Lost != 0 {
		t.Fatalf("after rebuild: %+v", st)
	}
	states := map[string]bool{}
	vs, _ := h.ledger.ListVeins(ctx)
	for _, v := range vs {
		states[v.ID] = v.State == ledger.VeinHealthy
	}
	for i, shards := range h.shardSets("ecr", "obj") {
		idx := map[int]bool{}
		for _, s := range shards {
			if states[s.Vein] {
				idx[s.Index] = true
			}
		}
		if len(idx) != 6 {
			t.Fatalf("nugget %d has %d healthy shard indices after rebuild", i, len(idx))
		}
	}
	// Still survives two more losses after the rebuild.
	stopped := 0
	for _, v := range h.veins {
		if v.id != victim && stopped < 2 {
			v.stop()
			stopped++
		}
	}
	if got := h.expect(h.do("GET", "/ecr/obj", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("object wrong after rebuild and two more losses")
	}
}

// Phase 5 exit criterion: after a vein joins, rebalancing moves about 1/N
// of the copies (N = veins after the join) and nothing else.
func TestRebalanceMovesAboutOneNth(t *testing.T) {
	h := newClusterHarness(t, 5, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/rebal", nil, nil), 200, "")
	const objects = 300
	for i := range objects {
		h.expect(h.do("PUT", fmt.Sprintf("/rebal/o%d", i), []byte(fmt.Sprintf("object %d", i)), nil), 200, "")
	}
	h.addVein("b") // same failure domain as existing veins; a new zone would rightly take a copy of everything
	placer, _ := survey.ByName(survey.ZoneSpread, nil)
	rb := smelter.NewRebalancer(smelter.RebalanceOptions{Ledger: h.ledger, Cluster: h.cluster, Strategy: placer})
	st, err := rb.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	total := objects * 3
	frac := float64(st.Moved) / float64(total)
	t.Logf("moved %d of %d copies (%.3f; 1/N = %.3f)", st.Moved, total, frac, 1.0/6)
	if frac > 1.4/6 || frac < 0.5/6 {
		t.Fatalf("rebalance moved %.3f of copies, want about 1/6", frac)
	}
	if n, _ := h.ledger.VeinReplicaCount(ctx, "v6"); int(n) != st.Moved {
		t.Fatalf("new vein holds %d copies, moved %d", n, st.Moved)
	}
	// A second pass is a no-op, every object still reads, nothing under-replicated.
	if st2, _ := rb.RunOnce(ctx); st2.Moved != 0 {
		t.Fatalf("second pass moved %d", st2.Moved)
	}
	if s := h.stats(); s.UnderReplicated != 0 {
		t.Fatalf("under-replicated after rebalance: %+v", s)
	}
	for i := 0; i < objects; i += 37 {
		if got := h.expect(h.do("GET", fmt.Sprintf("/rebal/o%d", i), nil, nil), 200, ""); string(got) != fmt.Sprintf("object %d", i) {
			t.Fatalf("o%d wrong after rebalance", i)
		}
	}
}

// A connection that drifts to another vein (address reuse after restarts)
// must be refused, never trusted: v2 answering as v1 would record data on
// the wrong vein.
func TestMisaddressedVeinIsRefused(t *testing.T) {
	h := newClusterHarness(t, 2, 1)
	ctx := context.Background()
	v1, v2 := h.veins[0], h.veins[1]
	v1.stop()
	v2.stop()
	// v2 comes back listening on v1's old address.
	lis, err := net.Listen("tcp", v1.addr)
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(vein.IdentityInterceptors(v2.id)...)
	lodev1.RegisterVeinServiceServer(gs, vein.NewServer(v2.store, v2.id))
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()

	data := []byte("must not land on the wrong vein")
	id := nugget.Sum(data)
	var err2 error
	for range 20 { // wait for the reconnect
		if err2 = h.cluster.Put(ctx, v1.id, id, data); err2 != nil && vein.IsWrongVein(err2) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !vein.IsWrongVein(err2) {
		t.Fatalf("put addressed to v1 was not refused by v2: %v", err2)
	}
	if ok, _ := v2.store.Has(id); ok {
		t.Fatal("v2 stored data addressed to v1")
	}
}

// Too few veins to restore full redundancy is a wait, not a loss.
func TestRepairWaitsForCapacityInsteadOfGivingUp(t *testing.T) {
	h := newClusterHarness(t, 6, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/wait", nil, map[string]string{"X-Lode-Storage-Class": "ec:4+2"}), 200, "")
	h.expect(h.do("PUT", "/wait/k", randomBytes(100<<10), nil), 200, "")
	h.veins[5].stop()
	_, _ = h.ledger.SetVeinState(ctx, "v6", ledger.VeinDown)
	_, _ = h.ledger.EnqueueVeinRepairs(ctx, "v6", ledger.ReasonVeinDown)
	_ = h.cluster.Refresh(ctx)
	s := smelter.New(smelter.Options{Ledger: h.ledger, Cluster: h.cluster, Owner: "t", MaxAttempts: 1})
	if did, err := s.RunOne(ctx, "t"); !did || err != nil {
		t.Fatalf("run: %v %v", did, err)
	}
	if st := h.stats(); st.Lost != 0 || st.Queued != 1 {
		t.Fatalf("capacity shortage must stay queued, not lost: %+v", st)
	}
}
