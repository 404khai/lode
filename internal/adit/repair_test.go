package adit

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/404khai/lode/internal/assay"
	"github.com/404khai/lode/internal/canary"
	"github.com/404khai/lode/internal/ledger"
	"github.com/404khai/lode/internal/nugget"
	"github.com/404khai/lode/internal/smelter"
)

// drainQueue runs Smelter until the repair queue has nothing runnable.
func (h *harness) drainQueue() {
	h.t.Helper()
	ctx := context.Background()
	_ = h.cluster.Refresh(ctx)
	s := smelter.New(smelter.Options{Ledger: h.ledger, Cluster: h.cluster, Owner: "test", MaxAttempts: 3})
	for i := 0; i < 10000; i++ {
		did, err := s.RunOne(ctx, "test")
		if err != nil {
			h.t.Fatal(err)
		}
		if !did {
			return
		}
	}
	h.t.Fatal("repair queue never drained")
}

func (h *harness) stats() ledger.RepairStats {
	h.t.Helper()
	st, err := h.ledger.RepairStats(context.Background())
	if err != nil {
		h.t.Fatal(err)
	}
	return st
}

// healthyCopies reports, per nugget of key, the copies on healthy veins.
func (h *harness) healthyCopies(bucket, key string) []int {
	h.t.Helper()
	states := map[string]string{}
	vs, _ := h.ledger.ListVeins(context.Background())
	for _, v := range vs {
		states[v.ID] = v.State
	}
	var out []int
	for _, rs := range h.replicaSets(bucket, key) {
		n := 0
		for _, v := range rs {
			if states[v] == ledger.VeinHealthy {
				n++
			}
		}
		out = append(out, n)
	}
	return out
}

func TestRepairAfterVeinDies(t *testing.T) {
	h := newClusterHarness(t, 4, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/fix", nil, nil), 200, "")
	data := randomBytes(3 * nugget.MaxSize)
	h.expect(h.do("PUT", "/fix/obj", data, nil), 200, "")

	// v1 dies for good; Canary marks it down, which queues its nuggets.
	h.veins[0].stop()
	if _, err := h.ledger.SetVeinState(ctx, "v1", ledger.VeinDown); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ledger.EnqueueVeinRepairs(ctx, "v1", ledger.ReasonVeinDown); err != nil {
		t.Fatal(err)
	}
	if st := h.stats(); st.UnderReplicated == 0 || st.Queued == 0 {
		t.Fatalf("expected under-replicated nuggets and queued jobs: %+v", st)
	}
	h.drainQueue()
	if st := h.stats(); st.UnderReplicated != 0 || st.Queued+st.Leased != 0 || st.Lost != 0 {
		t.Fatalf("redundancy not restored: %+v", st)
	}
	for i, n := range h.healthyCopies("fix", "obj") {
		if n != 3 {
			t.Fatalf("nugget %d has %d healthy copies", i, n)
		}
	}
	if got := h.expect(h.do("GET", "/fix/obj", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("object wrong after repair")
	}
}

func TestAssayFindsCorruptionAndSmelterRepairs(t *testing.T) {
	h := newClusterHarness(t, 3, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/scrub", nil, nil), 200, "")
	data := randomBytes(200 << 10)
	h.expect(h.do("PUT", "/scrub/k", data, nil), 200, "")

	// Flip bytes in v2's copy on disk.
	v := h.veins[1]
	segs, _ := filepath.Glob(filepath.Join(v.dir, "segments", "*.seg"))
	raw, _ := os.ReadFile(segs[0])
	off := bytes.Index(raw, data[1000:1100])
	f, _ := os.OpenFile(segs[0], os.O_WRONLY, 0)
	_, _ = f.WriteAt([]byte{0, 0, 0, 0}, int64(off))
	_ = f.Close()

	a := assay.New(assay.Options{Store: v.store, Ledger: h.ledger, VeinID: v.id})
	n, err := a.Pass(ctx)
	if err != nil || n != 1 {
		t.Fatalf("assay found %d corrupt copies, err %v", n, err)
	}
	h.drainQueue()
	if st := h.stats(); st.UnderReplicated != 0 || st.Queued != 0 {
		t.Fatalf("after repair: %+v", st)
	}
	if n, _ := a.Pass(ctx); n != 0 {
		t.Fatalf("corrupt copy still present after repair: %d", n)
	}
	for i, c := range h.healthyCopies("scrub", "k") {
		if c != 3 {
			t.Fatalf("nugget %d has %d copies after repair", i, c)
		}
	}
}

func TestDrainMovesEverythingThenRetires(t *testing.T) {
	h := newClusterHarness(t, 4, 3)
	ctx := context.Background()
	h.expect(h.do("PUT", "/drain", nil, nil), 200, "")
	data := randomBytes(3 * nugget.MaxSize)
	h.expect(h.do("PUT", "/drain/k", data, nil), 200, "")
	before, _ := h.ledger.VeinReplicaCount(ctx, "v3")
	if before == 0 {
		t.Skip("placement put nothing on v3")
	}
	if _, err := h.ledger.SetVeinState(ctx, "v3", ledger.VeinDraining); err != nil {
		t.Fatal(err)
	}
	c := canary.New(canary.Options{Ledger: h.ledger, DownAfter: time.Hour, SuspectPhi: 1e9})
	if err := c.Tick(ctx); err != nil { // queues drain jobs
		t.Fatal(err)
	}
	h.drainQueue()
	if left, _ := h.ledger.VeinReplicaCount(ctx, "v3"); left != 0 {
		t.Fatalf("%d copies left on draining vein", left)
	}
	if err := c.Tick(ctx); err != nil { // retires the empty vein
		t.Fatal(err)
	}
	vs, _ := h.ledger.ListVeins(ctx)
	for _, v := range vs {
		if v.ID == "v3" {
			t.Fatalf("v3 still listed as %s", v.State)
		}
	}
	h.veins[2].stop()
	if got := h.expect(h.do("GET", "/drain/k", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("object wrong after drain")
	}
	for i, n := range h.healthyCopies("drain", "k") {
		if n != 3 {
			t.Fatalf("nugget %d has %d healthy copies after drain", i, n)
		}
	}
}

// Canary's state machine, driven by heartbeat timestamps.
func TestCanaryDetectsSilenceAndRecovery(t *testing.T) {
	h := newClusterHarness(t, 2, 2)
	ctx := context.Background()
	h.expect(h.do("PUT", "/can", nil, nil), 200, "")
	h.expect(h.do("PUT", "/can/k", []byte("watched"), nil), 200, "")

	now := time.Now()
	clock := func() time.Time { return now }
	beat := func(vein string, at time.Time) {
		if _, err := h.ledger.Pool().Exec(ctx, "UPDATE veins SET last_heartbeat = $2 WHERE id = $1", vein, at); err != nil {
			t.Fatal(err)
		}
	}
	c := canary.New(canary.Options{Ledger: h.ledger, Now: clock, DownAfter: 20 * time.Second, SuspectPhi: 8})
	state := func(id string) string {
		vs, _ := h.ledger.ListVeins(ctx)
		for _, v := range vs {
			if v.ID == id {
				return v.State
			}
		}
		return ""
	}
	// Twenty regular heartbeats from both veins.
	for i := 0; i < 20; i++ {
		now = now.Add(2 * time.Second)
		beat("v1", now)
		beat("v2", now)
		if err := c.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// v2 goes silent; v1 keeps beating.
	var sawSuspect bool
	for i := 0; i < 12; i++ {
		now = now.Add(2 * time.Second)
		beat("v1", now)
		if err := c.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if state("v2") == ledger.VeinSuspect {
			sawSuspect = true
		}
	}
	if !sawSuspect || state("v2") != ledger.VeinDown || state("v1") != ledger.VeinHealthy {
		t.Fatalf("suspect seen=%v, v1=%s v2=%s", sawSuspect, state("v1"), state("v2"))
	}
	if st := h.stats(); st.Queued == 0 {
		t.Fatal("down vein's nuggets were not queued for repair")
	}
	// v2 comes back.
	for i := 0; i < 3; i++ {
		now = now.Add(2 * time.Second)
		beat("v1", now)
		beat("v2", now)
		_ = c.Tick(ctx)
	}
	if state("v2") != ledger.VeinHealthy {
		t.Fatalf("v2 did not recover: %s", state("v2"))
	}
}
