package adit

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/404khai/lode/internal/nugget"
)

func (h *harness) replicaSets(bucket, key string) [][]string {
	h.t.Helper()
	o, err := h.ledger.GetObject(context.Background(), bucket, key, "")
	if err != nil {
		h.t.Fatal(err)
	}
	var out [][]string
	for _, n := range o.Nuggets {
		out = append(out, n.Replicas)
	}
	return out
}

func TestReplicatedWritesSpreadAcrossVeins(t *testing.T) {
	h := newClusterHarness(t, 4, 3)
	h.expect(h.do("PUT", "/rep", nil, nil), 200, "")
	data := randomBytes(3 * nugget.MaxSize)
	h.expect(h.do("PUT", "/rep/obj", data, nil), 200, "")
	sets := h.replicaSets("rep", "obj")
	if len(sets) < 3 {
		t.Fatalf("expected several nuggets, got %d", len(sets))
	}
	for i, rs := range sets {
		if len(rs) != 3 {
			t.Fatalf("nugget %d has %d replicas: %v", i, len(rs), rs)
		}
		zones := map[string]bool{}
		for _, v := range rs {
			zones[map[string]string{"v1": "a", "v2": "b", "v3": "a", "v4": "b"}[v]] = true
		}
		if len(zones) != 2 {
			t.Fatalf("nugget %d replicas all in one zone: %v", i, rs)
		}
	}
	// Bucket-level replication override.
	h.expect(h.do("PUT", "/rep2", nil, map[string]string{"X-Lode-Replication": "2"}), 200, "")
	h.expect(h.do("PUT", "/rep2/obj", []byte("two copies"), nil), 200, "")
	if rs := h.replicaSets("rep2", "obj"); len(rs[0]) != 2 {
		t.Fatalf("x-lode-replication ignored: %v", rs)
	}
}

// Phase 3 exit criterion: a vein dies in the middle of an upload and the
// upload still succeeds; afterwards reads succeed with any one vein down.
func TestVeinDiesMidUpload(t *testing.T) {
	h := newClusterHarness(t, 4, 3)
	h.expect(h.do("PUT", "/kill", nil, nil), 200, "")
	data := randomBytes(6 * nugget.MaxSize)

	pr, pw := io.Pipe()
	r, _ := http.NewRequest("PUT", h.ts.URL+"/kill/big", pr)
	r.ContentLength = int64(len(data))
	sign(r, nil, unsignedPayload)
	done := make(chan *http.Response, 1)
	go func() {
		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Error(err)
		}
		done <- resp
	}()
	half := len(data) / 3
	if _, err := pw.Write(data[:half]); err != nil {
		t.Fatal(err)
	}
	h.veins[1].stop() // v2 dies mid-upload
	if _, err := pw.Write(data[half:]); err != nil {
		t.Fatal(err)
	}
	_ = pw.Close()
	resp := <-done
	h.expect(resp, 200, "")

	degraded := 0
	for _, rs := range h.replicaSets("kill", "big") {
		if len(rs) < 2 {
			t.Fatalf("nugget below write quorum: %v", rs)
		}
		if len(rs) == 2 {
			degraded++
		}
	}
	if degraded == 0 {
		t.Fatal("expected some nuggets written while v2 was down to be degraded")
	}
	if got := h.expect(h.do("GET", "/kill/big", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("object written across a vein failure reads back wrong")
	}
	h.veins[1].restart(t)

	// Reads succeed with any single vein down.
	small := randomBytes(1 << 20)
	h.expect(h.do("PUT", "/kill/small", small, nil), 200, "")
	for i, v := range h.veins {
		v.stop()
		for key, want := range map[string][]byte{"big": data, "small": small} {
			if got := h.expect(h.do("GET", "/kill/"+key, nil, nil), 200, ""); !bytes.Equal(got, want) {
				t.Fatalf("read of %s with v%d down returned wrong bytes", key, i+1)
			}
		}
		v.restart(t)
	}
}

func TestWriteFailsBelowQuorum(t *testing.T) {
	h := newClusterHarness(t, 4, 3)
	h.expect(h.do("PUT", "/quorum", nil, nil), 200, "")
	for _, v := range h.veins[:3] {
		v.stop()
	}
	h.expect(h.do("PUT", "/quorum/k", []byte("needs two copies"), nil), 503, "ServiceUnavailable")
	h.expect(h.do("GET", "/quorum/k", nil, nil), 404, "NoSuchKey")
}

// A corrupt replica is skipped in favour of a good one.
func TestReadFailsOverFromCorruptReplica(t *testing.T) {
	h := newClusterHarness(t, 3, 3)
	h.expect(h.do("PUT", "/heal", nil, nil), 200, "")
	data := bytes.Repeat([]byte("lode"), 1000)
	h.expect(h.do("PUT", "/heal/k", data, nil), 200, "")
	id := nugget.Sum(data)
	// Replace the first vein's copy with garbage of the same length.
	if err := h.veins[0].store.Delete(id); err != nil {
		t.Fatal(err)
	}
	h.veins[0].stop()
	h.veins[0].restart(t)
	if got := h.expect(h.do("GET", "/heal/k", nil, nil), 200, ""); !bytes.Equal(got, data) {
		t.Fatal("failover read returned wrong bytes")
	}
}

// A client hanging up mid-upload must not take healthy veins out of rotation.
func TestClientCancelDoesNotMarkVeinsDown(t *testing.T) {
	h := newClusterHarness(t, 3, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	data := []byte("cancelled")
	for _, v := range []string{"v1", "v2", "v3"} {
		if err := h.cluster.Put(ctx, v, nugget.Sum(data), data); err == nil {
			t.Fatal("put with a cancelled context succeeded")
		}
	}
	if n := len(h.cluster.Available()); n != 3 {
		t.Fatalf("caller cancellation took veins out of rotation: %d available", n)
	}
	h.expect(h.do("PUT", "/cancel", nil, nil), 200, "")
	h.expect(h.do("PUT", "/cancel/after", []byte("still writable"), nil), 200, "")
}
