package survey

import (
	"fmt"
	"testing"

	"github.com/404khai/lode/internal/nugget"
)

func cluster() []Node {
	return []Node{
		{ID: "v1", Zone: "a", Host: "h1"}, {ID: "v2", Zone: "a", Host: "h1"},
		{ID: "v3", Zone: "a", Host: "h2"}, {ID: "v4", Zone: "b", Host: "h3"},
		{ID: "v5", Zone: "b", Host: "h4"}, {ID: "v6", Zone: "c", Host: "h5"},
	}
}

func id(i int) nugget.ID { return nugget.Sum([]byte(fmt.Sprint(i))) }

func TestPlaceSpreadsFailureDomains(t *testing.T) {
	for i := range 1000 {
		got := Place(id(i), 3, cluster())
		if len(got) != 3 {
			t.Fatalf("got %d nodes", len(got))
		}
		zones := map[string]bool{}
		for _, n := range got {
			zones[n.Zone] = true
		}
		if len(zones) != 3 {
			t.Fatalf("nugget %d: replicas share a zone: %+v", i, got)
		}
	}
	// Four replicas over three zones: the fourth must still avoid sharing a host.
	for i := range 1000 {
		hosts := map[string]bool{}
		for _, n := range Place(id(i), 4, cluster()) {
			hosts[n.Zone+n.Host] = true
		}
		if len(hosts) != 4 {
			t.Fatalf("nugget %d: four replicas on fewer than four hosts", i)
		}
	}
	if got := Place(id(1), 10, cluster()); len(got) != 6 {
		t.Fatalf("asking for more replicas than nodes: %d", len(got))
	}
}

func TestPlaceIsDeterministicAndBalanced(t *testing.T) {
	nodes := []Node{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}
	count := map[string]int{}
	for i := range 4000 {
		p := Place(id(i), 1, nodes)
		if Place(id(i), 1, nodes)[0] != p[0] {
			t.Fatal("placement not deterministic")
		}
		count[p[0].ID]++
	}
	for n, c := range count {
		if c < 800 || c > 1200 {
			t.Fatalf("node %s got %d of 4000 primaries", n, c)
		}
	}
}

// Removing a node moves only the nuggets that ranked it first.
func TestRemovalIsMinimalDisruption(t *testing.T) {
	nodes := []Node{{ID: "a"}, {ID: "b"}, {ID: "c"}, {ID: "d"}}
	moved := 0
	for i := range 4000 {
		before := Place(id(i), 1, nodes)[0]
		after := Place(id(i), 1, nodes[:3])[0]
		if before != after {
			if before.ID != "d" {
				t.Fatalf("nugget %d moved from %s although d was removed", i, before.ID)
			}
			moved++
		}
	}
	if moved < 800 || moved > 1200 {
		t.Fatalf("moved %d of 4000", moved)
	}
}

func TestCapacityWeightedFollowsFreeSpace(t *testing.T) {
	nodes := []Node{{ID: "big", Capacity: 300, Zone: "a"}, {ID: "small", Capacity: 100, Zone: "b"}}
	s, _ := ByName(CapacityWeighted, nil)
	count := map[string]int{}
	for i := range 8000 {
		count[s.Place(id(i), 1, nodes)[0].ID]++
	}
	ratio := float64(count["big"]) / float64(count["small"])
	if ratio < 2.5 || ratio > 3.5 {
		t.Fatalf("big:small = %.2f, want ~3", ratio)
	}
}

func TestStrategies(t *testing.T) {
	nodes := cluster()
	p, err := ByName(Pinned, []string{"v1", "v4", "v6"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		for _, n := range p.Place(id(i), 3, nodes) {
			if n.ID != "v1" && n.ID != "v4" && n.ID != "v6" {
				t.Fatalf("pinned placed on %s", n.ID)
			}
		}
	}
	h, _ := ByName(HRW, nil)
	if got := h.Place(id(1), 3, nodes); len(got) != 3 || got[0] != Rank(id(1), nodes)[0] {
		t.Fatal("hrw is not the rank prefix")
	}
	if _, err := ByName("nope", nil); err == nil {
		t.Fatal("unknown strategy accepted")
	}
	if _, err := ByName(Pinned, nil); err == nil {
		t.Fatal("pinned with no veins accepted")
	}
}
