// Package survey is Lode's placement engine. It decides which veins hold a
// nugget's replicas using rendezvous (highest-random-weight) hashing, so
// adding or removing a vein only moves the nuggets that rank it highly.
// Placement is failure-domain aware: replicas spread across zones first,
// then hosts, before sharing a host.
package survey

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/zeebo/blake3"

	"github.com/404khai/lode/internal/nugget"
)

// Node is a placement target.
type Node struct {
	ID       string
	Zone     string
	Host     string
	Capacity int64 // bytes; used by capacity-weighted placement
	Used     int64
}

func score(id nugget.ID, node string) uint64 {
	h := blake3.New()
	_, _ = h.Write(id[:])
	_, _ = h.Write([]byte(node))
	var sum [8]byte
	_, _ = h.Digest().Read(sum[:])
	return binary.BigEndian.Uint64(sum[:])
}

// Rank orders nodes by their rendezvous score for id, highest first.
func Rank(id nugget.ID, nodes []Node) []Node {
	type scored struct {
		n Node
		s uint64
	}
	ss := make([]scored, len(nodes))
	for i, n := range nodes {
		ss[i] = scored{n, score(id, n.ID)}
	}
	sort.Slice(ss, func(i, j int) bool {
		if ss[i].s != ss[j].s {
			return ss[i].s > ss[j].s
		}
		return ss[i].n.ID < ss[j].n.ID
	})
	out := make([]Node, len(ss))
	for i, s := range ss {
		out[i] = s.n
	}
	return out
}

// Place returns up to n nodes for id with the zone-spread strategy.
func Place(id nugget.ID, n int, nodes []Node) []Node { return spread(Rank(id, nodes), n) }

// spread picks n nodes from ranked (best first): the best of each unused
// zone, then of each unused host, then the best remaining.
func spread(ranked []Node, n int) []Node {
	if n > len(ranked) {
		n = len(ranked)
	}
	chosen := make([]Node, 0, n)
	used := map[string]bool{}
	zones, hosts := map[string]bool{}, map[string]bool{}
	domains := []func(Node) bool{
		func(x Node) bool { return !zones[x.Zone] },
		func(x Node) bool { return !hosts[x.Zone+"/"+x.Host] },
		func(Node) bool { return true },
	}
	for _, free := range domains {
		for _, x := range ranked {
			if len(chosen) == n {
				return chosen
			}
			if used[x.ID] || !free(x) {
				continue
			}
			used[x.ID] = true
			zones[x.Zone] = true
			hosts[x.Zone+"/"+x.Host] = true
			chosen = append(chosen, x)
		}
	}
	return chosen
}

// Strategy names.
const (
	HRW              = "hrw"
	ZoneSpread       = "zone-spread"
	CapacityWeighted = "capacity-weighted"
	Pinned           = "pinned"
)

// Strategy chooses where a nugget's copies or shards go.
type Strategy interface {
	Name() string
	// Place returns up to n distinct nodes for id, best first.
	Place(id nugget.ID, n int, nodes []Node) []Node
	// Stable reports whether placement depends only on the node set (not on
	// usage), which makes it safe for the rebalancer to converge on.
	Stable() bool
}

type hrw struct{}

func (hrw) Name() string { return HRW }
func (hrw) Stable() bool { return true }
func (hrw) Place(id nugget.ID, n int, nodes []Node) []Node {
	r := Rank(id, nodes)
	return r[:min(n, len(r))]
}

type zoneSpread struct{}

func (zoneSpread) Name() string                                   { return ZoneSpread }
func (zoneSpread) Stable() bool                                   { return true }
func (zoneSpread) Place(id nugget.ID, n int, nodes []Node) []Node { return Place(id, n, nodes) }

// capacityWeighted is weighted rendezvous hashing: each node scores
// -free/ln(u) for a per-(nugget,node) uniform u, so a node receives new data
// in proportion to its free space; replicas still spread across domains.
type capacityWeighted struct{}

func (capacityWeighted) Name() string { return CapacityWeighted }
func (capacityWeighted) Stable() bool { return false }
func (capacityWeighted) Place(id nugget.ID, n int, nodes []Node) []Node {
	type scored struct {
		n Node
		s float64
	}
	ss := make([]scored, len(nodes))
	for i, x := range nodes {
		free := float64(max(x.Capacity-x.Used, 1))
		u := (float64(score(id, x.ID)>>11) + 0.5) / (1 << 53) // uniform in (0,1)
		ss[i] = scored{x, -free / math.Log(u)}
	}
	sort.Slice(ss, func(i, j int) bool {
		if ss[i].s != ss[j].s {
			return ss[i].s > ss[j].s
		}
		return ss[i].n.ID < ss[j].n.ID
	})
	ranked := make([]Node, len(ss))
	for i, x := range ss {
		ranked[i] = x.n
	}
	return spread(ranked, n)
}

// pinned restricts placement to a fixed set of veins.
type pinned struct{ allowed map[string]bool }

func (pinned) Name() string { return Pinned }
func (pinned) Stable() bool { return true }
func (p pinned) Place(id nugget.ID, n int, nodes []Node) []Node {
	var in []Node
	for _, x := range nodes {
		if p.allowed[x.ID] {
			in = append(in, x)
		}
	}
	return Place(id, n, in)
}

// ErrUnknownStrategy means a placement name is not recognised.
var ErrUnknownStrategy = errors.New("survey: unknown placement strategy")

// ByName returns a strategy; pinnedVeins is used by "pinned". An empty name
// means zone-spread.
func ByName(name string, pinnedVeins []string) (Strategy, error) {
	switch name {
	case "", ZoneSpread:
		return zoneSpread{}, nil
	case HRW:
		return hrw{}, nil
	case CapacityWeighted:
		return capacityWeighted{}, nil
	case Pinned:
		if len(pinnedVeins) == 0 {
			return nil, fmt.Errorf("%w: pinned needs at least one vein", ErrUnknownStrategy)
		}
		p := pinned{allowed: map[string]bool{}}
		for _, v := range pinnedVeins {
			p.allowed[v] = true
		}
		return p, nil
	}
	return nil, fmt.Errorf("%w %q", ErrUnknownStrategy, name)
}
