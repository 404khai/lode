// Package survey is Lode's placement engine. It decides which veins hold a
// nugget's replicas using rendezvous (highest-random-weight) hashing, so
// adding or removing a vein only moves the nuggets that rank it highly.
// Placement is failure-domain aware: replicas spread across zones first,
// then hosts, before sharing a host.
package survey

import (
	"encoding/binary"
	"sort"

	"github.com/zeebo/blake3"

	"github.com/404khai/lode/internal/nugget"
)

// Node is a placement target.
type Node struct {
	ID   string
	Zone string
	Host string
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

// Place returns up to n nodes for id, in preference order. It takes the
// best-ranked node of each unused zone, then of each unused host, then the
// best remaining nodes.
func Place(id nugget.ID, n int, nodes []Node) []Node {
	ranked := Rank(id, nodes)
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
