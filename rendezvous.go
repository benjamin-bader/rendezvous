package rendezvous

import (
	"hash"
	"hash/fnv"
	"io"
	"math"
	"sort"
)

const (
	defaultWeight = 1.0
)

type (
	HashProvider func() hash.Hash64

	// A Ring is a collection of nodes making up a rendezvous group.
	// Nodes have a label and, optionally, a weight.  If unspecified,
	// a default weighting is used.
	Ring struct {
		nodes []*node
		h     hash.Hash64
	}

	node struct {
		name   string
		hash   uint64
		weight float64
	}
)

func New() *Ring {
	return NewWithHash(fnv.New64a())
}

func NewWithHash(h hash.Hash64) *Ring {
	return &Ring{
		nodes: make([]*node, 0),
		h:     h,
	}
}

func (r *Ring) Add(name string) {
	r.AddWithWeight(name, defaultWeight)
}

func (r *Ring) AddWithWeight(name string, weight float64) {
	var ix int
	if len(r.nodes) > 0 {
		ix = sort.Search(len(r.nodes), r.cmp(name))
	}

	if ix < len(r.nodes) && r.nodes[ix].name == name {
		r.nodes[ix].weight = weight
	} else {
		n := &node{
			name:   name,
			hash:   r.hash(name),
			weight: weight,
		}
		r.nodes = append(r.nodes, nil)
		copy(r.nodes[ix+1:], r.nodes[ix:])
		r.nodes[ix] = n
	}
}

func (r *Ring) Remove(node string) {
	ix := sort.Search(len(r.nodes), r.cmp(node))
	if ix == len(r.nodes) {
		return
	}

	if r.nodes[ix].name == node {
		copy(r.nodes[:ix], r.nodes[:ix+1])
		r.nodes = r.nodes[:len(r.nodes)-1]
	}
}

func (r *Ring) Lookup(key string) string {
	keyHash := r.hash(key)

	maxScore := -math.MaxFloat64
	var assignedNode *node
	for _, n := range r.nodes {
		s := computeScore(keyHash, n.hash, n.weight)
		if s > maxScore {
			maxScore = s
			assignedNode = n
		}
	}

	if assignedNode == nil {
		panic("assert false") // implies that the ring is empty
	}

	return assignedNode.name
}

func (r *Ring) hash(s string) uint64 {
	r.h.Reset()
	_, _ = io.WriteString(r.h, s)
	return r.h.Sum64()
}

func (r *Ring) cmp(name string) func(int) bool {
	return func(i int) bool {
		return r.nodes[i].name >= name
	}
}

func computeScore(keyHash, nodeHash uint64, nodeWeight float64) float64 {
	h := combineHashes(keyHash, nodeHash)
	return -nodeWeight / math.Log(float64(h)/float64(math.MaxUint64))
}

func combineHashes(a, b uint64) uint64 {
	// uses the "xorshift*" mix function which is simple and effective
	// see: https://en.wikipedia.org/wiki/Xorshift#xorshift*
	x := a ^ b
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	return x * 0x2545F4914F6CDD1D
}
