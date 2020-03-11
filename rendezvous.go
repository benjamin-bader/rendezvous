package rendezvous

import (
	"hash"
	"hash/fnv"
	"io"
	"math"
	"sync"
)

type HashProvider func() hash.Hash64

type node struct {
	name   string
	hash   uint64
	weight float64
}

type Ring struct {
	nodes map[string]*node
	hp    HashProvider
	m     sync.RWMutex
}

func New() *Ring {
	return NewWithHashProvider(fnv.New64a)
}

func NewWithHashProvider(hp HashProvider) *Ring {
	return &Ring{
		nodes: make(map[string]*node),
		hp:    hp,
	}
}

func (r *Ring) Add(name string) {
	r.AddWithWeight(name, 1.0)
}

func (r *Ring) AddWithWeight(name string, weight float64) {
	r.m.Lock()
	defer r.m.Unlock()

	n := r.nodes[name]
	if n == nil {
		n = &node{
			name:   name,
			hash:   r.hash(name),
			weight: weight,
		}
		r.nodes[name] = n
	} else {
		n.weight = weight
	}
}

func (r *Ring) Remove(node string) {
	r.m.Lock()
	defer r.m.Unlock()

	delete(r.nodes, node)
}

func (r *Ring) Lookup(key string) string {
	r.m.RLock()
	defer r.m.RUnlock()

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
	h := r.hp()
	_, _ = io.WriteString(h, s)
	return h.Sum64()
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
