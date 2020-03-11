package rendezvous

import (
	"strconv"
	"testing"
)

func TestRendezvous_Lookup(t *testing.T) {
	rv := New()
	rv.AddWithWeight("x", 1.0)
	rv.AddWithWeight("y", 0.5)
	rv.AddWithWeight("z", 0.5)

	allocs := map[string]int{
		"x": 0,
		"y": 0,
		"z": 0,
	}
	for i := 0; i < 10000; i++ {
		node := rv.Lookup("n"+strconv.Itoa(i))
		allocs[node]++
	}

	t.Logf("proportions: %v", allocs)
}

