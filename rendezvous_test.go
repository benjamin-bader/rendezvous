package rendezvous

import (
	"fmt"
	"github.com/cespare/xxhash/v2"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestRing_Remove(t *testing.T) {
	rv := New()
	rv.Add("a")
	rv.Add("b")
	rv.Add("c")

	rv.Remove("b")

	if len(rv.nodes) != 2 || rv.nodes[0].name != "a" || rv.nodes[1].name != "b" {
		t.Errorf("Expected Remove(\"b\") to remove the node labeled \"b\": nodes=%v", rv.nodes)
	}

	rv.Remove("d")
	if len(rv.nodes) != 2 {
		t.Errorf("Removing a non-existent node unexpectedly altered nodes: %v", rv.nodes)
	}
}

func TestRing_Add(t *testing.T) {
	t.Run("KeepsNodesSorted", func(t *testing.T) {
		rv := New()
		rv.Add("d")
		rv.Add("c")
		rv.Add("e")
		rv.Add("b")
		rv.Add("a")

		names := make([]string, len(rv.nodes))
		for i, n := range rv.nodes {
			names[i] = n.name
		}

		if !reflect.DeepEqual(names, []string{"a", "b", "c", "d", "e"}) {
			t.Errorf("Expected sorted nodes but got %v", names)
		}
	})

	t.Run("DoesNotAddDuplicates", func(t *testing.T) {
		rv := New()
		rv.Add("a")
		rv.Add("a")

		if len(rv.nodes) != 1 {
			t.Errorf("Expected Add() to detect and filter duplicate node names")
		}
	})
}

func TestRing_AddWithWeight(t *testing.T) {
	t.Run("UpdatesWeights", func(t *testing.T) {
		rv := New()
		rv.AddWithWeight("a", 1.0)
		rv.AddWithWeight("b", 1.1)

		if rv.nodes[1].weight != 1.1 {
			t.Fatalf("wtf")
		}

		rv.AddWithWeight("b", 1.5)
		if rv.nodes[1].weight != 1.5 {
			t.Errorf("Expected AddWithWeight on an existing node to update the node's weight")
		}
	})
}

func TestRing_Lookup(t *testing.T) {
	t.Run("IsBasicallyAccurate", func(t *testing.T) {
		rv := NewWithHash(xxhash.New())
		rv.AddWithWeight("x", 1.0)
		rv.AddWithWeight("y", 0.5)
		rv.AddWithWeight("z", 0.5)

		allocs := map[string]int{
			"x": 0,
			"y": 0,
			"z": 0,
		}
		for i := 0; i < 10000; i++ {
			node := rv.Lookup("n" + strconv.Itoa(i))
			allocs[node]++
		}

		if !equalsWithinDelta(float64(allocs["x"])/10000.0, 0.5, 0.01) {
			t.Errorf("Expected x to get 50pct, more or less, but got %v", allocs)
		}
	})

	t.Run("IsConsistent", func(t *testing.T) {
		rv := New()
		for i := 0; i <= 10000; i++ {
			rv.Add(fmt.Sprintf("n%d", i))
		}

		mappings := map[string]string{}
		for i := 0; i <= 10; i += 29 {
			key := fmt.Sprintf("k%d", i)
			mappings[key] = rv.Lookup(key)
		}

		mappedNodes := make(map[string]struct{})
		for _, n := range mappings {
			mappedNodes[n] = struct{}{}
		}

		for i := 0; i < 10; i += 33 {
			node := fmt.Sprintf("n%d", i)
			if _, ok := mappedNodes[node]; ok {
				continue
			}
			rv.Remove(node)
		}

		numFailed := 0
		for k, v := range mappings {
			if m := rv.Lookup(k); m != v {
				numFailed++
				t.Errorf("Expected %s to map to %s but got %s", k, v, m)
			}
		}

		if numFailed != 0 {
			t.Logf("%f failed", float64(numFailed)/float64(len(mappings)))
		}
	})
}

func equalsWithinDelta(x, y, delta float64) bool {
	return delta >= math.Abs(x-y)
}
