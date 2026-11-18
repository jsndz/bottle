package scheduler

import (
	"testing"

	"github.com/jsndz/bottle/apiserver"
)

func TestNodeReadyFilter(t *testing.T) {
	pod := &apiserver.Pod{Metadata: apiserver.Metadata{Name: "pod-1"}}

	readyNode := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "node-1"},
		Status:   apiserver.NodeStatus{Conditions: []string{"Ready"}},
	}
	if !NodeReadyFilter(pod, readyNode) {
		t.Fatal("expected ready node to pass filter")
	}

	notReadyNode := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "node-2"},
		Status:   apiserver.NodeStatus{Conditions: []string{"NotReady"}},
	}
	if NodeReadyFilter(pod, notReadyNode) {
		t.Fatal("expected not ready node to fail filter")
	}

	emptyNode := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "node-3"},
	}
	if NodeReadyFilter(pod, emptyNode) {
		t.Fatal("expected node with no conditions to fail filter")
	}

	if NodeReadyFilter(pod, nil) {
		t.Fatal("expected nil node to fail filter")
	}
}

func TestNodeCapacityFilter(t *testing.T) {
	pod := &apiserver.Pod{Metadata: apiserver.Metadata{Name: "pod-1"}}

	validNode := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "node-1"},
		Status: apiserver.NodeStatus{
			Capacity: apiserver.NodeResources{CPU: 4000, Memory: 8192},
		},
	}
	if !NodeCapacityFilter(pod, validNode) {
		t.Fatal("expected node with valid capacity to pass filter")
	}

	zeroCPUNode := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "node-2"},
		Status: apiserver.NodeStatus{
			Capacity: apiserver.NodeResources{CPU: 0, Memory: 8192},
		},
	}
	if NodeCapacityFilter(pod, zeroCPUNode) {
		t.Fatal("expected node with 0 CPU to fail filter")
	}

	zeroMemNode := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "node-3"},
		Status: apiserver.NodeStatus{
			Capacity: apiserver.NodeResources{CPU: 4000, Memory: 0},
		},
	}
	if NodeCapacityFilter(pod, zeroMemNode) {
		t.Fatal("expected node with 0 Memory to fail filter")
	}

	if NodeCapacityFilter(pod, nil) {
		t.Fatal("expected nil node to fail filter")
	}
}

func TestFilterNodes(t *testing.T) {
	pod := &apiserver.Pod{Metadata: apiserver.Metadata{Name: "pod-1"}}

	nodes := map[string]apiserver.Node{
		"n1": {
			Metadata: apiserver.Metadata{Name: "n1"},
			Status: apiserver.NodeStatus{
				Conditions: []string{"Ready"},
				Capacity:   apiserver.NodeResources{CPU: 4000, Memory: 8192},
			},
		},
		"n2": {
			Metadata: apiserver.Metadata{Name: "n2"},
			Status: apiserver.NodeStatus{
				Conditions: []string{"NotReady"},
				Capacity:   apiserver.NodeResources{CPU: 4000, Memory: 8192},
			},
		},
		"n3": {
			Metadata: apiserver.Metadata{Name: "n3"},
			Status: apiserver.NodeStatus{
				Conditions: []string{"Ready"},
				Capacity:   apiserver.NodeResources{CPU: 0, Memory: 8192},
			},
		},
		"n4": {
			Metadata: apiserver.Metadata{Name: "n4"},
			Status: apiserver.NodeStatus{
				Conditions: []string{"Ready"},
				Capacity:   apiserver.NodeResources{CPU: 2000, Memory: 4096},
			},
		},
	}

	filtered := FilterNodes(pod, nodes)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 nodes to pass filter, got %d", len(filtered))
	}

	names := make(map[string]bool)
	for _, n := range filtered {
		names[n.Metadata.Name] = true
	}
	if !names["n1"] || !names["n4"] {
		t.Fatalf("unexpected filtered nodes: %+v", filtered)
	}

	customFilter := func(p *apiserver.Pod, n *apiserver.Node) bool {
		return n.Metadata.Name == "n4"
	}
	customFiltered := FilterNodes(pod, nodes, customFilter)
	if len(customFiltered) != 1 || customFiltered[0].Metadata.Name != "n4" {
		t.Fatalf("expected only n4 to pass custom filter, got %+v", customFiltered)
	}
}
