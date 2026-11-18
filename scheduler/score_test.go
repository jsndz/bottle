package scheduler

import (
	"errors"
	"testing"

	"github.com/jsndz/bottle/apiserver"
)

func TestLeastAllocatedScore(t *testing.T) {
	pod := &apiserver.Pod{Metadata: apiserver.Metadata{Name: "pod-1"}}

	node := &apiserver.Node{
		Metadata: apiserver.Metadata{Name: "n1"},
		Status: apiserver.NodeStatus{
			Capacity: apiserver.NodeResources{CPU: 4000, Memory: 8192},
		},
	}

	score := LeastAllocatedScore(pod, node)
	if score != 4000+8192 {
		t.Fatalf("expected score %d, got %d", 4000+8192, score)
	}

	if LeastAllocatedScore(pod, nil) != 0 {
		t.Fatal("expected 0 score for nil node")
	}
}

func TestScoreNodesEmptyAndSingle(t *testing.T) {
	pod := &apiserver.Pod{Metadata: apiserver.Metadata{Name: "pod-1"}}

	_, err := ScoreNodes(pod, nil)
	if !errors.Is(err, ErrNoNodesAvailable) {
		t.Fatalf("expected ErrNoNodesAvailable, got %v", err)
	}

	single := []*apiserver.Node{
		{Metadata: apiserver.Metadata{Name: "only-node"}},
	}
	winner, err := ScoreNodes(pod, single)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if winner != "only-node" {
		t.Fatalf("expected only-node, got %s", winner)
	}
}

func TestScoreNodesMultiple(t *testing.T) {
	pod := &apiserver.Pod{Metadata: apiserver.Metadata{Name: "pod-1"}}

	nodes := []*apiserver.Node{
		{
			Metadata: apiserver.Metadata{Name: "small-node"},
			Status: apiserver.NodeStatus{
				Capacity: apiserver.NodeResources{CPU: 1000, Memory: 2048},
			},
		},
		{
			Metadata: apiserver.Metadata{Name: "large-node"},
			Status: apiserver.NodeStatus{
				Capacity: apiserver.NodeResources{CPU: 8000, Memory: 16384},
			},
		},
		{
			Metadata: apiserver.Metadata{Name: "medium-node"},
			Status: apiserver.NodeStatus{
				Capacity: apiserver.NodeResources{CPU: 4000, Memory: 8192},
			},
		},
	}

	winner, err := ScoreNodes(pod, nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if winner != "large-node" {
		t.Fatalf("expected large-node, got %s", winner)
	}

	customScorer := func(p *apiserver.Pod, n *apiserver.Node) int64 {
		if n.Metadata.Name == "small-node" {
			return 999999
		}
		return 0
	}
	customWinner, err := ScoreNodes(pod, nodes, customScorer)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if customWinner != "small-node" {
		t.Fatalf("expected small-node with custom scorer, got %s", customWinner)
	}
}

func TestScoreNodesDeterministicTie(t *testing.T) {
	pod := &apiserver.Pod{Metadata: apiserver.Metadata{Name: "pod-1"}}

	nodes := []*apiserver.Node{
		{
			Metadata: apiserver.Metadata{Name: "node-b"},
			Status: apiserver.NodeStatus{
				Capacity: apiserver.NodeResources{CPU: 4000, Memory: 8192},
			},
		},
		{
			Metadata: apiserver.Metadata{Name: "node-a"},
			Status: apiserver.NodeStatus{
				Capacity: apiserver.NodeResources{CPU: 4000, Memory: 8192},
			},
		},
	}

	winner, err := ScoreNodes(pod, nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if winner != "node-a" {
		t.Fatalf("expected tie-breaker to pick node-a lexicographically, got %s", winner)
	}
}
