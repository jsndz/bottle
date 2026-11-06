package cluster

import (
	"testing"
)

func TestClusterPeersAndQuorum(t *testing.T) {
	node1 := NewNode("127.0.0.1:8001")
	node2 := NewNode("127.0.0.1:8002")
	node3 := NewNode("127.0.0.1:8003")

	cls := NewCluster(node1, nil, "test-cluster")
	cls.Nodes[node2.ID] = node2
	cls.Nodes[node3.ID] = node3

	if q := cls.QuorumSize(); q != 2 {
		t.Fatalf("expected quorum size 2, got %d", q)
	}

	peers := cls.Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}

	for _, p := range peers {
		if p.ID == node1.ID {
			t.Fatalf("Peers() should not contain Self node")
		}
	}
}
