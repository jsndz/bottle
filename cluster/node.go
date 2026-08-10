package cluster

import (
	"time"

	"github.com/google/uuid"
)

type NodeState string

const (
	DEAD    NodeState = "DEAD"
	JOINING NodeState = "JOINING"
	LEFT    NodeState = "LEFT"
	ACTIVE  NodeState = "ACTIVE"
)

type Node struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	State    NodeState `json:"state"`
	LastPing time.Time `json:"lastping"`
}

func NewNode(address string) *Node {
	return &Node{
		ID:      uuid.NewString(),
		Address: address,
		State:   JOINING,
	}
}
