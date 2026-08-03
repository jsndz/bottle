package cluster

import (
	"net"
	"time"

	"github.com/google/uuid"
)

type NodeState string

const (
	DEAD   NodeState = "DEAD"
	JOING  NodeState = "JOINING"
	LEFT   NodeState = "LEFT"
	ACTIVE NodeState = "ACTIVE"
)

type Node struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	State    NodeState `json:"srate"`
	LastPing time.Time `json:"lastping"`
}

func NewNode(address string) *Node {
	return &Node{
		ID:      uuid.NewString(),
		Address: address,
		State:   JOING,
	}
}

func (n *Node) Ping() bool {
	conn, err := net.DialTimeout("tcp", n.Address, 3*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
