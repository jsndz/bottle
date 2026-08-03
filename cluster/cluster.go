package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/jsndz/bottle/rpc"
)

type Cluster struct {
	id    string
	mu    sync.RWMutex
	nodes map[string]*Node
	self  *Node
	pool  *rpc.GlobalPool
}

func NewCluster(self *Node, srv *rpc.Server) *Cluster {
	nodes := make(map[string]*Node)
	nodes[self.ID] = self
	pool := rpc.NewGlobalPool()
	cls := &Cluster{
		id:    uuid.NewString(),
		nodes: nodes,
		self:  self,
		pool:  pool,
	}
	cls.RegisterHandlers(srv)
	return cls
}

// node as a joining node
func (c *Cluster) Join(addr string) error {

	client := rpc.NewClient(addr, 1, c.pool)
	payload, _ := json.Marshal(c.self)
	resp, _ := client.Call(context.Background(), "cluster.Join", payload, nil)
	if resp.Error != "" {
		return errors.New("Failed to Join the Cluster " + resp.Error)
	}
	// in payload it with c.Nodes
	var cls Cluster
	json.Unmarshal(resp.Payload, &cls)

	c.mu.Lock()
	c.nodes = cls.nodes
	c.id = cls.id
	c.mu.Unlock()

	return nil
}

func (c *Cluster) BroadCast(method string, headers map[string]string, payload []byte) error {

	for _, node := range c.nodes {
		if node == c.self {
			break
		}
		client := rpc.NewClient(node.Address, 1, c.pool)
		client.Call(context.Background(), method, payload, headers)
	}

	return nil
}

func (c *Cluster) Leave(cl *rpc.Client) error {
	payload, _ := json.Marshal(c.self)
	c.BroadCast("cluster.left", nil, payload)
	return nil
}
