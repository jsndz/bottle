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
}

func NewCluster(self *Node, srv *rpc.Server) *Cluster {
	nodes := make(map[string]*Node)
	nodes[self.ID] = self
	cls := &Cluster{
		id:    uuid.NewString(),
		nodes: nodes,
		self:  self,
	}
	cls.RegisterHandlers(srv)
	return cls
}

// node as a joining node
func (c *Cluster) Join(cl *rpc.Client) error {
	payload, _ := json.Marshal(c.self)
	resp, _ := cl.Call(context.Background(), "cluster.Join", payload, nil)
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

func (c *Cluster) Leave(cl *rpc.Client) error {
	return nil
}
