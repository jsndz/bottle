package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
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

func (c *Cluster) HeartbeatTicker() {
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for range ticker.C {
			c.Heartbeat()
		}
	}()
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
	c.self.State = ACTIVE
	c.mu.Unlock()

	return nil
}

func (c *Cluster) BroadCast(method string, headers map[string]string, payload []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, node := range c.nodes {
		if node.ID == c.self.ID {
			continue
		}
		go func(addr string) {
			client := rpc.NewClient(addr, 1, c.pool)
			client.Call(context.Background(), method, payload, headers)
		}(node.Address)
	}
	return nil
}

func (c *Cluster) Heartbeat() {
	payload, _ := json.Marshal(c.self)
	c.mu.RLock()
	peers := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		if node.ID != c.self.ID {
			peers = append(peers, node)
		}
	}
	c.mu.RUnlock()

	if len(peers) == 0 {
		return
	}

	ch := make(chan *pb.Message, len(peers))
	for _, node := range peers {

		go func(node *Node) {
			client := rpc.NewClient(node.Address, 1, c.pool)
			reply, err := client.Call(context.Background(), "cluster.heartbeat", payload, nil)
			if err != nil {
				payload, _ := json.Marshal(node)
				ch <- &pb.Message{
					Payload: payload,
					Error:   "No Reply",
				}
				return
			}
			ch <- reply
		}(node)
	}
	for i := 0; i < len(peers); i++ {
		msg := <-ch
		var node Node
		json.Unmarshal(msg.Payload, &node)

		if msg.Error == "No Reply" {
			c.nodes[node.ID].State = DEAD
			return
		}
		c.nodes[node.ID].State = ACTIVE
		c.nodes[node.ID].LastPing = time.Now()

	}

}

func (c *Cluster) Leave(cl *rpc.Client) error {
	payload, _ := json.Marshal(c.self)
	c.id = ""
	for _, node := range c.nodes {
		if node.ID == c.self.ID {
			continue
		}
		delete(c.nodes, node.ID)
	}
	c.BroadCast("cluster.left", nil, payload)
	return nil
}
