package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
)

type Cluster struct {
	ID    string
	mu    sync.RWMutex
	Nodes map[string]*Node
	Self  *Node
	Pool  *rpc.GlobalPool
}

func NewCluster(self *Node, srv *rpc.Server, id string) *Cluster {
	nodes := make(map[string]*Node)
	nodes[self.ID] = self
	cls := &Cluster{
		ID:    id,
		Nodes: nodes,
		Self:  self,
		Pool:  rpc.NewGlobalPool(),
	}
	if srv != nil {
		cls.RegisterHandlers(srv)
	}
	return cls
}

func (c *Cluster) HeartbeatTicker() {
	ticker := time.Tick(5 * time.Second)

	go func() {
		for range ticker {
			c.Heartbeat()
		}
	}()
}

// node as a joining node
func (c *Cluster) Join(addr string) error {
	client := rpc.NewClient(addr, c.Pool)
	payload, _ := json.Marshal(c.Self)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Call(ctx, "cluster.join", payload, nil)
	if err != nil {
		return errors.New("failed to join the cluster: " + err.Error())
	}
	if resp == nil || resp.Error != "" {
		errMsg := "unknown error"
		if resp != nil {
			errMsg = resp.Error
		}
		return errors.New("failed to join the cluster: " + errMsg)
	}

	var cls Cluster
	if err := json.Unmarshal(resp.Payload, &cls); err != nil {
		return errors.New("failed to parse cluster state: " + err.Error())
	}

	c.mu.Lock()
	c.Nodes = cls.Nodes
	c.ID = cls.ID
	c.Self.State = ACTIVE
	c.mu.Unlock()

	return nil
}

func (c *Cluster) Peers() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	peers := make([]*Node, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		if node.ID != c.Self.ID {
			peers = append(peers, node)
		}
	}
	return peers
}

func (c *Cluster) QuorumSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return (len(c.Nodes) / 2) + 1
}

func (c *Cluster) BroadCast(method string, headers map[string]string, payload []byte) error {
	for _, node := range c.Peers() {
		go func(addr string) {
			client := rpc.NewClient(addr, c.Pool)
			client.Call(context.Background(), method, payload, headers)
		}(node.Address)
	}
	return nil
}

func (c *Cluster) Heartbeat() {
	payload, _ := json.Marshal(c.Self)
	peers := c.Peers()
	if len(peers) == 0 {
		return
	}

	type peerResult struct {
		node *Node
		err  error
	}

	ch := make(chan peerResult, len(peers))
	for _, node := range peers {
		go func(node *Node) {
			client := rpc.NewClient(node.Address, c.Pool)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := client.Call(ctx, "cluster.heartbeat", payload, nil)
			ch <- peerResult{node: node, err: err}
		}(node)
	}

	for i := 0; i < len(peers); i++ {
		res := <-ch
		c.mu.Lock()
		if node, ok := c.Nodes[res.node.ID]; ok && node != nil {
			if res.err != nil {
				node.MissedHeartbeats++
				if node.MissedHeartbeats > 3 {
					node.State = DEAD
					node.MissedHeartbeats = 0
				}
			} else {
				node.State = ACTIVE
				node.MissedHeartbeats = 0
				node.LastPing = time.Now()
			}
		}
		c.mu.Unlock()
	}
}

func (c *Cluster) Leave() error {
	payload, _ := json.Marshal(c.Self)
	c.mu.Lock()
	c.ID = ""
	c.Nodes = map[string]*Node{c.Self.ID: c.Self}
	c.Self.State = DEAD
	c.mu.Unlock()

	c.BroadCast("cluster.left", nil, payload)
	return nil
}

func (c *Cluster) BroadcastWithChannel(method string, headers map[string]string, payload []byte) (chan *pb.Message, int) {
	peers := c.Peers()
	if len(peers) == 0 {
		return nil, 0
	}
	ch := make(chan *pb.Message, len(peers))

	for _, node := range peers {
		go func(node *Node) {
			client := rpc.NewClient(node.Address, c.Pool)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			reply, err := client.Call(ctx, method, payload, headers)
			if err != nil {
				ch <- &pb.Message{
					Method:  method,
					Payload: nil,
					Error:   err.Error(),
				}
				return
			}
			ch <- reply
		}(node)
	}
	return ch, len(peers)
}

func (c *Cluster) SendToNode(nodeId string, method string, payload []byte, headers map[string]string) *pb.Message {
	c.mu.RLock()
	node, ok := c.Nodes[nodeId]
	c.mu.RUnlock()
	if !ok || node == nil {
		return &pb.Message{
			Method:  method,
			Payload: nil,
			Error:   "node not found",
		}
	}

	client := rpc.NewClient(node.Address, c.Pool)
	res, err := client.Call(context.Background(), method, payload, headers)
	if err != nil {
		return &pb.Message{
			Method:  method,
			Payload: nil,
			Error:   err.Error(),
		}
	}
	return res
}
