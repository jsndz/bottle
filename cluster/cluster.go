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

func NewCluster(Self *Node, srv *rpc.Server, id string) *Cluster {
	Nodes := make(map[string]*Node)
	Nodes[Self.ID] = Self
	Pool := rpc.NewGlobalPool()
	cls := &Cluster{
		ID:    id,
		Nodes: Nodes,
		Self:  Self,
		Pool:  Pool,
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

	client := rpc.NewClient(addr, 1, c.Pool)
	payload, _ := json.Marshal(c.Self)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, _ := client.Call(ctx, "cluster.join", payload, nil)
	if resp.Error != "" {
		return errors.New("Failed to Join the Cluster " + resp.Error)
	}
	// in payload it with c.Nodes
	var cls Cluster
	json.Unmarshal(resp.Payload, &cls)

	c.mu.Lock()
	c.Nodes = cls.Nodes
	c.ID = cls.ID
	c.Self.State = ACTIVE
	c.mu.Unlock()

	return nil
}

func (c *Cluster) BroadCast(method string, headers map[string]string, payload []byte) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, node := range c.Nodes {
		if node.ID == c.Self.ID {
			continue
		}
		go func(addr string) {
			client := rpc.NewClient(addr, 1, c.Pool)
			client.Call(context.Background(), method, payload, headers)
		}(node.Address)
	}
	return nil
}

func (c *Cluster) Heartbeat() {
	payload, _ := json.Marshal(c.Self)
	c.mu.RLock()
	peers := make([]*Node, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		if node.ID != c.Self.ID {
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
			client := rpc.NewClient(node.Address, 1, c.Pool)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			reply, err := client.Call(ctx, "cluster.heartbeat", payload, nil)
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
		c.mu.Lock()
		if msg.Error == "No Reply" {
			c.Nodes[node.ID].MissedHeartbeats++
			if c.Nodes[node.ID].MissedHeartbeats > 3 {
				c.Nodes[node.ID].State = DEAD
				c.Nodes[node.ID].MissedHeartbeats = 0
			}
			c.mu.Unlock()
			continue
		}
		c.Nodes[node.ID].State = ACTIVE
		c.Nodes[node.ID].LastPing = time.Now()
		c.mu.Unlock()
	}
}

func (c *Cluster) Leave(cl *rpc.Client) error {
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
	c.mu.RLock()
	peers := make([]*Node, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		if node.ID != c.Self.ID {
			peers = append(peers, node)
		}
	}
	c.mu.RUnlock()

	if len(peers) == 0 {
		return nil, 0
	}
	ch := make(chan *pb.Message, len(peers))

	for _, node := range peers {
		go func(node *Node) {
			client := rpc.NewClient(node.Address, 1, c.Pool)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			reply, err := client.Call(ctx, method, payload, nil)
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
