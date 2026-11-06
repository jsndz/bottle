package cluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
)

// node as seed node
func (c *Cluster) HandleJoin(ctx context.Context, msg *pb.Message) *pb.Message {
	var joiningNode Node
	if err := json.Unmarshal(msg.Payload, &joiningNode); err != nil {
		return rpc.NewUnaryResponse(msg, nil, "Invalid Payload")
	}
	client := rpc.NewClient(joiningNode.Address, c.Pool)
	_, err := client.Ping(ctx)
	if err != nil {
		return rpc.NewUnaryResponse(msg, nil, "Can't Connect")
	}
	c.mu.Lock()
	joiningNode.State = ACTIVE
	c.Nodes[joiningNode.ID] = &joiningNode
	c.mu.Unlock()
	payload, _ := json.Marshal(c)
	c.BroadCast("cluster.update", nil, payload)

	return rpc.NewUnaryResponse(msg, payload, "")
}

func (c *Cluster) HandleUpdate(ctx context.Context, msg *pb.Message) *pb.Message {
	var cls Cluster
	if err := json.Unmarshal(msg.Payload, &cls); err == nil && cls.Nodes != nil {
		c.mu.Lock()
		c.Nodes = cls.Nodes
		c.mu.Unlock()
	}
	return rpc.NewUnaryResponse(msg, nil, "")
}

func (c *Cluster) HandleLeave(ctx context.Context, msg *pb.Message) *pb.Message {
	var node Node
	if err := json.Unmarshal(msg.Payload, &node); err == nil && node.ID != "" {
		c.mu.Lock()
		delete(c.Nodes, node.ID)
		c.mu.Unlock()
	}
	return rpc.NewUnaryResponse(msg, nil, "")
}

func (c *Cluster) HandleHeartBeat(ctx context.Context, msg *pb.Message) *pb.Message {
	payload, _ := json.Marshal(c.Self)

	var sender Node
	if err := json.Unmarshal(msg.Payload, &sender); err == nil {
		c.mu.Lock()
		c.Self.LastPing = time.Now()
		if peer, ok := c.Nodes[sender.ID]; ok && peer != nil {
			peer.LastPing = time.Now()
			peer.MissedHeartbeats = 0
			peer.State = ACTIVE
		}
		c.mu.Unlock()
	}
	return rpc.NewUnaryResponse(msg, payload, "")
}

func (c *Cluster) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("cluster.join", c.HandleJoin, nil)
	rpcServer.Handler.AddHandler("cluster.update", c.HandleUpdate, nil)
	rpcServer.Handler.AddHandler("cluster.left", c.HandleLeave, nil)
	rpcServer.Handler.AddHandler("cluster.heartbeat", c.HandleHeartBeat, nil)
}
