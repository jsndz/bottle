package cluster

import (
	"context"
	"encoding/json"

	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
)

// node as seed node
func (c *Cluster) HandleJoin(ctx context.Context, msg *pb.Message) *pb.Message {
	var joiningNode Node
	if err := json.Unmarshal(msg.Payload, &joiningNode); err != nil {
		return &pb.Message{
			Id:     msg.Id,
			Type:   pb.FrameType_UNARY,
			Method: msg.Method,
			Error:  "Invalid Payload",
		}
	}
	c.mu.Lock()
	joiningNode.State = ACTIVE
	c.nodes[joiningNode.ID] = &joiningNode
	c.mu.Unlock()
	payload, _ := json.Marshal(c)
	c.BroadCast("cluster.update", nil, payload)

	return &pb.Message{
		Id:      msg.Id,
		Type:    pb.FrameType_UNARY,
		Method:  msg.Method,
		Payload: payload,
	}
}

func (c *Cluster) HandleUpdate(ctx context.Context, msg *pb.Message) *pb.Message {
	var cls Cluster
	json.Unmarshal(msg.Payload, &cls)
	c.mu.Lock()
	c.nodes = cls.nodes
	c.mu.Unlock()
	return &pb.Message{
		Id:      msg.Id,
		Type:    pb.FrameType_UNARY,
		Method:  msg.Method,
		Payload: nil,
	}
}

func (c *Cluster) HandleLeave(ctx context.Context, msg *pb.Message) *pb.Message {
	var node Node
	json.Unmarshal(msg.Payload, &node)
	c.mu.Lock()
	delete(c.nodes, node.ID)
	c.mu.Unlock()
	return &pb.Message{
		Id:      msg.Id,
		Type:    pb.FrameType_UNARY,
		Method:  msg.Method,
		Payload: nil,
	}
}

func (c *Cluster) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("cluster.join", c.HandleJoin, nil)
	rpcServer.Handler.AddHandler("cluster.update", c.HandleUpdate, nil)
	rpcServer.Handler.AddHandler("cluster.left", c.HandleLeave, nil)
}
