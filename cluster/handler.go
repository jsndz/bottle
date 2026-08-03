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
	c.nodes[joiningNode.ID] = &joiningNode
	joiningNode.State = ACTIVE
	c.mu.Unlock()
	payload, _ := json.Marshal(c)

	return &pb.Message{
		Id:      msg.Id,
		Type:    pb.FrameType_UNARY,
		Method:  msg.Method,
		Payload: payload,
	}
}

func (c *Cluster) HandleLeave(ctx context.Context, msg *pb.Message) *pb.Message {
	message := &pb.Message{}
	return message
}

func (c *Cluster) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("cluster.Join", c.HandleJoin, nil)

}
