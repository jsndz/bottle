package raft

import (
	"context"
	"encoding/json"

	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
)

func (r *Raft) HandleElection(ctx context.Context, msg *pb.Message) *pb.Message {
	var req VoteRequest
	err := json.Unmarshal(msg.Payload, &req)
	if err != nil {
		return &pb.Message{
			Error: "Invalid Data",
		}
	}
	var res VoteResponse
	if r.Term > req.Term {
		res.Granted = false
		res.Term = r.Term
	} else {
		res.Granted = true
		r.mu.Lock()
		r.Term = req.Term
		r.mu.Unlock()
		res.Term = req.Term
	}

	payload, err := json.Marshal(res)
	return &pb.Message{
		Id:      msg.Id,
		Method:  "raft.election",
		Payload: payload,
		Type:    pb.FrameType_UNARY,
	}
}

func (r *Raft) HandleClientCommand(ctx context.Context, msg *pb.Message) *pb.Message {
	leader := r.Cluster.Nodes[r.LeaderID]
	client := rpc.NewClient(leader.Address, 1, r.Cluster.Pool)
	if r.LeaderID != r.Cluster.Self.ID {
		reply, err := client.Call(context.Background(), msg.Method, msg.Payload, msg.Headers)
		if err != nil {
			return &pb.Message{
				Error: err.Error(),
			}
		}
		return reply
	}
	// add the log to the raft logs
	r.Logs = append(r.Logs, Log{
		Term:    r.Term,
		Command: string(msg.Payload),
	})
	// broadcast the log to all the nodes in the cluster

	// if quorum is reached, commit the log and return success
	return nil
}

func (r *Raft) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("raft.election", r.HandleElection, nil)
	rpcServer.Handler.AddHandler("raft.client.command", r.HandleClientCommand, nil)
}
