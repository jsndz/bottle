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

func (r *Raft) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("raft.election", r.HandleElection, nil)
}
