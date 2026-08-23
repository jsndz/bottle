package raft

import (
	"context"

	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
)

func (r *Raft) HandleElection(ctx context.Context, msg *pb.Message) *pb.Message {
	return nil
}

func (r *Raft) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("cluster.join", r.HandleElection, nil)
}
