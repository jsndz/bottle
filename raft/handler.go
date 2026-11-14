package raft

import (
	"context"
	"encoding/json"

	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
)

func (r *Raft) HandleElection(ctx context.Context, msg *pb.Message) *pb.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	var req VoteRequest
	err := json.Unmarshal(msg.Payload, &req)
	if err != nil {
		return rpc.NewUnaryResponse(msg, nil, "Invalid Data")
	}

	var res VoteResponse
	if r.Term < req.Term {
		r.convertToFollower(req.Term, "")
	}
	lastLogTerm, lastLogIndex := r.GetPrevLog()
	logOk := (lastLogTerm < req.LastLogTerm) || (lastLogTerm == req.LastLogTerm && lastLogIndex <= req.LastLogIndex)

	if r.Term == req.Term && logOk && (r.VotedFor == "" || r.VotedFor == req.CandidateId) {
		r.VotedFor = req.CandidateId
		res.Granted = true
		res.Term = r.Term
		res.VoterId = r.Cluster.Self.ID
		if r.Ticker != nil {
			r.Ticker.Reset(r.Timeout)
		}
	} else {
		res.Granted = false
		res.Term = r.Term
		res.VoterId = r.Cluster.Self.ID
	}

	payload, _ := json.Marshal(res)
	return rpc.NewUnaryResponse(msg, payload, "")
}

func (r *Raft) HandlePropose(ctx context.Context, msg *pb.Message) *pb.Message {
	r.mu.Lock()
	isLeader := (r.Role == Leader)
	leaderID := r.CurrentLeader
	r.mu.Unlock()

	if !isLeader {
		if leaderID == "" {
			return rpc.NewUnaryResponse(msg, nil, "no leader currently elected")
		}
		leader, ok := r.Cluster.Nodes[leaderID]
		if !ok {
			return rpc.NewUnaryResponse(msg, nil, "leader address unknown")
		}
		client := rpc.NewClient(leader.Address, r.Cluster.Pool)
		reply, err := client.Call(ctx, msg.Method, msg.Payload, msg.Headers)
		if err != nil {
			return rpc.NewUnaryResponse(msg, nil, err.Error())
		}
		return reply
	}

	err := r.Propose(msg.Payload)
	if err != nil {
		return rpc.NewUnaryResponse(msg, nil, err.Error())
	}

	resp := rpc.NewUnaryResponse(msg, nil, "")
	resp.Method = msg.Method
	return resp
}

func (r *Raft) HandleClientCommand(ctx context.Context, msg *pb.Message) *pb.Message {
	return r.HandlePropose(ctx, msg)
}

func (r *Raft) HandleAppend(ctx context.Context, msg *pb.Message) *pb.Message {
	var req AppendEntriesReq
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return rpc.NewUnaryResponse(msg, nil, err.Error())
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	var res AppendEntriesRes
	res.FollowerID = r.Cluster.Self.ID

	if req.Term > r.Term {
		r.convertToFollower(req.Term, "")
	}
	if r.Term == req.Term {
		r.convertToFollower(req.Term, req.CurrentLeader)
	}
	logOk := (req.PrevLogIndex == 0) || (req.PrevLogIndex <= len(r.Logs) && r.Logs[req.PrevLogIndex-1].Term == req.PrevLogTerm)

	if r.Term == req.Term && logOk {
		r.AppendLog(req.Logs, req.PrevLogIndex, req.LeaderCommitIndex)
		ack := req.PrevLogIndex + len(req.Logs)
		res.Ack = ack
		res.Success = true
		res.Term = r.Term
	} else {
		res.Ack = 0
		res.Success = false
		res.Term = r.Term
	}
	payload, _ := json.Marshal(res)

	resp := rpc.NewUnaryResponse(msg, payload, "")
	resp.Method = "raft.log"
	return resp
}

func (r *Raft) HandleHeartbeat(ctx context.Context, msg *pb.Message) *pb.Message {
	return r.HandleAppend(ctx, msg)
}

func (r *Raft) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("raft.election", r.HandleElection, nil)
	rpcServer.Handler.AddHandler("raft.client.command", r.HandleClientCommand, nil)
	rpcServer.Handler.AddHandler("raft.logs", r.HandleAppend, nil)
	rpcServer.Handler.AddHandler("raft.heartbeat", r.HandleHeartbeat, nil)
	rpcServer.Handler.AddHandler("raft.propose", r.HandlePropose, nil)
}
