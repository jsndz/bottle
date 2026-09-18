package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jsndz/bottle/cluster"
	"github.com/jsndz/bottle/rpc"
	pb "github.com/jsndz/bottle/rpc/proto"
)

func (r *Raft) HandleElection(ctx context.Context, msg *pb.Message) *pb.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	var req VoteRequest
	err := json.Unmarshal(msg.Payload, &req)
	if err != nil {
		return &pb.Message{
			Error: "Invalid Data",
		}
	}

	var res VoteResponse
	if r.Term < req.Term {
		r.Term = req.Term
		r.VotedFor = ""
		r.Role = Follower
	}
	lastLogTerm := 0

	if len(r.Logs) > 0 {
		lastLogTerm = r.Logs[len(r.Logs)-1].Term
	}
	logOk := (lastLogTerm < req.LastLogTerm) || (lastLogTerm == req.LastLogTerm) && (len(r.Logs) <= req.LastLogIndex)

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

	payload, err := json.Marshal(res)
	return &pb.Message{
		Id:      msg.Id,
		Method:  "raft.election",
		Payload: payload,
		Type:    pb.FrameType_UNARY,
	}
}

func (r *Raft) HandleClientCommand(ctx context.Context, msg *pb.Message) *pb.Message {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.CurrentLeader != r.Cluster.Self.ID {
		leader := r.Cluster.Nodes[r.CurrentLeader]
		if r.CurrentLeader == "" {
			return &pb.Message{Id: msg.Id, Error: "no leader currently elected"}
		}
		client := rpc.NewClient(leader.Address, 1, r.Cluster.Pool)
		reply, err := client.Call(context.Background(), msg.Method, msg.Payload, msg.Headers)
		if err != nil {
			return &pb.Message{
				Error: err.Error(),
			}
		}
		return reply
	}

	var prevLogIndex, prevLogTerm int
	if len(r.Logs) > 0 {
		prevLog := r.Logs[len(r.Logs)-1]
		prevLogIndex = prevLog.Index
		prevLogTerm = prevLog.Term
	}
	fmt.Printf("prevLogTerm: %v\n", prevLogTerm)
	log := Log{
		Term:    r.Term,
		Command: (msg.Payload),
		Index:   prevLogIndex + 1,
	}
	// add the log to the raft logs
	r.Logs = append(r.Logs, log)
	r.AckLength[r.Cluster.Self.ID] = len(r.Logs)

	peers := make([]*cluster.Node, 0)
	for _, node := range r.Cluster.Nodes {
		if node.ID != r.Cluster.Self.ID {
			peers = append(peers, node)
		}
	}
	numPeers := len(peers)

	// Single node cluster
	if numPeers == 0 {
		r.CommitLogEntries()
		return &pb.Message{
			Method: "raft.appended",
			Type:   pb.FrameType_UNARY,
		}
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	ch := make(chan *pb.Message, numPeers)
	for _, peer := range peers {
		go func(node *cluster.Node) {
			ch <- r.ReplicateLog(r.Cluster.Self.ID, node.ID)
		}(peer)
	}

	for i := 0; i < numPeers; i++ {
		select {
		case resMsg := <-ch:
			var res AppendEntriesRes
			_ = json.Unmarshal(resMsg.Payload, &res)
			if r.Term == res.Term && r.AckLength[res.FollowerID] <= res.Ack {
				r.SentLength[res.FollowerID] = res.Ack
				r.AckLength[res.FollowerID] = res.Ack
				r.CommitLogEntries()
			} else if r.SentLength[res.FollowerID] > 0 {
				r.SentLength[res.FollowerID] -= 1
				r.ReplicateLog(r.Cluster.Self.ID, res.FollowerID)
			} else if r.Term < res.Term {
				r.Role = Follower
				r.Term = res.Term
				r.VotedFor = ""
				if r.Ticker != nil {
					r.Ticker.Reset(r.Timeout)
				}
			}
		case <-ctxTimeout.Done():
			break
		}
	}

	if r.CommitIndex >= log.Index {
		return &pb.Message{
			Method: "raft.appended",
			Type:   pb.FrameType_UNARY,
		}
	}
	return &pb.Message{
		Id:    msg.Id,
		Error: "failed to reach quorum consensus",
	}
}

func (r *Raft) HandleAppend(ctx context.Context, msg *pb.Message) *pb.Message {
	var req AppendEntriesReq
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return &pb.Message{
			Error: err.Error(),
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	var res AppendEntriesRes
	res.FollowerID = r.Cluster.Self.ID

	if req.Term > r.Term {
		r.Term = req.Term
		r.VotedFor = ""
		if r.Ticker != nil {
			r.Ticker.Reset(r.Timeout)
		}
	}
	if r.Term == req.Term {
		r.Role = Follower
		r.CurrentLeader = req.CurrentLeader
		if r.Ticker != nil {
			r.Ticker.Reset(r.Timeout)
		}
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

	return &pb.Message{
		Method:  "raft.log",
		Payload: payload,
	}
}

func (r *Raft) HandleHeartbeat(ctx context.Context, msg *pb.Message) *pb.Message {
	return r.HandleAppend(ctx, msg)
}

func (r *Raft) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("raft.election", r.HandleElection, nil)
	rpcServer.Handler.AddHandler("raft.client.command", r.HandleClientCommand, nil)
	rpcServer.Handler.AddHandler("raft.logs", r.HandleAppend, nil)
	rpcServer.Handler.AddHandler("raft.heartbeat", r.HandleHeartbeat, nil)
}
