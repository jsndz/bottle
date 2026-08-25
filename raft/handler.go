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
	prevLog := r.Logs[len(r.Logs)]
	log := Log{
		Term:    r.Term,
		Command: string(msg.Payload),
		Index:   prevLog.Index + 1,
	}
	// add the log to the raft logs
	r.Logs = append(r.Logs, log)
	// broadcast the log to all the nodes in the cluster
	appendEntry := &AppendEntriesReq{
		Term:              r.Term,
		LeaderID:          r.LeaderID,
		Logs:              log,
		PrevLogIndex:      prevLog.Index,
		PrevLogTerm:       prevLog.Term,
		LeaderCommitIndex: r.CommitIndex,
	}
	payload, err := json.Marshal(appendEntry)
	if err != nil {
		return &pb.Message{
			Error: err.Error(),
		}
	}
	ch, numPeers := r.Cluster.BroadcastWithChannel("raft.append", nil, payload)
	numberOfAppends := 1
	majority := ((numPeers + 1) / 2) + 1

	for i := 0; i < numPeers; i++ {
		data := <-ch
		if data.Error != "" {
			continue
		}
		var res AppendEntriesRes
		if err := json.Unmarshal(data.Payload, &res); err != nil {
			continue
		}
		if res.Success {
			numberOfAppends++
		}
		if numberOfAppends >= majority {
			break
		}
	}

	// if quorum is reached, commit the log and return success
	r.CommitIndex = log.Index
	return &pb.Message{
		Method: "raft.appended",
		Type:   pb.FrameType_UNARY,
	}
}

func (r *Raft) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("raft.election", r.HandleElection, nil)
	rpcServer.Handler.AddHandler("raft.client.command", r.HandleClientCommand, nil)
}
