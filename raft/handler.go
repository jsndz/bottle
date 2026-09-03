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

	} else if r.Term == req.Term {
		res.Term = r.Term

		if len(r.Logs) > 0 {
			lastLog := r.Logs[len(r.Logs)-1]

			if lastLog.Term > req.LastLogTerm ||
				(lastLog.Term == req.LastLogTerm && lastLog.Index > req.LastLogIndex) {
				res.Granted = false
			} else {
				res.Granted = true
			}
		} else {
			res.Granted = true
		}
	}
	if res.Granted == true {
		r.Term = req.Term
		r.Role = Follower
		r.LeaderID = req.CandidateId
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

	if r.LeaderID != r.Cluster.Self.ID {
		leader := r.Cluster.Nodes[r.LeaderID]
		if r.LeaderID == "" {
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

	log := Log{
		Term:    r.Term,
		Command: (msg.Payload),
		Index:   prevLogIndex + 1,
	}
	// add the log to the raft logs
	r.Logs = append(r.Logs, log)
	// broadcast the log to all the nodes in the cluster
	appendEntry := &AppendEntriesReq{
		Term:              r.Term,
		LeaderID:          r.LeaderID,
		Logs:              log,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
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
	if numberOfAppends < majority {
		return &pb.Message{
			Id:    msg.Id,
			Error: "failed to reach quorum consensus",
		}
	}
	// if quorum is reached, commit the log and return success
	r.CommitIndex = log.Index
	r.FSM.Apply(log.Command)
	return &pb.Message{
		Method: "raft.appended",
		Type:   pb.FrameType_UNARY,
	}
}

func (r *Raft) HandleAppend(ctx context.Context, msg *pb.Message) *pb.Message {
	// assuming reciever is follower
	// check term if greater then add to log
	//apply to FSM on heartbeat

	var req AppendEntriesReq
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		return &pb.Message{
			Error: err.Error(),
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	var prevLogIndex, prevLogTerm int
	var res AppendEntriesRes

	if len(r.Logs) > 0 {
		prevLog := r.Logs[len(r.Logs)-1]
		prevLogIndex = prevLog.Index
		prevLogTerm = prevLog.Term
	}

	if req.Term < r.Term {
		res.Success = false
		res.Term = r.Term
		payload, _ := json.Marshal(res)

		return &pb.Message{
			Payload: payload,
			Method:  msg.Method,
		}
	}
	if req.PrevLogIndex > prevLogIndex || req.PrevLogTerm != prevLogTerm {
		// if leader is greater then fine since follower can get the data
		res.Success = false
		res.Term = r.Term
		payload, _ := json.Marshal(res)
		// leader should jump back one by one and finds the match
		return &pb.Message{
			Payload: payload,
			Method:  msg.Method,
		}
	}
	if req.PrevLogIndex != prevLogIndex {
		r.Logs = r.Logs[:req.PrevLogIndex]
		r.Logs[req.PrevLogIndex+1] = req.Logs

	} else {
		r.Logs = append(r.Logs, req.Logs)
	}
	if req.Term > r.Term {
		r.Term = req.Term
		r.Role = Follower
	}
	r.LeaderID = req.LeaderID
	res.Success = true
	res.Term = r.Term
	payload, _ := json.Marshal(res)
	return &pb.Message{
		Method:  msg.Method,
		Payload: payload,
	}
}

func (r *Raft) HandleHeartbeat(ctx context.Context, msg *pb.Message) *pb.Message {
	var req AppendEntriesReq
	err := json.Unmarshal(msg.Payload, &req)
	if err != nil {
		return &pb.Message{
			Error: err.Error(),
		}
	}
	var res AppendEntriesRes
	if req.Term < r.Term {
		res.Success = false
		res.Term = r.Term
	} else {
		res.Success = true
		res.Term = req.Term
		r.mu.Lock()
		r.Term = req.Term
		r.Ticker.Reset(r.Timeout)
		r.mu.Unlock()
	}

	payload, _ := json.Marshal(res)
	return &pb.Message{
		Method:  msg.Method,
		Payload: payload,
	}
}

func (r *Raft) RegisterHandlers(rpcServer *rpc.Server) {
	rpcServer.Handler.AddHandler("raft.election", r.HandleElection, nil)
	rpcServer.Handler.AddHandler("raft.client.command", r.HandleClientCommand, nil)
	rpcServer.Handler.AddHandler("raft.append", r.HandleAppend, nil)
	rpcServer.Handler.AddHandler("raft.heartbeat", r.HandleHeartbeat, nil)
}
