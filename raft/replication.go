package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jsndz/bottle/cluster"
	pb "github.com/jsndz/bottle/rpc/proto"
)

func (r *Raft) AppendLog(suffix []Log, prevLogIndex, leaderCommit int) {
	for i, entry := range suffix {
		idx := prevLogIndex + 1 + i
		if idx <= len(r.Logs) {
			if r.Logs[idx-1].Term != entry.Term {
				r.Logs = r.Logs[:idx-1]
				r.Logs = append(r.Logs, entry)
			}
		} else {
			r.Logs = append(r.Logs, entry)
		}
	}
	if leaderCommit > r.CommitIndex {
		for i := r.CommitIndex; i < leaderCommit && i < len(r.Logs); i++ {
			if r.FSM != nil {
				r.FSM.Apply(r.Logs[i].Command)
			}
		}
		r.CommitIndex = min(leaderCommit, len(r.Logs))
	}
}

func (r *Raft) ReplicateLog(followerId string) *pb.Message {
	r.mu.Lock()
	sentLen := r.SentLength[followerId]
	if sentLen <= 0 {
		sentLen = 1
	}
	prevLogIndex := sentLen - 1
	prevLogTerm := 0
	if prevLogIndex > 0 && prevLogIndex <= len(r.Logs) {
		prevLogTerm = r.Logs[prevLogIndex-1].Term
	}

	suffix := make([]Log, 0)
	if prevLogIndex < len(r.Logs) {
		suffix = append(suffix, r.Logs[prevLogIndex:]...)
	}

	req := &AppendEntriesReq{
		Term:              r.Term,
		LeaderCommitIndex: r.CommitIndex,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		CurrentLeader:     r.CurrentLeader,
		Logs:              suffix,
	}
	payload, err := json.Marshal(req)
	r.mu.Unlock()

	if err != nil {
		return &pb.Message{Error: err.Error()}
	}
	return r.Cluster.SendToNode(followerId, "raft.logs", payload, nil)
}

func (r *Raft) processAppendResponse(res AppendEntriesRes, currentTerm int) bool {
	if res.Term > r.Term {
		r.convertToFollower(res.Term, "")
		return false
	}
	if r.Role != Leader || r.Term != currentTerm {
		return false
	}

	if res.Success {
		if res.Ack >= r.AckLength[res.FollowerID] {
			r.SentLength[res.FollowerID] = res.Ack
			r.AckLength[res.FollowerID] = res.Ack
			r.CommitLogEntries()
		}
	} else {
		if r.SentLength[res.FollowerID] > 1 {
			r.SentLength[res.FollowerID]--
		}
		go r.ReplicateLog(res.FollowerID)
	}
	return true
}

func (r *Raft) Heartbeat() error {
	r.mu.Lock()
	if r.Role != Leader {
		r.mu.Unlock()
		return nil
	}
	prevLogTerm, prevLogIndex := r.GetPrevLog()
	req := &AppendEntriesReq{
		Term:              r.Term,
		LeaderCommitIndex: r.CommitIndex,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		CurrentLeader:     r.CurrentLeader,
	}
	currentTerm := r.Term
	r.mu.Unlock()

	payload, _ := json.Marshal(req)
	ch, numPeers := r.Cluster.BroadcastWithChannel("raft.heartbeat", nil, payload)
	for i := 0; i < numPeers; i++ {
		data := <-ch
		if data.Error != "" {
			continue
		}
		var reply AppendEntriesRes
		if err := json.Unmarshal(data.Payload, &reply); err != nil {
			continue
		}

		r.mu.Lock()
		if !r.processAppendResponse(reply, currentTerm) {
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()
	}
	return nil
}

func (r *Raft) CommitLogEntries() {
	peers := r.Cluster.Peers()
	majority := r.Cluster.QuorumSize()
	for r.CommitIndex < len(r.Logs) {
		ack := 1
		for _, node := range peers {
			if r.AckLength[node.ID] > r.CommitIndex {
				ack += 1
			}
		}
		if ack >= majority {
			r.CommitIndex += 1
			if r.FSM != nil {
				r.FSM.Apply(r.Logs[r.CommitIndex-1].Command)
			}
		} else {
			break
		}
	}
}

func (r *Raft) Propose(command []byte) error {
	r.mu.Lock()
	if r.Role != Leader {
		leader := r.CurrentLeader
		r.mu.Unlock()
		if leader != "" {
			return fmt.Errorf("not leader; leader is %s", leader)
		}
		return errors.New("not leader; no leader currently elected")
	}

	_, prevLogIndex := r.GetPrevLog()
	entryIndex := prevLogIndex + 1
	log := Log{
		Term:    r.Term,
		Command: command,
		Index:   entryIndex,
	}
	r.Logs = append(r.Logs, log)
	r.AckLength[r.Cluster.Self.ID] = len(r.Logs)
	currentTerm := r.Term

	peers := r.Cluster.Peers()
	numPeers := len(peers)

	if numPeers == 0 {
		r.CommitLogEntries()
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	ctxTimeout, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	ch := make(chan *pb.Message, numPeers)
	for _, peer := range peers {
		go func(node *cluster.Node) {
			ch <- r.ReplicateLog(node.ID)
		}(peer)
	}

	for i := 0; i < numPeers; i++ {
		select {
		case resMsg := <-ch:
			if resMsg == nil || resMsg.Error != "" {
				continue
			}
			var res AppendEntriesRes
			if err := json.Unmarshal(resMsg.Payload, &res); err != nil {
				continue
			}

			r.mu.Lock()
			if !r.processAppendResponse(res, currentTerm) {
				r.mu.Unlock()
				return errors.New("leadership lost during replication")
			}
			if r.CommitIndex >= entryIndex {
				r.mu.Unlock()
				return nil
			}
			r.mu.Unlock()

		case <-ctxTimeout.Done():
			r.mu.Lock()
			committed := r.CommitIndex >= entryIndex
			r.mu.Unlock()
			if committed {
				return nil
			}
			return errors.New("failed to reach quorum consensus: timeout")
		}
	}

	r.mu.Lock()
	committed := r.CommitIndex >= entryIndex
	r.mu.Unlock()

	if committed {
		return nil
	}
	return errors.New("failed to reach quorum consensus")
}
