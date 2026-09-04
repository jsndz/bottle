package raft

import (
	"encoding/json"

	"sync"
	"time"

	"github.com/jsndz/bottle/cluster"
)

type Raft struct {
	mu          sync.Mutex
	Role        Role
	Cluster     *cluster.Cluster
	Ticker      time.Ticker
	Timeout     time.Duration
	Term        int
	Logs        []Log
	LeaderID    string
	CommitIndex int
	FSM         FSM
}

func NewRaft(cluster *cluster.Cluster) *Raft {
	timeout := RandomElectionTimeout()
	return &Raft{
		Cluster: cluster,
		Role:    Follower,
		Timeout: timeout,
		Ticker:  *time.NewTicker(timeout),
		Logs:    make([]Log, 0),
		Term:    0,
	}
}

func (r *Raft) AppendLog(log Log) {
	r.Logs = append(r.Logs, log)
}

func (r *Raft) HeartbeatTicker() {
	go func() {
		for range r.Ticker.C {
			r.StartElection()
		}
	}()

}

func (r *Raft) GetPrevLog() (int, int) {
	if len(r.Logs) > 0 {
		prevLog := r.Logs[len(r.Logs)-1]
		return prevLog.Term, prevLog.Index
	}

	return 0, 0
}

func (r *Raft) StartElection() error {
	r.mu.Lock()
	r.Term++
	r.Role = Candidate
	r.mu.Unlock()
	req := VoteRequest{
		Term:         r.Term,
		LastLogIndex: len(r.Logs),
		LastLogTerm:  r.Logs[len(r.Logs)-1].Term,
		CandidateId:  r.Cluster.Self.ID,
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	ch, numPeers := r.Cluster.BroadcastWithChannel("raft.election", nil, payload)
	votes := 1
	majority := ((numPeers + 1) / 2) + 1
	for i := 0; i < numPeers; i++ {

		data := <-ch

		if data.Error != "" {
			continue
		}
		var reply VoteResponse
		json.Unmarshal(data.Payload, &reply)
		if reply.Granted {
			votes++
			if votes >= majority {
				r.Role = Leader
				r.LeaderID = r.Cluster.Self.ID
				break
			}
		}

	}
	return nil
}

func (r *Raft) Heartbeat() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	prevLogTerm, prevLogIndex := r.GetPrevLog()
	req := &AppendEntriesReq{
		Term:              r.Term,
		LeaderCommitIndex: r.CommitIndex,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		LeaderID:          r.LeaderID,
	}
	payload, _ := json.Marshal(req)
	ch, numPeers := r.Cluster.BroadcastWithChannel("raft.heartbeat", nil, payload)
	for i := 0; i < numPeers; i++ {

		data := <-ch
		if data.Error != "" {
			continue
		}
		var reply AppendEntriesRes
		json.Unmarshal(data.Payload, &reply)
		if reply.Term > r.Term {
			r.Term = reply.Term
			r.Role = Follower
			r.Ticker.Reset(r.Timeout)
			return nil
		} else {
			// handling job mismatch
			//FIND THE peer who has log mismatch
			// send him th req again with the index--
		}
	}
	return nil
}
