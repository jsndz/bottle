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
	Timeout     time.Duration
	Term        int
	Logs        []Log
	LeaderID    string
	CommitIndex int
	FSM         FSM
}

func NewRaft(cluster *cluster.Cluster) *Raft {
	return &Raft{
		Cluster: cluster,
		Role:    Follower,
		Timeout: RandomElectionTimeout(),
		Logs:    make([]Log, 0),
		Term:    0,
	}
}

func (r *Raft) AppendLog(log Log) {
	r.Logs = append(r.Logs, log)
}

func (r *Raft) StartElection() error {
	r.mu.Lock()
	r.Term++
	r.Role = Candidate
	r.mu.Unlock()
	req := VoteRequest{
		Term:        r.Term,
		LogIndex:    len(r.Logs),
		CandidateId: r.Cluster.Self.ID,
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
				break
			}
		}

	}
	return nil
}
