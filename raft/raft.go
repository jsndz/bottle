package raft

import (
	"encoding/json"

	"sync"
	"time"

	"github.com/jsndz/bottle/cluster"
)

type Role string

const (
	Leader    Role = "leader"
	Follower  Role = "follower"
	Candidate Role = "candidate"
)

type Raft struct {
	mu      sync.Mutex
	Role    Role
	Cluster *cluster.Cluster
	Timeout time.Duration
	Term    int
	Logs    []Log
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
	defer r.mu.Unlock()
	r.Term++
	r.Role = Candidate
	req := VoteRequest{
		Term:     r.Term,
		LogIndex: len(r.Logs),
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	//broadcast to all nodes
	r.mu.Lock()
	peers := make([]*cluster.Node, 0, len(r.Cluster.Nodes))
	for _, node := range r.Cluster.Nodes {
		if node.ID != r.Cluster.Self.ID {
			peers = append(peers, node)
		}
	}
	r.mu.Unlock()

	ch, numPeers := r.Cluster.BroadcastWithChannel("raft.election", nil, payload)
	votes := 0
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
