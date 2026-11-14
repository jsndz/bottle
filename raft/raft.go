package raft

import (
	"sync"
	"time"

	"github.com/jsndz/bottle/cluster"
)

type Raft struct {
	mu sync.Mutex

	Term        int
	VotedFor    string
	Logs        []Log
	CommitIndex int

	Role          Role
	CurrentLeader string
	VoteReceived  []string
	SentLength    map[string]int
	AckLength     map[string]int

	Cluster *cluster.Cluster
	Ticker  *time.Ticker
	Timeout time.Duration
	FSM     FSM
}

func NewRaft(cluster *cluster.Cluster) *Raft {
	timeout := RandomElectionTimeout()
	return &Raft{
		Cluster:    cluster,
		Role:       Follower,
		Timeout:    timeout,
		Ticker:     time.NewTicker(timeout),
		Logs:       make([]Log, 0),
		Term:       0,
		SentLength: make(map[string]int),
		AckLength:  make(map[string]int),
	}
}

func (r *Raft) HeartbeatTicker() {
	go func() {
		for range r.Ticker.C {
			r.mu.Lock()
			role := r.Role
			r.mu.Unlock()

			if role == Leader {
				r.Heartbeat()
			} else {
				r.StartElection()
			}
		}
	}()
}

func (r *Raft) convertToFollower(term int, leaderID string) {
	r.Role = Follower
	r.Term = term
	r.VotedFor = ""
	r.VoteReceived = nil
	r.CurrentLeader = leaderID
	if r.Ticker != nil {
		r.Ticker.Reset(r.Timeout)
	}
}

func (r *Raft) becomeLeader() {
	r.Role = Leader
	r.CurrentLeader = r.Cluster.Self.ID
	peers := r.Cluster.Peers()
	for _, p := range peers {
		r.SentLength[p.ID] = len(r.Logs)
		r.AckLength[p.ID] = 0
		go r.ReplicateLog(p.ID)
	}
}

func (r *Raft) GetPrevLog() (int, int) {
	if len(r.Logs) > 0 {
		prevLog := r.Logs[len(r.Logs)-1]
		return prevLog.Term, prevLog.Index
	}
	return 0, 0
}
