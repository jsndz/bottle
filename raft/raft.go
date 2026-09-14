package raft

import (
	"context"
	"encoding/json"

	"sync"
	"time"

	"github.com/jsndz/bottle/cluster"
	"github.com/jsndz/bottle/rpc"
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
	Ticker  time.Ticker
	Timeout time.Duration
	FSM     FSM
}

func NewRaft(cluster *cluster.Cluster) *Raft {
	timeout := RandomElectionTimeout()
	return &Raft{
		Cluster:    cluster,
		Role:       Follower,
		Timeout:    timeout,
		Ticker:     *time.NewTicker(timeout),
		Logs:       make([]Log, 0),
		Term:       0,
		SentLength: make(map[string]int),
		AckLength:  make(map[string]int),
	}
}

func (r *Raft) AppendLog(log Log) {
	r.Logs = append(r.Logs, log)
}

func (r *Raft) HeartbeatTicker() {
	go func() {
		for range r.Ticker.C {
			if r.Role == Leader {
				r.Heartbeat()
			} else {
				r.StartElection()
			}

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
	r.VotedFor = r.Cluster.Self.ID
	r.VoteReceived = []string{r.VotedFor}
	currentTerm := r.Term

	peers := make([]*cluster.Node, 0)
	for _, node := range r.Cluster.Nodes {
		if node.ID != r.Cluster.Self.ID {
			peers = append(peers, node)
		}
	}

	totalNodes := len(peers) + 1
	majority := (totalNodes / 2) + 1

	// Single-node cluster case
	if len(r.VoteReceived) >= majority {
		r.Role = Leader
		r.CurrentLeader = r.Cluster.Self.ID
		r.mu.Unlock()
		for _, peer := range peers {
			r.SentLength[peer.ID] = len(r.Logs)
			r.AckLength[peer.ID] = 0
			go ReplicateLog(r.Cluster.Self.ID, peer.ID)
		}
		return nil
	}

	lastLogTerm := 0
	if len(r.Logs) > 0 {
		lastLogTerm = r.Logs[len(r.Logs)-1].Term
	}

	req := VoteRequest{
		Term:         currentTerm,
		LastLogIndex: len(r.Logs),
		LastLogTerm:  lastLogTerm,
		CandidateId:  r.Cluster.Self.ID,
	}

	payload, err := json.Marshal(req)
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()

	for _, peer := range peers {
		go func(node *cluster.Node) {
			client := rpc.NewClient(node.Address, 1, r.Cluster.Pool)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			reply, err := client.Call(ctx, "raft.election", payload, nil)
			if err != nil || reply.Error != "" {
				return
			}

			var res VoteResponse
			if err := json.Unmarshal(reply.Payload, &res); err != nil {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			// Ignore if state changed or term moved forward
			if r.Role != Candidate || r.Term != currentTerm {
				return
			}

			// Step down if peer has higher term
			if res.Term > r.Term {
				r.Term = res.Term
				r.VoteReceived = nil
				r.Role = Follower
				r.VotedFor = ""
				r.Ticker.Reset(r.Timeout)
				return
			}

			if res.Granted {
				r.VoteReceived = append(r.VoteReceived, res.VoterId)
				// Check majority
				if len(r.VoteReceived) >= majority {
					r.Role = Leader
					r.CurrentLeader = r.Cluster.Self.ID

					// Trigger log replication immediately upon becoming leader
					for _, p := range peers {
						r.SentLength[p.ID] = len(r.Logs)
						r.AckLength[p.ID] = 0
						go ReplicateLog(r.Cluster.Self.ID, p.ID)
					}
				}
			}
		}(peer)
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
		CurrentLeader:     r.CurrentLeader,
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
		}

		if !reply.Success {
			// handling log mismatch
			//FIND THE peer who has log mismatch
			req.PrevLogIndex--
			// r.Cluster.SendToNode(reply.FollowerID, "raft.heartbeat")
			// send him th req again with the index--
		}
	}
	return nil
}

func ReplicateLog(nodeId, followerId string) {}
