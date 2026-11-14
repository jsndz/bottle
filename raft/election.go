package raft

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jsndz/bottle/cluster"
	"github.com/jsndz/bottle/rpc"
)

func (r *Raft) StartElection() error {
	r.mu.Lock()
	r.Term++
	r.Role = Candidate
	r.VotedFor = r.Cluster.Self.ID
	r.VoteReceived = []string{r.VotedFor}
	currentTerm := r.Term
	if r.Ticker != nil {
		r.Ticker.Reset(RandomElectionTimeout())
	}

	peers := r.Cluster.Peers()
	majority := r.Cluster.QuorumSize()

	if len(r.VoteReceived) >= majority {
		r.becomeLeader()
		r.mu.Unlock()
		return nil
	}

	lastLogTerm, lastLogIndex := r.GetPrevLog()
	req := VoteRequest{
		Term:         currentTerm,
		LastLogIndex: lastLogIndex,
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
			client := rpc.NewClient(node.Address, r.Cluster.Pool)
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

			if r.Role != Candidate || r.Term != currentTerm {
				return
			}

			if res.Term > r.Term {
				r.convertToFollower(res.Term, "")
				return
			}

			if res.Granted {
				r.VoteReceived = append(r.VoteReceived, res.VoterId)
				if len(r.VoteReceived) >= majority {
					r.becomeLeader()
				}
			}
		}(peer)
	}

	return nil
}
