package raft

type VoteRequest struct {
	Term     int
	LogIndex int
}
