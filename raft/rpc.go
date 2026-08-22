package raft

type VoteRequest struct {
	Term        int
	LogIndex    int
	CandidateId string
}

type VoteResponse struct {
	Term    int
	Granted bool
}
