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

type AppendEntriesReq struct {
	Term              int
	LeaderID          string
	Logs              Log
	PrevLogIndex      int
	PrevLogTerm       int
	LeaderCommitIndex int
}

type AppendEntriesRes struct {
	Term    int
	Success bool
}
