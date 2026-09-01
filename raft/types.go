package raft

type Role string

const (
	Leader    Role = "leader"
	Follower  Role = "follower"
	Candidate Role = "candidate"
)

type Log struct {
	Index   int    `json:"index"`
	Term    int    `json:"term"`
	Command []byte `json:"command"`
}

type VoteRequest struct {
	Term         int    `json:"term"`
	LastLogTerm  int    `json:"last_log_term"`
	LastLogIndex int    `json:"last_log_index"`
	CandidateId  string `json:"candidate_id"`
}

type VoteResponse struct {
	Term    int  `json:"term"`
	Granted bool `json:"granted"`
}

type AppendEntriesReq struct {
	Term              int    `json:"term"`
	LeaderID          string `json:"leader_id"`
	Logs              Log    `json:"logs"`
	PrevLogIndex      int    `json:"prev_log_index"`
	PrevLogTerm       int    `json:"prev_log_term"`
	LeaderCommitIndex int    `json:"leader_commit_index"`
}

type AppendEntriesRes struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}
