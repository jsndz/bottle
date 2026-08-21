package raft

type Log struct {
	Term    int
	Index   int
	Command string
}
