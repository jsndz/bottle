package raft

import "github.com/jsndz/bottle/kv"

type FSM interface {
	Apply(cmd *kv.Command)
}
