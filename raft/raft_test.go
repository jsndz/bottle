package raft

import (
	"testing"

	"github.com/jsndz/bottle/cluster"
)

func TestRaftInitAndFollowerStepDown(t *testing.T) {
	self := cluster.NewNode("127.0.0.1:9001")
	cls := cluster.NewCluster(self, nil, "cluster-raft")
	r := NewRaft(cls)

	if r.Role != Follower {
		t.Fatalf("expected initial role Follower, got %v", r.Role)
	}

	r.Role = Candidate
	r.Term = 1
	r.VotedFor = self.ID

	r.convertToFollower(2, "leader-2")

	if r.Role != Follower {
		t.Fatalf("expected role Follower, got %v", r.Role)
	}
	if r.Term != 2 {
		t.Fatalf("expected term 2, got %d", r.Term)
	}
	if r.CurrentLeader != "leader-2" {
		t.Fatalf("expected leader-2, got %s", r.CurrentLeader)
	}
}

func TestRaftAppendLog(t *testing.T) {
	self := cluster.NewNode("127.0.0.1:9001")
	cls := cluster.NewCluster(self, nil, "cluster-raft")
	r := NewRaft(cls)

	r.AppendLog([]Log{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
	}, 0, 1)

	if len(r.Logs) != 2 {
		t.Fatalf("expected 2 logs, got %d", len(r.Logs))
	}
	if r.CommitIndex != 1 {
		t.Fatalf("expected commit index 1, got %d", r.CommitIndex)
	}

	term, index := r.GetPrevLog()
	if term != 1 || index != 2 {
		t.Fatalf("expected term 1, index 2, got term %d, index %d", term, index)
	}
}

type mockFSM struct {
	applied [][]byte
}

func (m *mockFSM) Apply(cmd []byte) any {
	m.applied = append(m.applied, cmd)
	return string(cmd) + "_applied"
}

func TestRaftPropose(t *testing.T) {
	self := cluster.NewNode("127.0.0.1:9001")
	cls := cluster.NewCluster(self, nil, "cluster-raft")
	r := NewRaft(cls)
	fsm := &mockFSM{}
	r.FSM = fsm

	err := r.Propose([]byte("test-cmd"))
	if err == nil {
		t.Fatalf("expected error proposing on follower")
	}

	r.Role = Leader
	r.CurrentLeader = self.ID

	err = r.Propose([]byte("cmd-leader"))
	if err != nil {
		t.Fatalf("unexpected error proposing on leader: %v", err)
	}

	if r.CommitIndex != 1 {
		t.Fatalf("expected commit index 1, got %d", r.CommitIndex)
	}
	if len(fsm.applied) != 1 || string(fsm.applied[0]) != "cmd-leader" {
		t.Fatalf("expected fsm to apply command, got %v", fsm.applied)
	}
}
