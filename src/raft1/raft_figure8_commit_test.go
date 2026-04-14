package raft

import (
	"testing"

	"6.5840/labrpc"
)

// Part 5 / 3C TDD: Figure 8 — never advance commitIndex on entries from a past term (§8 / extended paper).
// Persistence does not change this rule; these tests lock advanceCommitIndexLocked behavior.

func TestAdvanceCommitIndexFigure8SkipsOldTermEvenWithMajority3CPart5(t *testing.T) {
	nPeers := 3
	log := []LogEntry{{Term: 0}}
	for i := 0; i < 5; i++ {
		log = append(log, LogEntry{Term: 2, Command: i})
	}
	// All real entries are term 2; leader is now in term 5 but has not appended a term-5 entry yet.
	rf := &Raft{
		role:        RoleLeader,
		me:          0,
		currentTerm: 5,
		commitIndex: 0,
		peers:       make([]*labrpc.ClientEnd, nPeers),
		log:         log,
		matchIndex:  []int{5, 5, 5},
		nextIndex:   []int{6, 6, 6},
	}
	rf.mu.Lock()
	rf.advanceCommitIndexLocked()
	got := rf.commitIndex
	rf.mu.Unlock()
	if got != 0 {
		t.Fatalf("Figure 8: must not commit old-term tail; commitIndex=%d want 0", got)
	}
}

func TestAdvanceCommitIndexCommitsCurrentTermWhenMajority3CPart5(t *testing.T) {
	nPeers := 3
	log := []LogEntry{{Term: 0}}
	for i := 0; i < 4; i++ {
		log = append(log, LogEntry{Term: 2, Command: i})
	}
	log = append(log, LogEntry{Term: 5, Command: "new-leader"}) // index 5, current term
	rf := &Raft{
		role:        RoleLeader,
		me:          0,
		currentTerm: 5,
		commitIndex: 0,
		peers:       make([]*labrpc.ClientEnd, nPeers),
		log:         log,
		matchIndex:  []int{5, 5, 5},
		nextIndex:   []int{6, 6, 6},
	}
	rf.mu.Lock()
	rf.advanceCommitIndexLocked()
	got := rf.commitIndex
	rf.mu.Unlock()
	if got != 5 {
		t.Fatalf("want commitIndex=5 (first entry of leader's term replicated by majority); got %d", got)
	}
}

func TestAdvanceCommitIndexJumpsPastOldTermGap3CPart5(t *testing.T) {
	// commitIndex stuck at 2; higher indices are old term; only index 6 is current term and replicated.
	nPeers := 5
	log := []LogEntry{
		{Term: 0},
		{Term: 1, Command: "a"},
		{Term: 1, Command: "b"},
		{Term: 2, Command: "c"},
		{Term: 2, Command: "d"},
		{Term: 2, Command: "e"},
		{Term: 7, Command: "fig8"},
	}
	rf := &Raft{
		role:        RoleLeader,
		me:          0,
		currentTerm: 7,
		commitIndex: 2,
		peers:       make([]*labrpc.ClientEnd, nPeers),
		log:         log,
		matchIndex:  []int{6, 6, 6, 6, 6},
		nextIndex:   []int{7, 7, 7, 7, 7},
	}
	rf.mu.Lock()
	rf.advanceCommitIndexLocked()
	got := rf.commitIndex
	rf.mu.Unlock()
	if got != 6 {
		t.Fatalf("should commit index 6 only (term 7), not old-term 3–5; commitIndex=%d want 6", got)
	}
}

func TestAdvanceCommitIndexNonLeaderNoOp3CPart5(t *testing.T) {
	rf := &Raft{
		role:        RoleFollower,
		currentTerm: 3,
		commitIndex: 0,
		log:         []LogEntry{{Term: 0}, {Term: 3, Command: 1}},
		peers:       make([]*labrpc.ClientEnd, 3),
		matchIndex:  []int{1, 1, 1},
	}
	rf.mu.Lock()
	rf.advanceCommitIndexLocked()
	got := rf.commitIndex
	rf.mu.Unlock()
	if got != 0 {
		t.Fatalf("follower must not advance commitIndex here; got %d", got)
	}
}
