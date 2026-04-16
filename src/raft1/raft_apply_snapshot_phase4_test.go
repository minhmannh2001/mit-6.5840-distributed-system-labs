package raft

import (
	"bytes"
	"testing"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// encodeTestSnapshotBytes matches server.go applierSnap snapshot encoding (lastIncludedIndex + xlog).
func encodeTestSnapshotBytes(t *testing.T, lastIncluded int, xlog []any) []byte {
	t.Helper()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(lastIncluded); err != nil {
		t.Fatal(err)
	}
	if err := e.Encode(xlog); err != nil {
		t.Fatal(err)
	}
	return w.Bytes()
}

func encodeRaftStateForTest(t *testing.T, term, voted int, log []LogEntry, snapIdx, snapTerm int) []byte {
	t.Helper()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(term); err != nil {
		t.Fatal(err)
	}
	if err := e.Encode(voted); err != nil {
		t.Fatal(err)
	}
	if err := e.Encode(log); err != nil {
		t.Fatal(err)
	}
	if err := e.Encode(snapIdx); err != nil {
		t.Fatal(err)
	}
	if err := e.Encode(snapTerm); err != nil {
		t.Fatal(err)
	}
	return w.Bytes()
}

// Phase 4: restart with snapshot+trimmed log must emit SnapshotValid before commands (contract).
func TestRestartEmitsSnapshotApplyMsgPhase4(t *testing.T) {
	p := tester.MakePersister()
	snapIdx := 4
	snapTerm := 2
	// Tail log: logical indices 5 and 6 only (dummy at slice 0).
	log := []LogEntry{
		{Term: 0},
		{Term: 2, Command: 105},
		{Term: 2, Command: 106},
	}
	xlog := []any{nil, 101, 102, 103, 104}
	snapBytes := encodeTestSnapshotBytes(t, snapIdx, xlog)
	raftState := encodeRaftStateForTest(t, 2, noVote, log, snapIdx, snapTerm)
	p.Save(raftState, snapBytes)

	ch := make(chan raftapi.ApplyMsg, 8)
	r := Make([]*labrpc.ClientEnd{nil}, 0, p, ch)
	defer r.(*Raft).Kill()

	select {
	case m := <-ch:
		if !m.SnapshotValid {
			t.Fatalf("first ApplyMsg should be snapshot; got %+v", m)
		}
		if m.SnapshotIndex != snapIdx || m.SnapshotTerm != snapTerm {
			t.Fatalf("SnapshotIndex/Term = (%d,%d), want (%d,%d)", m.SnapshotIndex, m.SnapshotTerm, snapIdx, snapTerm)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for snapshot ApplyMsg")
	}
}

// Phase 4: Snapshot(index) must not lower lastApplied; trimmed indices must not reappear in log.
func TestSnapshotTrimDoesNotRewindLastAppliedPhase4(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    noVote,
		log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: 1},
			{Term: 2, Command: 2},
			{Term: 2, Command: 3},
			{Term: 2, Command: 4},
			{Term: 2, Command: 5},
			{Term: 2, Command: 6},
		},
		commitIndex: 6,
		lastApplied: 6,
	}
	snap := encodeTestSnapshotBytes(t, 3, []any{nil, 1, 2, 3})
	rf.Snapshot(3, snap)

	rf.mu.Lock()
	if rf.lastApplied != 6 {
		t.Fatalf("lastApplied = %d, want 6 (unchanged)", rf.lastApplied)
	}
	if rf.firstLogIndex() != 4 || rf.lastLogIndex() != 6 {
		t.Fatalf("firstLog=%d lastLog=%d want 4,6", rf.firstLogIndex(), rf.lastLogIndex())
	}
	if len(rf.log) != 4 { // dummy + 3 entries at logical 4,5,6
		t.Fatalf("len(log)=%d want 4", len(rf.log))
	}
	rf.mu.Unlock()
}
