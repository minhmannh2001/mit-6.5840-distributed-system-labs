package raft

import (
	"testing"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Phase 7: persist() must not wipe a persisted snapshot when rf.snapshot is temporarily nil
// (e.g. in-memory cleared) but snap metadata / persister still hold bytes — Save always gets
// a copy from ReadSnapshot() in that case.
func TestPersistNilInMemorySnapshotStillPreservesPersisterPhase7(t *testing.T) {
	p := tester.MakePersister()
	snapData := []byte{0xde, 0xad, 0xbe, 0xef}
	raftBlob := encodeRaftStateForTest(t, 2, noVote, []LogEntry{
		{Term: 0},
		{Term: 2, Command: 9},
		{Term: 2, Command: 10},
	}, 1, 1)
	p.Save(raftBlob, snapData)

	ch := make(chan raftapi.ApplyMsg, 4)
	r := Make([]*labrpc.ClientEnd{nil}, 0, p, ch)
	rf := r.(*Raft)
	defer rf.Kill()

	rf.mu.Lock()
	if len(rf.snapshot) == 0 {
		t.Fatal("Make should load snapshot from persister")
	}
	// Simulate rare path: in-memory pointer nil but disk still has snapshot (contract: persist recovers).
	rf.snapshot = nil
	rf.persist()
	rf.mu.Unlock()

	got := p.ReadSnapshot()
	if len(got) != len(snapData) {
		t.Fatalf("ReadSnapshot len=%d want %d", len(got), len(snapData))
	}
	for i := range snapData {
		if got[i] != snapData[i] {
			t.Fatalf("byte %d: got %x want %x", i, got[i], snapData[i])
		}
	}
}

// Phase 7: after Snapshot(), a subsequent persist (e.g. only term/vote change) must still pass non-nil snapshot to Save.
func TestPersistAfterSnapshotTrimKeepsBytesOnDiskPhase7(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    noVote,
		role:        RoleFollower,
		log: []LogEntry{
			{Term: 0},
			{Term: 2, Command: 1},
			{Term: 2, Command: 2},
			{Term: 2, Command: 3},
		},
		snapLastIdx:  0,
		snapLastTerm: 0,
		commitIndex:  3,
		lastApplied:  3,
	}
	snap := []byte("kv-snapshot-at-2")
	rf.Snapshot(2, snap)
	if p.SnapshotSize() == 0 {
		t.Fatal("expected snapshot bytes saved after Snapshot()")
	}
	// Vote change style persist (same pattern as RequestVote grant).
	rf.mu.Lock()
	rf.votedFor = 0
	rf.persist()
	rf.mu.Unlock()
	if p.SnapshotSize() == 0 {
		t.Fatal("persist after vote must not clear snapshot on persister")
	}
}
