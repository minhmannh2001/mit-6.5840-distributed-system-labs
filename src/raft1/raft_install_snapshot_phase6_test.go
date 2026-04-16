package raft

import (
	"testing"
	"time"

	tester "6.5840/tester1"
)

// Phase 6 — follower InstallSnapshot: stale term rejects without adopting sender term (Figure 2).
func TestInstallSnapshotStaleTermPhase6(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 10,
		votedFor:    0,
		role:        RoleFollower,
		log: []LogEntry{
			{Term: 0},
			{Term: 5, Command: 1},
		},
		snapLastIdx:  0,
		snapLastTerm: 0,
		commitIndex:  1,
		lastApplied:  1,
	}
	args := &InstallSnapshotArgs{
		Term:              3,
		LeaderId:          2,
		LastIncludedIndex: 50,
		LastIncludedTerm:  3,
		Data:              []byte{1, 2, 3},
		Done:              true,
	}
	reply := &InstallSnapshotReply{}
	rf.InstallSnapshot(args, reply)

	if reply.Term != 10 {
		t.Fatalf("reply.Term=%d want 10 (reject stale)", reply.Term)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != 10 {
		t.Fatalf("currentTerm=%d want 10", rf.currentTerm)
	}
	if rf.snapLastIdx != 0 || len(rf.snapshot) != 0 {
		t.Fatalf("snapshot state should be unchanged on stale RPC")
	}
}

// Phase 6 — higher term in InstallSnapshot steps down (Figure 2) and still applies snapshot when newer.
func TestInstallSnapshotNewerTermPhase6(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    0,
		role:        RoleCandidate,
		log: []LogEntry{
			{Term: 0},
			{Term: 2, Command: 1},
		},
		snapLastIdx:  0,
		snapLastTerm: 0,
		commitIndex:  1,
		lastApplied:  1,
	}
	data := []byte{9, 9, 9}
	args := &InstallSnapshotArgs{
		Term:              5,
		LeaderId:          1,
		LastIncludedIndex: 1,
		LastIncludedTerm:  2,
		Data:              data,
		Done:              true,
	}
	reply := &InstallSnapshotReply{}
	rf.InstallSnapshot(args, reply)

	if reply.Term != 5 {
		t.Fatalf("reply.Term=%d want 5", reply.Term)
	}
	rf.mu.Lock()
	if rf.currentTerm != 5 || rf.role != RoleFollower {
		t.Fatalf("want term=5 follower, got term=%d role=%v", rf.currentTerm, rf.role)
	}
	if rf.snapLastIdx != 1 || rf.snapLastTerm != 2 {
		t.Fatalf("snap metadata (%d,%d) want (1,2)", rf.snapLastIdx, rf.snapLastTerm)
	}
	if string(rf.snapshot) != string(data) {
		t.Fatalf("snapshot bytes mismatch")
	}
	rf.mu.Unlock()
}

// Phase 6 — Figure 13: matching (index, term) at lastIncluded keeps log suffix after that index.
func TestInstallSnapshotKeepSuffixPhase6(t *testing.T) {
	p := tester.MakePersister()
	// snapLastIdx=0: logical 1..4 in log[1..4], all term 2.
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    noVote,
		role:        RoleFollower,
		log: []LogEntry{
			{Term: 0},
			{Term: 2, Command: 101},
			{Term: 2, Command: 102},
			{Term: 2, Command: 103},
			{Term: 2, Command: 104},
		},
		snapLastIdx:  0,
		snapLastTerm: 0,
		commitIndex:  4,
		lastApplied:  4,
	}
	args := &InstallSnapshotArgs{
		Term:              2,
		LeaderId:          1,
		LastIncludedIndex: 2,
		LastIncludedTerm:  2,
		Data:              []byte("snap-at-2"),
		Done:              true,
	}
	reply := &InstallSnapshotReply{}
	rf.InstallSnapshot(args, reply)

	if reply.Term != 2 {
		t.Fatalf("reply.Term=%d", reply.Term)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.snapLastIdx != 2 || rf.snapLastTerm != 2 {
		t.Fatalf("snapLast=(%d,%d) want (2,2)", rf.snapLastIdx, rf.snapLastTerm)
	}
	if rf.lastLogIndex() != 4 {
		t.Fatalf("lastLogIndex=%d want 4 (suffix 3,4 kept)", rf.lastLogIndex())
	}
	if rf.logTerm(3) != 2 || rf.logTerm(4) != 2 {
		t.Fatalf("suffix terms wrong")
	}
	if rf.log[1].Command != 103 || rf.log[2].Command != 104 {
		t.Fatalf("suffix commands wrong: %+v, %+v", rf.log[1], rf.log[2])
	}
}

// Phase 6 — no matching entry at lastIncluded: discard log, only dummy + new snapshot metadata.
func TestInstallSnapshotDiscardLogPhase6(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 3,
		votedFor:    noVote,
		role:        RoleFollower,
		log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: 1},
		},
		snapLastIdx:  0,
		snapLastTerm: 0,
		commitIndex:  1,
		lastApplied:  1,
	}
	args := &InstallSnapshotArgs{
		Term:              3,
		LeaderId:          1,
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              []byte("full-replace"),
		Done:              true,
	}
	reply := &InstallSnapshotReply{}
	rf.InstallSnapshot(args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) != 1 {
		t.Fatalf("len(log)=%d want 1 (dummy only)", len(rf.log))
	}
	if rf.snapLastIdx != 10 || rf.snapLastTerm != 2 {
		t.Fatalf("snapLast=(%d,%d) want (10,2)", rf.snapLastIdx, rf.snapLastTerm)
	}
	if rf.commitIndex != 10 || rf.lastApplied != 10 {
		t.Fatalf("commit/lastApplied should jump to snapshot boundary")
	}
	if !rf.applySnapshotPending {
		t.Fatalf("applySnapshotPending should be true for service")
	}
	_ = reply
}

// Phase 6 — follower already past snapshot: idempotent no-op for log (bytes refresh path is same index).
func TestInstallSnapshotFollowerAheadPhase6(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    noVote,
		role:        RoleFollower,
		log: []LogEntry{
			{Term: 0},
			{Term: 2, Command: 200},
		},
		snapLastIdx:  5,
		snapLastTerm: 2,
		commitIndex:  6,
		lastApplied:  6,
	}
	prevLogLen := len(rf.log)
	args := &InstallSnapshotArgs{
		Term:              2,
		LeaderId:          1,
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
		Data:              []byte("old"),
		Done:              true,
	}
	reply := &InstallSnapshotReply{}
	rf.InstallSnapshot(args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.snapLastIdx != 5 {
		t.Fatalf("snapLastIdx should stay 5, got %d", rf.snapLastIdx)
	}
	if len(rf.log) != prevLogLen {
		t.Fatalf("log should not shrink when leader snapshot is older")
	}
	if reply.Term != 2 {
		t.Fatalf("reply.Term=%d", reply.Term)
	}
}

// Phase 6 — election timer reset on valid RPC (regression: follower should not timeout immediately after install).
func TestInstallSnapshotResetsElectionDeadlinePhase6(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    noVote,
		role:        RoleFollower,
		log:         []LogEntry{{Term: 0}, {Term: 2, Command: 1}},
		snapLastIdx: 0, snapLastTerm: 0,
		commitIndex: 1, lastApplied: 1,
	}
	rf.resetElectionTimerLocked()
	deadlinePast := rf.electionDeadline
	time.Sleep(5 * time.Millisecond)

	args := &InstallSnapshotArgs{
		Term: 2, LeaderId: 1,
		LastIncludedIndex: 1, LastIncludedTerm: 2,
		Data: []byte("x"), Done: true,
	}
	reply := &InstallSnapshotReply{}
	rf.InstallSnapshot(args, reply)
	_ = reply

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.electionDeadline.After(deadlinePast) {
		t.Fatalf("election deadline should move forward after InstallSnapshot")
	}
}
