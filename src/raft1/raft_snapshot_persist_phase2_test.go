package raft

import (
	"reflect"
	"testing"

	tester "6.5840/tester1"
)

// Phase 2: persist() must not accidentally drop an existing snapshot blob.
func TestPersistKeepsExistingSnapshotPhase2(t *testing.T) {
	p := tester.MakePersister()
	initialSnapshot := []byte{1, 2, 3, 4}
	p.Save([]byte{9}, initialSnapshot)

	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    1,
		log:         []LogEntry{{Term: 0}, {Term: 2, Command: "x"}},
	}
	rf.persist()

	got := p.ReadSnapshot()
	if !reflect.DeepEqual(got, initialSnapshot) {
		t.Fatalf("persist should preserve existing snapshot: got=%v want=%v", got, initialSnapshot)
	}
}

// Phase 2: snapshot metadata should round-trip through raftstate blob.
func TestPersistReadPersistSnapshotMetadataPhase2(t *testing.T) {
	p := tester.MakePersister()
	src := &Raft{
		persister:    p,
		currentTerm:  7,
		votedFor:     3,
		log:          []LogEntry{{Term: 0}, {Term: 5, Command: 11}},
		snapLastIdx:  0,
		snapLastTerm: 0,
	}
	src.persist()

	dst := &Raft{
		currentTerm: 0,
		votedFor:    noVote,
		log:         []LogEntry{{Term: 0}},
	}
	dst.readPersist(p.ReadRaftState())

	if dst.snapLastIdx != src.snapLastIdx || dst.snapLastTerm != src.snapLastTerm {
		t.Fatalf("snapshot metadata mismatch: got (%d,%d) want (%d,%d)",
			dst.snapLastIdx, dst.snapLastTerm, src.snapLastIdx, src.snapLastTerm)
	}
}
