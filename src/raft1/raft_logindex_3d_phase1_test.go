package raft

import "testing"

// Unit tests for logical log indexing (Lab 3D Phase 1). No network / Persister.

func TestLogIndexHelpersPhase1NoSnapshot(t *testing.T) {
	rf := &Raft{}
	rf.snapLastIdx = 0
	rf.snapLastTerm = 0
	rf.log = []LogEntry{{Term: 0}, {Term: 1, Command: 10}, {Term: 2, Command: 20}}

	if g := rf.firstLogIndex(); g != 1 {
		t.Fatalf("firstLogIndex: got %d want 1", g)
	}
	if g := rf.lastLogIndex(); g != 2 {
		t.Fatalf("lastLogIndex: got %d want 2", g)
	}
	if g := rf.logTerm(0); g != 0 {
		t.Fatalf("logTerm(0): got %d want 0", g)
	}
	if g := rf.logTerm(1); g != 1 {
		t.Fatalf("logTerm(1): got %d want 1", g)
	}
	if g := rf.logTerm(2); g != 2 {
		t.Fatalf("logTerm(2): got %d want 2", g)
	}
	if g := rf.lastLogTerm(); g != 2 {
		t.Fatalf("lastLogTerm: got %d want 2", g)
	}
}

func TestLogIndexHelpersPhase1WithSnapshotMetadata(t *testing.T) {
	rf := &Raft{}
	rf.snapLastIdx = 100
	rf.snapLastTerm = 4
	rf.log = []LogEntry{
		{Term: 0},
		{Term: 4, Command: "a"},
		{Term: 4, Command: "b"},
	}

	if g := rf.firstLogIndex(); g != 101 {
		t.Fatalf("firstLogIndex: got %d want 101", g)
	}
	if g := rf.lastLogIndex(); g != 102 {
		t.Fatalf("lastLogIndex: got %d want 102", g)
	}
	if g := rf.logTerm(100); g != 4 {
		t.Fatalf("logTerm(100): got %d want 4", g)
	}
	if g := rf.logTerm(101); g != 4 {
		t.Fatalf("logTerm(101): got %d want 4", g)
	}
	if g := rf.logTerm(102); g != 4 {
		t.Fatalf("logTerm(102): got %d want 4", g)
	}
}

func TestLogIndexHelpersPhase1DummyOnly(t *testing.T) {
	rf := &Raft{}
	rf.snapLastIdx = 0
	rf.snapLastTerm = 0
	rf.log = []LogEntry{{Term: 0}}

	if rf.lastLogIndex() != 0 {
		t.Fatalf("lastLogIndex dummy-only: got %d want 0", rf.lastLogIndex())
	}
	if rf.lastLogTerm() != 0 {
		t.Fatalf("lastLogTerm dummy-only: got %d want 0", rf.lastLogTerm())
	}
}
