package raft

import (
	"reflect"
	"testing"

	"6.5840/labgob"
	tester "6.5840/tester1"
)

func init() {
	// Same concrete command types as Make(); needed before labgob encode/decode in unit tests.
	labgob.Register(LogEntry{})
	labgob.Register(0)
	labgob.Register("")
}

// TestPersistReadPersistRoundTrip3C (3C Part 1 / TDD): encode then decode restores Figure-2 durable fields.
func TestPersistReadPersistRoundTrip3C(t *testing.T) {
	p := tester.MakePersister()
	src := &Raft{
		persister:   p,
		currentTerm: 7,
		votedFor:    2,
		log: []LogEntry{
			{Term: 0, Command: nil},
			{Term: 3, Command: 100},
			{Term: 7, Command: "hello"},
		},
	}
	src.persist()
	data := p.ReadRaftState()
	if len(data) < 1 {
		t.Fatalf("persist should write non-empty raftstate")
	}

	dst := &Raft{
		currentTerm: 0,
		votedFor:    noVote,
		log:         []LogEntry{{Term: 0}},
	}
	dst.readPersist(data)

	if dst.currentTerm != 7 {
		t.Errorf("currentTerm got %d want 7", dst.currentTerm)
	}
	if dst.votedFor != 2 {
		t.Errorf("votedFor got %d want 2", dst.votedFor)
	}
	if !reflect.DeepEqual(dst.log, src.log) {
		t.Errorf("log mismatch:\ngot  %#v\nwant %#v", dst.log, src.log)
	}
}

// TestReadPersistEmpty3C: empty blob must not overwrite existing in-memory fields (bootstrap path).
func TestReadPersistEmpty3C(t *testing.T) {
	rf := &Raft{
		currentTerm: 5,
		votedFor:    1,
		log:         []LogEntry{{Term: 0}, {Term: 1, Command: nil}},
	}
	rf.readPersist(nil)
	rf.readPersist([]byte{})
	if rf.currentTerm != 5 || rf.votedFor != 1 || len(rf.log) != 2 {
		t.Fatalf("empty readPersist should be no-op, got term=%d votedFor=%d log=%v",
			rf.currentTerm, rf.votedFor, rf.log)
	}
}

// TestReadPersistCorrupt3C: decode failure must not partially apply (leave rf unchanged).
func TestReadPersistCorrupt3C(t *testing.T) {
	rf := &Raft{
		currentTerm: 9,
		votedFor:    3,
		log:         []LogEntry{{Term: 0}},
	}
	beforeTerm, beforeVote, beforeLog := rf.currentTerm, rf.votedFor, append([]LogEntry(nil), rf.log...)
	rf.readPersist([]byte{0xff, 0xfe, 0xfd})
	if rf.currentTerm != beforeTerm || rf.votedFor != beforeVote || !reflect.DeepEqual(rf.log, beforeLog) {
		t.Fatalf("corrupt blob should not change state: got term=%d vote=%d log=%v",
			rf.currentTerm, rf.votedFor, rf.log)
	}
}
