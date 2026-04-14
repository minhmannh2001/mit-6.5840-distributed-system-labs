package raft

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

func init() {
	// Same concrete command types as Make(); needed before labgob encode/decode in unit tests.
	labgob.Register(LogEntry{})
	labgob.Register(0)
	labgob.Register("")
}

func mustDecodeRaftState(t *testing.T, data []byte) (term int, voted int, log []LogEntry) {
	t.Helper()
	if len(data) < 1 {
		t.Fatal("empty raftstate")
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&term) != nil || d.Decode(&voted) != nil || d.Decode(&log) != nil {
		t.Fatal("decode raftstate failed")
	}
	return term, voted, log
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

// --- 3C Part 2 (TDD): durable state must hit Persister when RPCs / election / Start mutate it.

func TestBecomeFollowerHigherTermPersists3C(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    1,
		log:         []LogEntry{{Term: 0}, {Term: 2, Command: nil}},
		role:        RoleCandidate,
	}
	rf.mu.Lock()
	rf.becomeFollower(5)
	rf.mu.Unlock()

	term, voted, log := mustDecodeRaftState(t, p.ReadRaftState())
	if term != 5 || voted != noVote {
		t.Fatalf("after term bump: term=%d votedFor=%d want term=5 votedFor=noVote", term, voted)
	}
	if !reflect.DeepEqual(log, rf.log) {
		t.Fatalf("log should be unchanged, got %#v want %#v", log, rf.log)
	}
}

func TestBecomeFollowerSameTermDoesNotWritePersister3C(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 3,
		votedFor:    0,
		log:         []LogEntry{{Term: 0}},
		role:        RoleLeader,
	}
	rf.mu.Lock()
	rf.becomeFollower(3) // demote only; Figure-2 durable fields unchanged
	rf.mu.Unlock()
	if len(p.ReadRaftState()) != 0 {
		t.Fatalf("same-term becomeFollower should not persist, got %d bytes", len(p.ReadRaftState()))
	}
}

func TestRequestVoteGrantPersists3C(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 1,
		votedFor:    noVote,
		log:         []LogEntry{{Term: 0}, {Term: 1, Command: 7}},
		role:        RoleFollower,
	}
	args := &RequestVoteArgs{Term: 1, CandidateId: 2, LastLogIndex: 1, LastLogTerm: 1}
	reply := &RequestVoteReply{}
	rf.RequestVote(args, reply)
	if !reply.VoteGranted {
		t.Fatal("expected vote granted")
	}
	term, voted, _ := mustDecodeRaftState(t, p.ReadRaftState())
	if term != 1 || voted != 2 {
		t.Fatalf("persisted term=%d votedFor=%d want 1, 2", term, voted)
	}
}

func TestAppendEntriesSuccessPersistsLog3C(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    noVote,
		log:         []LogEntry{{Term: 0}, {Term: 1, Command: "old"}},
		role:        RoleFollower,
	}
	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2, Command: 99}},
		LeaderCommit: 0,
	}
	reply := &AppendEntriesReply{}
	rf.AppendEntries(args, reply)
	if !reply.Success {
		t.Fatalf("AppendEntries failed: %+v", reply)
	}
	_, _, log := mustDecodeRaftState(t, p.ReadRaftState())
	want := []LogEntry{{Term: 0}, {Term: 1, Command: "old"}, {Term: 2, Command: 99}}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("log got %#v want %#v", log, want)
	}
}

func TestStartLeaderPersists3C(t *testing.T) {
	p := tester.MakePersister()
	n := 3
	rf := &Raft{
		persister:   p,
		peers:       make([]*labrpc.ClientEnd, n),
		me:          1,
		currentTerm: 4,
		votedFor:    1,
		role:        RoleLeader,
		log:         []LogEntry{{Term: 0}, {Term: 4, Command: 1}},
		nextIndex:   make([]int, n),
		matchIndex:  make([]int, n),
	}
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = 2
	}
	idx, _, ok := rf.Start(200)
	if !ok || idx != 2 {
		t.Fatalf("Start ok=%v idx=%d want true, 2", ok, idx)
	}
	_, _, log := mustDecodeRaftState(t, p.ReadRaftState())
	if len(log) != 3 || log[2].Term != 4 || log[2].Command != 200 {
		t.Fatalf("persisted log after Start: %#v", log)
	}
}

func TestStartElectionPersists3C(t *testing.T) {
	p := tester.MakePersister()
	n := 2
	rf := &Raft{
		persister:        p,
		peers:            make([]*labrpc.ClientEnd, n),
		me:               0,
		currentTerm:      1,
		votedFor:         noVote,
		role:             RoleFollower,
		log:              []LogEntry{{Term: 0}},
		electionDeadline: time.Time{}, // always in the past → startElection proceeds
	}
	rf.startElection()
	term, voted, _ := mustDecodeRaftState(t, p.ReadRaftState())
	if term != 2 || voted != 0 {
		t.Fatalf("after startElection: term=%d votedFor=%d want 2, 0", term, voted)
	}
}

// TestAppendEntriesShrinksLogClampsCommitIndex3C: after truncating the log, commitIndex must not
// stay above lastLogIndex (otherwise applier can index past the log under concurrency / reordering).
func TestAppendEntriesShrinksLogClampsCommitIndex3C(t *testing.T) {
	p := tester.MakePersister()
	rf := &Raft{
		persister:   p,
		currentTerm: 2,
		votedFor:    noVote,
		log: []LogEntry{
			{Term: 0}, {Term: 1, Command: "a"}, {Term: 1, Command: "b"}, {Term: 2, Command: "c"},
		},
		commitIndex: 3,
		role:        RoleFollower,
	}
	args := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2, Command: "c"}},
		LeaderCommit: 2, // < old commitIndex; log shrinks so commitIndex must clamp
	}
	reply := &AppendEntriesReply{}
	rf.AppendEntries(args, reply)
	if !reply.Success {
		t.Fatalf("expected success, got %+v", reply)
	}
	if rf.commitIndex > rf.lastLogIndex() {
		t.Fatalf("commitIndex %d > lastLogIndex %d", rf.commitIndex, rf.lastLogIndex())
	}
}

// --- 3C Part 3 (TDD): Make() default → readPersist → election timer; log[0] dummy invariant.

func encodeRaftState3C(t *testing.T, term int, voted int, log []LogEntry) []byte {
	t.Helper()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(term) != nil || e.Encode(voted) != nil || e.Encode(log) != nil {
		t.Fatal("encode failed")
	}
	return w.Bytes()
}

// TestMakeEmptyPersisterKeepsDefaults3C: no raftstate blob → defaults from Make (including dummy log[0]).
func TestMakeEmptyPersisterKeepsDefaults3C(t *testing.T) {
	p := tester.MakePersister()
	applyCh := make(chan raftapi.ApplyMsg, 64)
	r := Make([]*labrpc.ClientEnd{nil}, 0, p, applyCh)
	defer r.Kill()

	rf := r.(*Raft)
	rf.mu.Lock()
	tm, vf, ln := rf.currentTerm, rf.votedFor, len(rf.log)
	z := rf.log[0].Term
	rf.mu.Unlock()
	if tm != 0 || vf != noVote || ln != 1 || z != 0 {
		t.Fatalf("want term=0 votedFor=noVote len=1 log[0].term=0; got term=%d voted=%d len=%d log0.term=%d",
			tm, vf, ln, z)
	}
	term, leader := r.GetState()
	if term != 0 || leader {
		t.Fatalf("GetState: term=%d leader=%v want 0, false", term, leader)
	}
}

// TestMakeReadPersistOverwritesDefaults3C: persisted state must replace defaults after Make.
func TestMakeReadPersistOverwritesDefaults3C(t *testing.T) {
	p := tester.MakePersister()
	blob := encodeRaftState3C(t, 11, 2, []LogEntry{{Term: 0}, {Term: 11, Command: 99}})
	p.Save(blob, nil)

	applyCh := make(chan raftapi.ApplyMsg, 64)
	r := Make([]*labrpc.ClientEnd{nil}, 0, p, applyCh)
	defer r.Kill()

	rf := r.(*Raft)
	rf.mu.Lock()
	tm, vf := rf.currentTerm, rf.votedFor
	if len(rf.log) != 2 || rf.log[1].Term != 11 || rf.log[1].Command != 99 {
		t.Fatalf("log got %#v", rf.log)
	}
	rf.mu.Unlock()
	if tm != 11 || vf != 2 {
		t.Fatalf("want term=11 votedFor=2; got term=%d voted=%d", tm, vf)
	}
	gotTerm, _ := r.GetState()
	if gotTerm != 11 {
		t.Fatalf("GetState term=%d want 11", gotTerm)
	}
}

// TestReadPersistRejectsMalformedDummyLog3C: index 0 must be term 0; otherwise keep prior rf fields.
func TestReadPersistRejectsMalformedDummyLog3C(t *testing.T) {
	bad := encodeRaftState3C(t, 3, noVote, []LogEntry{{Term: 5, Command: "x"}})
	rf := &Raft{
		currentTerm: 9,
		votedFor:    1,
		log:         []LogEntry{{Term: 0}, {Term: 4, Command: 1}},
	}
	rf.readPersist(bad)
	if rf.currentTerm != 9 || rf.votedFor != 1 || len(rf.log) != 2 {
		t.Fatalf("expected no-op on bad dummy, got term=%d voted=%d log=%v", rf.currentTerm, rf.votedFor, rf.log)
	}
}

// TestReadPersistRejectsEmptyDecodedLog3C: empty log slice is invalid for this Raft implementation.
func TestReadPersistRejectsEmptyDecodedLog3C(t *testing.T) {
	emptyLog := encodeRaftState3C(t, 1, noVote, []LogEntry{})
	rf := &Raft{
		currentTerm: 2,
		votedFor:    0,
		log:         []LogEntry{{Term: 0}},
	}
	rf.readPersist(emptyLog)
	if rf.currentTerm != 2 || rf.votedFor != 0 || len(rf.log) != 1 {
		t.Fatalf("expected defaults kept, got term=%d voted=%d log=%v", rf.currentTerm, rf.votedFor, rf.log)
	}
}

// TestMakeWithMalformedPersistKeepsDummy3C: Make must not install a log without a valid dummy at 0.
func TestMakeWithMalformedPersistKeepsDummy3C(t *testing.T) {
	p := tester.MakePersister()
	p.Save(encodeRaftState3C(t, 4, noVote, []LogEntry{{Term: 2, Command: nil}}), nil)

	applyCh := make(chan raftapi.ApplyMsg, 64)
	r := Make([]*labrpc.ClientEnd{nil}, 0, p, applyCh)
	defer r.Kill()

	rf := r.(*Raft)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) != 1 || rf.log[0].Term != 0 {
		t.Fatalf("want bootstrap dummy only, got %#v", rf.log)
	}
	// term/votedFor from blob should also be rejected entirely when log is bad — same guard.
	if rf.currentTerm != 0 || rf.votedFor != noVote {
		t.Fatalf("malformed log blob should not apply term/vote; got term=%d voted=%d", rf.currentTerm, rf.votedFor)
	}
}
