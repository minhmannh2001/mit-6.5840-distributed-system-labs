package raft

import (
	"testing"

	"6.5840/labrpc"
)

// Part 4 / 3C TDD: follower conflict hints (XLen / XTerm+XIndex) and leader nextIndex backup (paper p.7–8).

func TestFollowerAppendEntriesConflictShortLog3CPart4(t *testing.T) {
	rf := &Raft{
		currentTerm: 3,
		votedFor:    noVote,
		log:         []LogEntry{{Term: 0}, {Term: 1, Command: "a"}},
		role:        RoleFollower,
	}
	reply := &AppendEntriesReply{}
	rf.AppendEntries(&AppendEntriesArgs{
		Term:         3,
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 0,
	}, reply)
	if reply.Success {
		t.Fatal("expected reject")
	}
	if reply.ConflictTerm != -1 || reply.ConflictIndex != len(rf.log) {
		t.Fatalf("short log: want ConflictTerm=-1 ConflictIndex=%d; got %+v", len(rf.log), reply)
	}
}

func TestFollowerAppendEntriesConflictTermMismatch3CPart4(t *testing.T) {
	rf := &Raft{
		currentTerm: 4,
		log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: "a"},
			{Term: 1, Command: "b"},
			{Term: 2, Command: "c"},
			{Term: 2, Command: "d"},
		},
		role: RoleFollower,
	}
	reply := &AppendEntriesReply{}
	rf.AppendEntries(&AppendEntriesArgs{
		Term:         4,
		PrevLogIndex: 3,
		PrevLogTerm:  99, // mismatch: log[3].Term is 2
		Entries:      nil,
		LeaderCommit: 0,
	}, reply)
	if reply.Success {
		t.Fatal("expected reject")
	}
	if reply.ConflictTerm != 2 || reply.ConflictIndex != 3 {
		t.Fatalf("want ConflictTerm=2 ConflictIndex=3 (first index of term 2); got %+v", reply)
	}
}

func TestLeaderNextIndexBackupShortFollowerLog3CPart4(t *testing.T) {
	n := 3
	rf := &Raft{
		me:          0,
		currentTerm: 5,
		role:        RoleLeader,
		log:         []LogEntry{{Term: 0}, {Term: 1}, {Term: 5}},
		peers:       make([]*labrpc.ClientEnd, n),
		nextIndex:   []int{9, 9, 9},
		matchIndex:  make([]int, n),
	}
	peer := 1
	args := &AppendEntriesArgs{PrevLogIndex: 8, PrevLogTerm: 4}
	reply := &AppendEntriesReply{Term: 5, Success: false, ConflictTerm: -1, ConflictIndex: 2}

	rf.mu.Lock()
	rf.appendEntriesFailureUpdateNextIndexLocked(peer, args, reply)
	got := rf.nextIndex[peer]
	rf.mu.Unlock()
	if got != 2 {
		t.Fatalf("XLen case: want nextIndex=2 got %d", got)
	}
}

func TestLeaderNextIndexBackupLeaderLacksConflictTerm3CPart4(t *testing.T) {
	n := 3
	rf := &Raft{
		me:          0,
		currentTerm: 6,
		role:        RoleLeader,
		// Leader has no term 7
		log:   []LogEntry{{Term: 0}, {Term: 1}, {Term: 6}},
		peers: make([]*labrpc.ClientEnd, n),
		// nextIndex[1]=4 → prev index 3
		nextIndex:  []int{9, 4, 9},
		matchIndex: make([]int, n),
	}
	peer := 1
	args := &AppendEntriesArgs{PrevLogIndex: 3, PrevLogTerm: 6}
	reply := &AppendEntriesReply{Term: 6, Success: false, ConflictTerm: 7, ConflictIndex: 1}

	rf.mu.Lock()
	rf.appendEntriesFailureUpdateNextIndexLocked(peer, args, reply)
	got := rf.nextIndex[peer]
	rf.mu.Unlock()
	if got != 1 {
		t.Fatalf("leader lacks XTerm: want nextIndex=ConflictIndex=1 got %d", got)
	}
}

func TestLeaderNextIndexBackupLeaderHasConflictTerm3CPart4(t *testing.T) {
	n := 3
	rf := &Raft{
		me:          0,
		currentTerm: 8,
		role:        RoleLeader,
		log: []LogEntry{
			{Term: 0},
			{Term: 1},
			{Term: 3},
			{Term: 3},
			{Term: 3},
			{Term: 8},
		},
		peers:      make([]*labrpc.ClientEnd, n),
		nextIndex:  []int{9, 6, 9}, // prev = 5
		matchIndex: make([]int, n),
	}
	peer := 1
	args := &AppendEntriesArgs{PrevLogIndex: 5, PrevLogTerm: 8}
	// Follower's first index of term 3 is 2; leader's last index of term 3 is 4 → nextIndex 5
	reply := &AppendEntriesReply{Term: 8, Success: false, ConflictTerm: 3, ConflictIndex: 2}

	rf.mu.Lock()
	rf.appendEntriesFailureUpdateNextIndexLocked(peer, args, reply)
	got := rf.nextIndex[peer]
	rf.mu.Unlock()
	if got != 5 {
		t.Fatalf("leader has XTerm: want nextIndex=5 (after leader's last term-3 entry) got %d", got)
	}
}

func TestLeaderIgnoresStaleAppendEntriesFailure3CPart4(t *testing.T) {
	n := 3
	rf := &Raft{
		me:          0,
		currentTerm: 2,
		role:        RoleLeader,
		log:         []LogEntry{{Term: 0}, {Term: 1}, {Term: 2}},
		peers:       make([]*labrpc.ClientEnd, n),
		nextIndex:   []int{9, 10, 9},
		matchIndex:  make([]int, n),
	}
	peer := 1
	// Old RPC assumed nextIndex was 5 (prev+1=5); leader has since moved to 10
	args := &AppendEntriesArgs{PrevLogIndex: 4, PrevLogTerm: 1}
	reply := &AppendEntriesReply{Term: 2, Success: false, ConflictTerm: -1, ConflictIndex: 1}

	rf.mu.Lock()
	rf.appendEntriesFailureUpdateNextIndexLocked(peer, args, reply)
	got := rf.nextIndex[peer]
	rf.mu.Unlock()
	if got != 10 {
		t.Fatalf("stale failure must not move nextIndex: want 10 got %d", got)
	}
}

func TestLeaderNextIndexBackupClampsToOne3CPart4(t *testing.T) {
	n := 2
	rf := &Raft{
		me:          0,
		currentTerm: 1,
		role:        RoleLeader,
		log:         []LogEntry{{Term: 0}},
		peers:       make([]*labrpc.ClientEnd, n),
		nextIndex:   []int{9, 2},
		matchIndex:  make([]int, n),
	}
	peer := 1
	args := &AppendEntriesArgs{PrevLogIndex: 1, PrevLogTerm: 0}
	reply := &AppendEntriesReply{Term: 1, Success: false, ConflictTerm: -1, ConflictIndex: 0}

	rf.mu.Lock()
	rf.appendEntriesFailureUpdateNextIndexLocked(peer, args, reply)
	got := rf.nextIndex[peer]
	rf.mu.Unlock()
	if got != 1 {
		t.Fatalf("nextIndex must be >= 1, got %d", got)
	}
}
