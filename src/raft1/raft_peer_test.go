package raft

import (
	"sync"
	"testing"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// unitTestNewRaft builds a Raft peer for isolated unit tests (no labrpc network).
// peers may be nil ClientEnds; handlers are invoked directly on *Raft.
func unitTestNewRaft(tb testing.TB, npeers, me int) *Raft {
	tb.Helper()
	peers := make([]*labrpc.ClientEnd, npeers)
	p := tester.MakePersister()
	applyCh := make(chan raftapi.ApplyMsg, 16)
	r := Make(peers, me, p, applyCh)
	rf, ok := r.(*Raft)
	if !ok {
		tb.Fatal("Make() must return concrete *Raft")
	}
	tb.Cleanup(func() {
		rf.Kill()
	})
	return rf
}

// TestPeer_Part0 covers bootstrap state and API surface from the incremental 3A plan.
func TestPeer_Part0(t *testing.T) {
	t.Parallel()

	t.Run("bootstrap_follower_term_zero", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != 0 {
			t.Fatalf("currentTerm = %d, want 0", rf.currentTerm)
		}
		if rf.votedFor != -1 {
			t.Fatalf("votedFor = %d, want -1 (none)", rf.votedFor)
		}
		if rf.role != RoleFollower {
			t.Fatalf("role = %v, want RoleFollower", rf.role)
		}
		if rf.me != 0 {
			t.Fatalf("me = %d, want 0", rf.me)
		}
	})

	t.Run("getState_not_leader", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 1)
		term, isLeader := rf.GetState()
		if term != 0 || isLeader {
			t.Fatalf("GetState() = (%d, %v), want (0, false)", term, isLeader)
		}
	})

	t.Run("start_before_election_not_leader", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 2)
		idx, term, ok := rf.Start("x")
		if idx != -1 || term != 0 || ok {
			t.Fatalf("Start() = (%d, %d, %v), want (-1, 0, false)", idx, term, ok)
		}
	})

	t.Run("getState_concurrent_no_panic", func(t *testing.T) {
		rf := unitTestNewRaft(t, 5, 0)
		var wg sync.WaitGroup
		for g := 0; g < 32; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					_, _ = rf.GetState()
				}
			}()
		}
		wg.Wait()
	})
}

// TestPeer_Part1 exercises becomeFollower: higher term clears vote and demotes to follower (Figure 2).
func TestPeer_Part1(t *testing.T) {
	t.Parallel()

	t.Run("higher_term_updates_term_clears_vote_sets_follower", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.role = RoleLeader
		rf.votedFor = rf.me
		rf.becomeFollower(6)
		if rf.currentTerm != 6 {
			t.Fatalf("currentTerm = %d, want 6", rf.currentTerm)
		}
		if rf.votedFor != -1 {
			t.Fatalf("votedFor = %d, want -1 after new term", rf.votedFor)
		}
		if rf.role != RoleFollower {
			t.Fatalf("role = %v, want RoleFollower", rf.role)
		}
		rf.mu.Unlock()
	})

	t.Run("same_term_only_demotes_preserves_term_and_vote", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 4
		rf.role = RoleCandidate
		rf.votedFor = 2
		rf.becomeFollower(4)
		if rf.currentTerm != 4 {
			t.Fatalf("currentTerm = %d, want 4 (unchanged)", rf.currentTerm)
		}
		if rf.votedFor != 2 {
			t.Fatalf("votedFor = %d, want 2 (same term, not a new term)", rf.votedFor)
		}
		if rf.role != RoleFollower {
			t.Fatalf("role = %v, want RoleFollower", rf.role)
		}
		rf.mu.Unlock()
	})

	t.Run("lower_term_is_no_op", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 8
		rf.role = RoleLeader
		rf.votedFor = rf.me
		rf.becomeFollower(3)
		if rf.currentTerm != 8 || rf.votedFor != rf.me || rf.role != RoleLeader {
			t.Fatalf("stale newTerm must not change state; got term=%d votedFor=%d role=%v",
				rf.currentTerm, rf.votedFor, rf.role)
		}
		rf.mu.Unlock()
	})
}

// TestPeer_Part2 covers RequestVote: one vote per term + election restriction (Figure 2, §5.4.1).
func TestPeer_Part2_RequestVote(t *testing.T) {
	t.Parallel()

	t.Run("reject_stale_term", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 7
		rf.votedFor = -1
		rf.mu.Unlock()

		reply := &RequestVoteReply{}
		rf.RequestVote(&RequestVoteArgs{Term: 4, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0}, reply)
		if reply.VoteGranted || reply.Term != 7 {
			t.Fatalf("want reject stale; got granted=%v term=%d", reply.VoteGranted, reply.Term)
		}
	})

	t.Run("one_vote_per_term_second_candidate_denied", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.votedFor = -1
		rf.mu.Unlock()

		rf.RequestVote(&RequestVoteArgs{Term: 2, CandidateId: 0, LastLogIndex: 0, LastLogTerm: 0}, &RequestVoteReply{})
		r2 := &RequestVoteReply{}
		rf.RequestVote(&RequestVoteArgs{Term: 2, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0}, r2)
		if r2.VoteGranted {
			t.Fatal("second different candidate must not get vote in same term")
		}
	})

	t.Run("same_candidate_retry_still_granted", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		args := &RequestVoteArgs{Term: 3, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}
		rA, rB := &RequestVoteReply{}, &RequestVoteReply{}
		rf.RequestVote(args, rA)
		rf.RequestVote(args, rB)
		if !rA.VoteGranted || !rB.VoteGranted {
			t.Fatalf("same candidate retries should grant; A=%v B=%v", rA.VoteGranted, rB.VoteGranted)
		}
	})

	t.Run("deny_when_candidate_last_term_behind", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.votedFor = -1
		rf.log = []LogEntry{{Term: 0}, {Term: 1}, {Term: 5}}
		rf.mu.Unlock()

		reply := &RequestVoteReply{}
		rf.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 1, LastLogIndex: 1, LastLogTerm: 1}, reply)
		if reply.VoteGranted {
			t.Fatal("candidate with older last log term must not get vote")
		}
	})

	t.Run("deny_when_same_last_term_but_shorter_log", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.votedFor = -1
		rf.log = []LogEntry{{Term: 0}, {Term: 3}, {Term: 3}, {Term: 3}}
		rf.mu.Unlock()

		reply := &RequestVoteReply{}
		rf.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 1, LastLogIndex: 1, LastLogTerm: 3}, reply)
		if reply.VoteGranted {
			t.Fatal("candidate with same last term but shorter log must not get vote")
		}
	})

	t.Run("grant_when_candidate_log_up_to_date", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.votedFor = -1
		rf.log = []LogEntry{{Term: 0}, {Term: 3}, {Term: 3}, {Term: 3}}
		rf.mu.Unlock()

		reply := &RequestVoteReply{}
		rf.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 1, LastLogIndex: 3, LastLogTerm: 3}, reply)
		if !reply.VoteGranted {
			t.Fatal("candidate at least as up-to-date should get vote when eligible")
		}
	})

	t.Run("higher_rpc_term_steps_down_then_grants_if_log_ok", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.role = RoleCandidate
		rf.votedFor = rf.me
		rf.log = []LogEntry{{Term: 0}}
		rf.mu.Unlock()

		reply := &RequestVoteReply{}
		rf.RequestVote(&RequestVoteArgs{Term: 4, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}, reply)
		if !reply.VoteGranted || reply.Term != 4 {
			t.Fatalf("want grant after stepping to new term; granted=%v term=%d", reply.VoteGranted, reply.Term)
		}
		rf.mu.Lock()
		if rf.votedFor != 2 {
			t.Fatalf("votedFor = %d, want 2", rf.votedFor)
		}
		rf.mu.Unlock()
	})

	t.Run("grant_vote_pushes_election_deadline_forward", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.votedFor = -1
		rf.electionDeadline = time.Now().Add(-time.Hour)
		rf.mu.Unlock()

		before := time.Now()
		rf.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 0, LastLogIndex: 0, LastLogTerm: 0}, &RequestVoteReply{})

		rf.mu.Lock()
		deadline := rf.electionDeadline
		rf.mu.Unlock()
		if !deadline.After(before) {
			t.Fatalf("election deadline should be reset to future on grant; deadline=%v before=%v", deadline, before)
		}
	})
}
