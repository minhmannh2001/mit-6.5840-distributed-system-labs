//go:build raft_spec

// Raft peer behavior specs for TDD (Figure 2, election + heartbeats).
//
// Run (from src/):
//
//	go test -tags=raft_spec -race -run TestPeer_Spec ./raft1
//
// Expect failures until you implement RequestVote, AppendEntries, and related logic.
// Course tests: go test -race -run 3A ./raft1

package raft

import "testing"

func TestPeer_Spec_RequestVote(t *testing.T) {
	t.Parallel()

	t.Run("rejects_stale_term", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 5
		rf.role = RoleFollower
		rf.votedFor = -1
		rf.mu.Unlock()

		args := &RequestVoteArgs{Term: 3, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)

		if reply.VoteGranted {
			t.Fatal("VoteGranted: want false for stale term")
		}
		if reply.Term != 5 {
			t.Fatalf("reply.Term = %d, want 5 (follower's current term)", reply.Term)
		}
	})

	t.Run("higher_term_resets_to_follower_and_grants_when_eligible", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.role = RoleCandidate
		rf.votedFor = rf.me
		rf.mu.Unlock()

		args := &RequestVoteArgs{Term: 5, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)

		if !reply.VoteGranted {
			t.Fatal("VoteGranted: want true on first eligible vote in new term")
		}
		if reply.Term != 5 {
			t.Fatalf("reply.Term = %d, want 5", reply.Term)
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != 5 {
			t.Fatalf("currentTerm = %d, want 5", rf.currentTerm)
		}
		if rf.role != RoleFollower {
			t.Fatalf("role = %v, want RoleFollower after seeing higher term", rf.role)
		}
		if rf.votedFor != 1 {
			t.Fatalf("votedFor = %d, want 1", rf.votedFor)
		}
	})

	t.Run("at_most_one_grant_per_term_different_candidates", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.role = RoleFollower
		rf.votedFor = -1
		rf.mu.Unlock()

		r1 := &RequestVoteReply{}
		rf.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 1, LastLogIndex: 0, LastLogTerm: 0}, r1)
		if !r1.VoteGranted {
			t.Fatal("first vote should be granted")
		}

		r2 := &RequestVoteReply{}
		rf.RequestVote(&RequestVoteArgs{Term: 1, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}, r2)
		if r2.VoteGranted {
			t.Fatal("second candidate in same term must not get vote")
		}
	})

	t.Run("repeat_same_candidate_same_term_idempotent", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 4
		rf.role = RoleFollower
		rf.votedFor = -1
		rf.mu.Unlock()

		args := &RequestVoteArgs{Term: 4, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0}
		rA := &RequestVoteReply{}
		rf.RequestVote(args, rA)
		rB := &RequestVoteReply{}
		rf.RequestVote(args, rB)

		if !rA.VoteGranted || !rB.VoteGranted {
			t.Fatalf("retries from same candidate should keep granting: rA=%v rB=%v", rA.VoteGranted, rB.VoteGranted)
		}
	})
}

func TestPeer_Spec_AppendEntries_Heartbeat(t *testing.T) {
	t.Parallel()

	t.Run("rejects_stale_term", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 4
		rf.role = RoleFollower
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{Term: 2}, reply)

		if reply.Success {
			t.Fatal("Success: want false when leader term is stale")
		}
		if reply.Term != 4 {
			t.Fatalf("reply.Term = %d, want follower current term 4", reply.Term)
		}
	})

	t.Run("valid_same_term_heartbeat_from_leader_makes_candidate_follower", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 3
		rf.role = RoleCandidate
		rf.votedFor = rf.me
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{Term: 3}, reply)

		if !reply.Success {
			t.Fatal("Success: want true for valid heartbeat at same term")
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != RoleFollower {
			t.Fatalf("role = %v, want RoleFollower after valid AppendEntries", rf.role)
		}
	})

	t.Run("higher_term_forces_follower", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.role = RoleLeader
		rf.votedFor = rf.me
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{Term: 7}, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != 7 {
			t.Fatalf("currentTerm = %d, want 7", rf.currentTerm)
		}
		if rf.role != RoleFollower {
			t.Fatalf("role = %v, want RoleFollower", rf.role)
		}
		if !reply.Success {
			t.Fatal("Success: want true when adopting higher term via AppendEntries")
		}
	})
}
