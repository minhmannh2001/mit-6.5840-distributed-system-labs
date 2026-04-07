package raft

import (
	"sync"
	"testing"

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
