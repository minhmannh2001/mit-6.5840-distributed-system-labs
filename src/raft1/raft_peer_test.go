package raft

import (
	"fmt"
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
		if rf.commitIndex != 0 || rf.lastApplied != 0 {
			t.Fatalf("commitIndex/lastApplied = (%d,%d), want (0,0)", rf.commitIndex, rf.lastApplied)
		}
		if len(rf.log) != 1 || rf.log[0].Term != 0 {
			t.Fatalf("bootstrap log = %v, want single dummy term 0", rf.log)
		}
		if rf.nextIndex != nil || rf.matchIndex != nil {
			t.Fatalf("follower should not have leader replication slices yet; next=%v match=%v", rf.nextIndex, rf.matchIndex)
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

// TestPeer_Part3 covers AppendEntries heartbeats: liveness signal + term handling (Figure 2).
func TestPeer_Part3_AppendEntries(t *testing.T) {
	t.Parallel()

	t.Run("stale_term_rejects_without_updating_follower_term", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 6
		rf.role = RoleFollower
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{Term: 4}, reply)
		if reply.Success || reply.Term != 6 {
			t.Fatalf("want stale reject; Success=%v reply.Term=%d", reply.Success, reply.Term)
		}
		rf.mu.Lock()
		if rf.currentTerm != 6 {
			t.Fatalf("currentTerm = %d, want 6", rf.currentTerm)
		}
		rf.mu.Unlock()
	})

	t.Run("stale_term_does_not_reset_election_deadline", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 5
		rf.role = RoleFollower
		old := time.Now().Add(-2 * time.Hour)
		rf.electionDeadline = old
		rf.mu.Unlock()

		rf.AppendEntries(&AppendEntriesArgs{Term: 2}, &AppendEntriesReply{})

		rf.mu.Lock()
		d := rf.electionDeadline
		rf.mu.Unlock()
		if !d.Equal(old) {
			t.Fatalf("stale AppendEntries must not reset deadline; got %v want %v", d, old)
		}
	})

	t.Run("valid_heartbeat_resets_election_deadline", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.role = RoleFollower
		rf.electionDeadline = time.Now().Add(-time.Hour)
		rf.mu.Unlock()

		before := time.Now()
		rf.AppendEntries(&AppendEntriesArgs{Term: 2}, &AppendEntriesReply{})

		rf.mu.Lock()
		dl := rf.electionDeadline
		rf.mu.Unlock()
		if !dl.After(before) {
			t.Fatalf("valid heartbeat should push deadline forward; deadline=%v before=%v", dl, before)
		}
	})

	t.Run("valid_heartbeat_candidate_same_term_becomes_follower", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 3
		rf.role = RoleCandidate
		rf.votedFor = rf.me
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{Term: 3}, reply)
		if !reply.Success {
			t.Fatal("want Success on valid heartbeat")
		}
		rf.mu.Lock()
		if rf.role != RoleFollower {
			t.Fatalf("role = %v, want RoleFollower", rf.role)
		}
		rf.mu.Unlock()
	})

	t.Run("valid_heartbeat_leader_higher_rpc_term_steps_down", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.role = RoleLeader
		rf.votedFor = rf.me
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{Term: 8}, reply)
		if !reply.Success || reply.Term != 8 {
			t.Fatalf("want Success and new term in reply; Success=%v Term=%d", reply.Success, reply.Term)
		}
		rf.mu.Lock()
		if rf.currentTerm != 8 || rf.role != RoleFollower {
			t.Fatalf("want term 8 follower; term=%d role=%v", rf.currentTerm, rf.role)
		}
		rf.mu.Unlock()
	})

	t.Run("leader_receiving_stale_ae_stays_leader", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 5
		rf.role = RoleLeader
		rf.votedFor = rf.me
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{Term: 3}, reply)
		if reply.Success {
			t.Fatal("stale AE must not succeed")
		}
		rf.mu.Lock()
		if rf.role != RoleLeader || rf.currentTerm != 5 {
			t.Fatalf("leader should stay leader on stale AE; role=%v term=%d", rf.role, rf.currentTerm)
		}
		rf.mu.Unlock()
	})
}

// unitTestRaftCluster wires n Raft peers through labrpc (reliable). Cleanup kills all rafts.
// The returned Network can be used with GetCount(serverName) for heartbeat / RPC assertions.
func unitTestRaftCluster(tb testing.TB, n int) ([]*Raft, *labrpc.Network) {
	tb.Helper()
	rn := labrpc.MakeNetwork()
	rn.Reliable(true)

	endnames := make([][]string, n)
	peers := make([][]*labrpc.ClientEnd, n)
	for i := 0; i < n; i++ {
		endnames[i] = make([]string, n)
		peers[i] = make([]*labrpc.ClientEnd, n)
		for j := 0; j < n; j++ {
			endnames[i][j] = fmt.Sprintf("End-%d-to-%d", i, j)
			peers[i][j] = rn.MakeEnd(endnames[i][j])
		}
	}

	rafs := make([]*Raft, n)
	for i := 0; i < n; i++ {
		applyCh := make(chan raftapi.ApplyMsg, 100)
		r := Make(peers[i], i, tester.MakePersister(), applyCh)
		rf := r.(*Raft)
		rafs[i] = rf
		srv := labrpc.MakeServer()
		srv.AddService(labrpc.MakeService(rf))
		rn.AddServer(raftServerName(i), srv)
	}

	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			rn.Connect(endnames[i][j], raftServerName(j))
			rn.Enable(endnames[i][j], true)
		}
	}

	tb.Cleanup(func() {
		for _, rf := range rafs {
			rf.Kill()
		}
		rn.Cleanup()
	})
	return rafs, rn
}

func raftServerName(i int) string {
	return fmt.Sprintf("raft-server-%d", i)
}

// assertLeaderReplicationFields checks Figure 2 leader invariants after election (3B Part 1).
func assertLeaderReplicationFields(tb testing.TB, rf *Raft, npeers int) {
	tb.Helper()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != RoleLeader {
		tb.Fatalf("want RoleLeader, got %v", rf.role)
	}
	if len(rf.nextIndex) != npeers || len(rf.matchIndex) != npeers {
		tb.Fatalf("nextIndex/matchIndex len: got %d/%d, want %d each", len(rf.nextIndex), len(rf.matchIndex), npeers)
	}
	wantNext := rf.lastLogIndex() + 1
	for i := 0; i < npeers; i++ {
		if rf.nextIndex[i] != wantNext {
			tb.Fatalf("nextIndex[%d] = %d, want %d (lastLogIndex+1)", i, rf.nextIndex[i], wantNext)
		}
		if rf.matchIndex[i] != 0 {
			tb.Fatalf("matchIndex[%d] = %d, want 0 after election", i, rf.matchIndex[i])
		}
	}
}

// TestPeer_3B_Part1: Figure 2 volatile state for log/commit + leader nextIndex/matchIndex (TDD milestone).
func TestPeer_3B_Part1_ReplicationState(t *testing.T) {
	t.Parallel()

	t.Run("manual_leader_init_matches_figure2", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 1)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.role = RoleLeader
		rf.votedFor = rf.me
		rf.initLeaderReplicationLocked()
		rf.mu.Unlock()
		assertLeaderReplicationFields(t, rf, 3)
	})

	t.Run("cluster_elected_leader_has_replication_arrays", func(t *testing.T) {
		if testing.Short() {
			t.Skip("uses cluster election timeouts")
		}
		rafs, _ := unitTestRaftCluster(t, 3)
		waitSingleLeader(t, rafs, 4*time.Second)
		var leader *Raft
		for _, rf := range rafs {
			if _, isL := rf.GetState(); isL {
				leader = rf
				break
			}
		}
		if leader == nil {
			t.Fatal("no leader")
		}
		assertLeaderReplicationFields(t, leader, 3)
	})

	t.Run("solo_leader_has_replication_arrays", func(t *testing.T) {
		if testing.Short() {
			t.Skip("uses cluster election timeouts")
		}
		rafs, _ := unitTestRaftCluster(t, 1)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if _, isL := rafs[0].GetState(); isL {
				assertLeaderReplicationFields(t, rafs[0], 1)
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatal("solo server did not become leader")
	})

	t.Run("append_entries_prev_log_mismatch_rejected", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.role = RoleFollower
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term: 1, PrevLogIndex: 1, PrevLogTerm: 0, Entries: nil, LeaderCommit: 0,
		}, reply)
		if reply.Success {
			t.Fatal("PrevLogIndex beyond log should fail")
		}

		reply2 := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term: 1, PrevLogIndex: 0, PrevLogTerm: 99, Entries: nil, LeaderCommit: 0,
		}, reply2)
		if reply2.Success {
			t.Fatal("PrevLogTerm mismatch should fail")
		}
	})

	t.Run("append_entries_non_empty_succeeds_when_prev_ok", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.role = RoleFollower
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term: 2, PrevLogIndex: 0, PrevLogTerm: 0,
			Entries:      []LogEntry{{Term: 2, Command: 42}},
			LeaderCommit: 0,
		}, reply)
		if !reply.Success {
			t.Fatal("want Success when prev matches and entries replicate")
		}
		rf.mu.Lock()
		if rf.lastLogIndex() != 1 || rf.log[1].Term != 2 {
			t.Fatalf("log after append: got len tail %+v", rf.log)
		}
		rf.mu.Unlock()
	})
}

// TestPeer_3B_Part2: AppendEntries follower — prev check, truncate+append, LeaderCommit (Figure 2).
func TestPeer_3B_Part2_AppendEntriesLog(t *testing.T) {
	t.Parallel()

	t.Run("prev_index_out_of_range_fails_no_timer_reset", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 3
		rf.role = RoleFollower
		old := time.Now().Add(-2 * time.Hour)
		rf.electionDeadline = old
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term: 3, PrevLogIndex: 5, PrevLogTerm: 0, Entries: nil, LeaderCommit: 0,
		}, reply)
		if reply.Success {
			t.Fatal("want failure when PrevLogIndex beyond log")
		}
		rf.mu.Lock()
		if !rf.electionDeadline.Equal(old) {
			t.Fatal("failed AppendEntries must not reset election timer")
		}
		rf.mu.Unlock()
	})

	t.Run("prev_term_mismatch_fails", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.role = RoleFollower
		rf.log = []LogEntry{{Term: 0}, {Term: 1, Command: "a"}}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term: 1, PrevLogIndex: 1, PrevLogTerm: 99, Entries: nil, LeaderCommit: 0,
		}, reply)
		if reply.Success {
			t.Fatal("want failure on PrevLogTerm mismatch")
		}
	})

	t.Run("heartbeat_empty_entries_log_unchanged_commit_updated", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.role = RoleFollower
		rf.log = []LogEntry{{Term: 0}, {Term: 2, Command: "x"}}
		rf.commitIndex = 0
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term: 2, PrevLogIndex: 1, PrevLogTerm: 2, Entries: nil, LeaderCommit: 1,
		}, reply)
		if !reply.Success {
			t.Fatal("want heartbeat success")
		}
		rf.mu.Lock()
		if len(rf.log) != 2 {
			t.Fatalf("log len = %d, want 2", len(rf.log))
		}
		if rf.commitIndex != 1 {
			t.Fatalf("commitIndex = %d, want 1", rf.commitIndex)
		}
		rf.mu.Unlock()
	})

	t.Run("leader_commit_capped_by_follower_log_len", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 1
		rf.role = RoleFollower
		rf.commitIndex = 0
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: nil, LeaderCommit: 99,
		}, reply)
		if !reply.Success {
			t.Fatal("want success")
		}
		rf.mu.Lock()
		if rf.commitIndex != 0 {
			t.Fatalf("commitIndex = %d, want 0 (only dummy at 0)", rf.commitIndex)
		}
		rf.mu.Unlock()
	})

	t.Run("truncate_conflict_then_append", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 4
		rf.role = RoleFollower
		rf.log = []LogEntry{
			{Term: 0},
			{Term: 1, Command: "old1"},
			{Term: 1, Command: "old2"},
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term:         4,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries: []LogEntry{
				{Term: 4, Command: "n1"},
				{Term: 4, Command: "n2"},
			},
			LeaderCommit: 0,
		}, reply)
		if !reply.Success {
			t.Fatal("want success")
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if len(rf.log) != 3 {
			t.Fatalf("len=%d want 3 (0 + 2 new)", len(rf.log))
		}
		if rf.log[1].Command != "n1" || rf.log[2].Command != "n2" {
			t.Fatalf("log = %+v", rf.log)
		}
	})

	t.Run("append_after_matching_prefix", func(t *testing.T) {
		rf := unitTestNewRaft(t, 3, 0)
		rf.mu.Lock()
		rf.currentTerm = 2
		rf.role = RoleFollower
		rf.log = []LogEntry{{Term: 0}, {Term: 2, Command: "p"}}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.AppendEntries(&AppendEntriesArgs{
			Term:         2,
			PrevLogIndex: 1,
			PrevLogTerm:  2,
			Entries:      []LogEntry{{Term: 2, Command: "q"}},
			LeaderCommit: 2,
		}, reply)
		if !reply.Success {
			t.Fatal("want success")
		}
		rf.mu.Lock()
		if len(rf.log) != 3 || rf.lastLogIndex() != 2 {
			t.Fatalf("want 3 entries, got %+v", rf.log)
		}
		if rf.commitIndex != 2 {
			t.Fatalf("commitIndex=%d want 2", rf.commitIndex)
		}
		rf.mu.Unlock()
	})
}

// TestPeer_Part4 integration: ticker + election + leader heartbeats (stable cluster).
func TestPeer_Part4_Election(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster election test uses real timeouts")
	}

	t.Run("three_peers_eventually_one_leader", func(t *testing.T) {
		rafs, _ := unitTestRaftCluster(t, 3)
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			leaders := 0
			var term0 int
			for i, rf := range rafs {
				term, isL := rf.GetState()
				if i == 0 {
					term0 = term
				}
				if isL {
					leaders++
				}
			}
			if leaders == 1 && term0 >= 1 {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatal("expected exactly one leader with term >= 1 within timeout")
	})

	t.Run("single_server_becomes_leader", func(t *testing.T) {
		rafs, _ := unitTestRaftCluster(t, 1)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			_, isL := rafs[0].GetState()
			if isL {
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
		t.Fatal("solo cluster should elect self as leader")
	})
}

// waitSingleLeader blocks until exactly one peer reports isLeader.
func waitSingleLeader(tb testing.TB, rafs []*Raft, maxWait time.Duration) (term int) {
	tb.Helper()
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		leaders := 0
		leaderTerm := 0
		for _, rf := range rafs {
			tm, isL := rf.GetState()
			if isL {
				leaders++
				leaderTerm = tm
			}
		}
		if leaders == 1 {
			return leaderTerm
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatal("timeout waiting for a single leader")
	return 0
}

// TestPeer_Part5: leader periodic AppendEntries; no lock across Call; step-down on higher term in reply (covered in Part 3 handler tests + integration).
func TestPeer_Part5_LeaderHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster heartbeat test uses real time")
	}

	t.Run("follower_incoming_rpc_count_increases_while_stable", func(t *testing.T) {
		rafs, net := unitTestRaftCluster(t, 3)
		waitSingleLeader(t, rafs, 4*time.Second)
		// Let post-election traffic settle so counts mostly reflect heartbeats.
		time.Sleep(200 * time.Millisecond)

		var follower int = -1
		for i, rf := range rafs {
			if _, isL := rf.GetState(); !isL {
				follower = i
				break
			}
		}
		if follower < 0 {
			t.Fatal("expected a non-leader follower")
		}
		sname := raftServerName(follower)
		before := net.GetCount(sname)
		time.Sleep(400 * time.Millisecond)
		after := net.GetCount(sname)
		if got := after - before; got < 2 {
			t.Fatalf("want at least 2 incoming RPCs on follower in 400ms (heartbeats); got %d (before=%d after=%d)", got, before, after)
		}
	})

	t.Run("stable_leader_term_unchanged_short_window", func(t *testing.T) {
		rafs, _ := unitTestRaftCluster(t, 3)
		term := waitSingleLeader(t, rafs, 4*time.Second)
		time.Sleep(2 * time.Second)
		leaders := 0
		for _, rf := range rafs {
			tm, isL := rf.GetState()
			if isL {
				leaders++
			}
			if tm != term {
				t.Fatalf("term drift: want stable %d, saw %d", term, tm)
			}
		}
		if leaders != 1 {
			t.Fatalf("want 1 leader after quiet period, got %d", leaders)
		}
	})

	t.Run("heartbeat_rate_at_most_roughly_ten_per_second", func(t *testing.T) {
		rafs, net := unitTestRaftCluster(t, 3)
		waitSingleLeader(t, rafs, 4*time.Second)
		time.Sleep(200 * time.Millisecond)

		follower := -1
		for i, rf := range rafs {
			if _, isL := rf.GetState(); !isL {
				follower = i
				break
			}
		}
		sname := raftServerName(follower)
		before := net.GetCount(sname)
		time.Sleep(1200 * time.Millisecond)
		after := net.GetCount(sname)
		delta := after - before
		// ~8 heartbeats/s at 120ms interval; allow slack for elections / other RPCs.
		if delta > 14 {
			t.Fatalf("too many incoming RPCs on follower in 1.2s: %d (lab limits ~10 heartbeats/s)", delta)
		}
	})
}
