package raft

import (
	"fmt"
	"math/rand"
	"strings"
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

// unitTestRaftClusterWithApplyCh wires n Raft peers through labrpc (reliable) and returns
// each peer's applyCh so tests can assert ApplyMsg (3B Part 5).
func unitTestRaftClusterWithApplyCh(tb testing.TB, n int) ([]*Raft, *labrpc.Network, []chan raftapi.ApplyMsg) {
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

	applyChs := make([]chan raftapi.ApplyMsg, n)
	rafs := make([]*Raft, n)
	for i := 0; i < n; i++ {
		applyChs[i] = make(chan raftapi.ApplyMsg, 100)
		r := Make(peers[i], i, tester.MakePersister(), applyChs[i])
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
	return rafs, rn, applyChs
}

// unitTestRaftCluster wires n Raft peers through labrpc (reliable). Cleanup kills all rafts.
// The returned Network can be used with GetCount(serverName) for heartbeat / RPC assertions.
func unitTestRaftCluster(tb testing.TB, n int) ([]*Raft, *labrpc.Network) {
	rafs, net, _ := unitTestRaftClusterWithApplyCh(tb, n)
	return rafs, net
}

func raftServerName(i int) string {
	return fmt.Sprintf("raft-server-%d", i)
}

// rpcTotalIncoming sums labrpc.Server incoming RPC counts for each Raft server (Phần 13).
func rpcTotalIncoming(net *labrpc.Network, n int) int {
	tot := 0
	for i := 0; i < n; i++ {
		tot += net.GetCount(raftServerName(i))
	}
	return tot
}

// unitTestDisconnectPeer disables all ClientEnds to/from peer pid (same idea as tester ServerGrp.DisconnectAll).
func unitTestDisconnectPeer(net *labrpc.Network, n, pid int) {
	for k := 0; k < n; k++ {
		net.Enable(fmt.Sprintf("End-%d-to-%d", k, pid), false)
		net.Enable(fmt.Sprintf("End-%d-to-%d", pid, k), false)
	}
}

// unitTestReconnectPeer re-enables all ends for peer pid.
func unitTestReconnectPeer(net *labrpc.Network, n, pid int) {
	for k := 0; k < n; k++ {
		net.Enable(fmt.Sprintf("End-%d-to-%d", k, pid), true)
		net.Enable(fmt.Sprintf("End-%d-to-%d", pid, k), true)
	}
}

// unitTestConnectSubset enables bidirectional links between every pair in peers (clique only).
// Use this instead of unitTestReconnectPeer when other peers must stay fully isolated (matches
// tester ConnectOne semantics: no accidental link to a disconnected peer).
func unitTestConnectSubset(net *labrpc.Network, n int, peers []int) {
	for a := 0; a < len(peers); a++ {
		for b := a + 1; b < len(peers); b++ {
			i, j := peers[a], peers[b]
			if i < 0 || i >= n || j < 0 || j >= n {
				continue
			}
			net.Enable(fmt.Sprintf("End-%d-to-%d", i, j), true)
			net.Enable(fmt.Sprintf("End-%d-to-%d", j, i), true)
		}
	}
}

// unitTestConnectAll enables the full mesh among all peers 0..n-1.
func unitTestConnectAll(net *labrpc.Network, n int) {
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			net.Enable(fmt.Sprintf("End-%d-to-%d", i, j), true)
		}
	}
}

// waitLeaderInSubset waits until some peer in subset reports isLeader (partition-aware leader check).
func waitLeaderInSubset(tb testing.TB, rafs []*Raft, subset []int, maxWait time.Duration) int {
	tb.Helper()
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		for _, i := range subset {
			if i < 0 || i >= len(rafs) {
				continue
			}
			if _, isL := rafs[i].GetState(); isL {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatal("timeout: no leader among subset", subset)
	return -1
}

// waitAtLeastNCommit waits until at least `atLeast` peers have commitIndex >= want.
func waitAtLeastNCommit(tb testing.TB, rafs []*Raft, want, atLeast int) {
	tb.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		n := 0
		for _, rf := range rafs {
			rf.mu.Lock()
			ci := rf.commitIndex
			rf.mu.Unlock()
			if ci >= want {
				n++
			}
		}
		if n >= atLeast {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timeout: want >=%d peers with commitIndex>=%d", atLeast, want)
}

// waitAtLeastNCommitSubset counts only peers in indices (for partitioned clusters).
func waitAtLeastNCommitSubset(tb testing.TB, rafs []*Raft, peerIdx []int, want int, atLeast int) {
	tb.Helper()
	waitAtLeastNCommitSubsetFor(tb, rafs, peerIdx, want, atLeast, 45*time.Second)
}

// waitAtLeastNCommitSubsetFor is like waitAtLeastNCommitSubset but with a custom wait budget.
func waitAtLeastNCommitSubsetFor(tb testing.TB, rafs []*Raft, peerIdx []int, want int, atLeast int, budget time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		n := 0
		for _, pi := range peerIdx {
			if pi < 0 || pi >= len(rafs) {
				continue
			}
			rf := rafs[pi]
			rf.mu.Lock()
			ci := rf.commitIndex
			rf.mu.Unlock()
			if ci >= want {
				n++
			}
		}
		if n >= atLeast {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	tb.Fatalf("timeout: want >=%d among %v with commitIndex>=%d", atLeast, peerIdx, want)
}

// assertMaxCommitIndex asserts no peer has commitIndex > maxWant (3B Part 7 — no commit past quorum).
func assertMaxCommitIndex(tb testing.TB, rafs []*Raft, maxWant int) {
	tb.Helper()
	for i, rf := range rafs {
		rf.mu.Lock()
		ci := rf.commitIndex
		rf.mu.Unlock()
		if ci > maxWant {
			tb.Fatalf("peer %d commitIndex=%d, want <= %d", i, ci, maxWant)
		}
	}
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

// peerLogSnapshot returns a copy of rf.log (for tests).
func peerLogSnapshot(rf *Raft) []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	out := make([]LogEntry, len(rf.log))
	copy(out, rf.log)
	return out
}

// TestPeer_3B_Part4: Start on leader + replication so all peers share the same log (TDD for §5.3).
func TestPeer_3B_Part4_LogReplicationCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, _ := unitTestRaftCluster(t, 3)
	waitSingleLeader(t, rafs, 5*time.Second)

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

	for i, rf := range rafs {
		if rf == leader {
			continue
		}
		idx, _, ok := rf.Start(999)
		if ok || idx != startIndexNotReady {
			t.Fatalf("follower %d: Start should fail (got idx=%d ok=%v)", i, idx, ok)
		}
	}

	if _, _, ok := leader.Start(10); !ok {
		t.Fatal("leader Start(10) failed")
	}
	if _, _, ok := leader.Start(20); !ok {
		t.Fatal("leader Start(20) failed")
	}
	if _, _, ok := leader.Start(30); !ok {
		t.Fatal("leader Start(30) failed")
	}

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		l0 := peerLogSnapshot(rafs[0])
		if len(l0) < 4 {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		match := true
		for i := 1; i < len(rafs); i++ {
			li := peerLogSnapshot(rafs[i])
			if len(li) != len(l0) {
				match = false
				break
			}
			for j := 1; j < len(l0); j++ {
				if l0[j].Term != li[j].Term || l0[j].Command != li[j].Command {
					match = false
					break
				}
			}
			if !match {
				break
			}
		}
		if match {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	l0 := peerLogSnapshot(rafs[0])
	t.Fatalf("logs did not converge: peer0 log=%+v", l0)
}

// assertLeaderFollowersCaughtUp checks Figure 2 replication on the leader after followers
// have received all entries: matchIndex[i]==lastLogIndex, nextIndex[i]==lastLogIndex+1 for i != me.
func assertLeaderFollowersCaughtUp(tb testing.TB, leader *Raft) {
	tb.Helper()
	leader.mu.Lock()
	defer leader.mu.Unlock()
	if leader.role != RoleLeader {
		tb.Fatalf("want RoleLeader, got %v", leader.role)
	}
	last := leader.lastLogIndex()
	wantNext := last + 1
	for i := range leader.peers {
		if i == leader.me {
			continue
		}
		if leader.matchIndex[i] != last {
			tb.Fatalf("matchIndex[%d]=%d, want %d (fully replicated)", i, leader.matchIndex[i], last)
		}
		if leader.nextIndex[i] != wantNext {
			tb.Fatalf("nextIndex[%d]=%d, want %d", i, leader.nextIndex[i], wantNext)
		}
	}
}

// TestPeer_3B_Part4_LeaderNextMatch: after Start + heartbeat replication, leader's nextIndex/matchIndex
// reflect successful AppendEntries (TDD for Phần 4 — §5.3 leader bookkeeping).
func TestPeer_3B_Part4_LeaderNextMatchAfterReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, _ := unitTestRaftCluster(t, 3)
	waitSingleLeader(t, rafs, 5*time.Second)

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

	for _, cmd := range []interface{}{11, 22, 33} {
		if _, _, ok := leader.Start(cmd); !ok {
			t.Fatalf("leader Start(%v) failed", cmd)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leader.mu.Lock()
		ok := leader.role == RoleLeader
		last := 0
		if ok {
			last = leader.lastLogIndex()
		}
		all := ok && last >= 3
		if all {
			for i := range leader.peers {
				if i == leader.me {
					continue
				}
				if leader.matchIndex[i] != last {
					all = false
					break
				}
			}
		}
		leader.mu.Unlock()
		if all {
			assertLeaderFollowersCaughtUp(t, leader)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timeout: followers did not catch up (matchIndex)")
}

// TestPeer_3B_Part5_ApplyChCommitOrder: leader advanceCommitIndex + applier — mọi peer nhận đủ
// ApplyMsg{CommandValid, CommandIndex 1..K} đúng thứ tự, khớp chuỗi Start trên leader (TDD Phần 5).
func TestPeer_3B_Part5_ApplyChCommitOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, _, applyChs := unitTestRaftClusterWithApplyCh(t, 3)
	waitSingleLeader(t, rafs, 5*time.Second)

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

	cmds := []string{"p5a", "p5b", "p5c"}
	for _, c := range cmds {
		if _, _, ok := leader.Start(c); !ok {
			t.Fatalf("leader Start(%q) failed", c)
		}
	}

	for peer := 0; peer < 3; peer++ {
		var got []raftapi.ApplyMsg
		deadline := time.Now().Add(6 * time.Second)
		for len(got) < 3 {
			if time.Now().After(deadline) {
				t.Fatalf("peer %d: got %d ApplyMsg, want 3", peer, len(got))
			}
			select {
			case m := <-applyChs[peer]:
				if m.CommandValid {
					got = append(got, m)
				}
			case <-time.After(50 * time.Millisecond):
			}
		}
		for j := 0; j < 3; j++ {
			if got[j].CommandIndex != j+1 {
				t.Fatalf("peer %d: ApplyMsg[%d].CommandIndex=%d want %d", peer, j, got[j].CommandIndex, j+1)
			}
			if got[j].Command != cmds[j] {
				t.Fatalf("peer %d: got cmd %v want %q", peer, got[j].Command, cmds[j])
			}
		}
	}
}

// TestPeer_3B_Part6_RPCBytesLargePayload: tăng byte RPC sau N entry lớn phải ~servers×tổng payload (giống ý TestRPCBytes3B).
// Nếu leader gửi lại toàn bộ log mỗi heartbeat, delta vượt ngưỡng.
func TestPeer_3B_Part6_RPCBytesLargePayload(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, net := unitTestRaftCluster(t, 3)
	waitSingleLeader(t, rafs, 5*time.Second)

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

	if _, _, ok := leader.Start("seed"); !ok {
		t.Fatal("seed Start failed")
	}
	waitAllServersCommitAtLeast(t, rafs, 1)

	bytes0 := net.GetTotalBytes()
	const payloadLen = 5000
	iters := 10
	var sent int64
	for i := 0; i < iters; i++ {
		cmd := strings.Repeat("x", payloadLen)
		sent += int64(payloadLen)
		if _, _, ok := leader.Start(cmd); !ok {
			t.Fatalf("Start iteration %d failed", i)
		}
		waitAllServersCommitAtLeast(t, rafs, 2+i)
	}

	bytes1 := net.GetTotalBytes()
	got := bytes1 - bytes0
	expected := int64(len(rafs)) * sent
	// Align with raft_test.go TestRPCBytes3B slack for RPC framing / heartbeats.
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes: got %d, expected about %d (+50k slack); leader may be re-sending full log each tick", got, expected)
	}
}

// TestPeer_3B_Part7_FollowerProgressiveDisconnect mirrors TestFollowerFailure3B (TDD Phần 7).
func TestPeer_3B_Part7_FollowerProgressiveDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, net := unitTestRaftCluster(t, 3)
	waitSingleLeader(t, rafs, 5*time.Second)

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
	if _, _, ok := leader.Start(101); !ok {
		t.Fatal("Start(101)")
	}
	waitAllServersCommitAtLeast(t, rafs, 1)

	leader1 := -1
	for i, rf := range rafs {
		if _, isL := rf.GetState(); isL {
			leader1 = i
			break
		}
	}
	unitTestDisconnectPeer(net, 3, (leader1+1)%3)
	time.Sleep(RaftElectionTimeout)

	leader = nil
	for _, rf := range rafs {
		if _, isL := rf.GetState(); isL {
			leader = rf
			break
		}
	}
	if leader == nil {
		t.Fatal("no leader after first partition")
	}
	if _, _, ok := leader.Start(102); !ok {
		t.Fatal("Start(102)")
	}
	waitAtLeastNCommit(t, rafs, 2, 2)
	time.Sleep(RaftElectionTimeout)
	if _, _, ok := leader.Start(103); !ok {
		t.Fatal("Start(103)")
	}
	waitAtLeastNCommit(t, rafs, 3, 2)

	leader2 := -1
	for i, rf := range rafs {
		if _, isL := rf.GetState(); isL {
			leader2 = i
			break
		}
	}
	unitTestDisconnectPeer(net, 3, (leader2+1)%3)
	unitTestDisconnectPeer(net, 3, (leader2+2)%3)

	idx, _, ok := rafs[leader2].Start(104)
	if !ok || idx != 4 {
		t.Fatalf("Start(104): idx=%d ok=%v", idx, ok)
	}
	time.Sleep(2 * RaftElectionTimeout)
	assertMaxCommitIndex(t, rafs, 3)
}

// TestPeer_3B_Part7_LeaderChainDisconnect mirrors TestLeaderFailure3B (TDD Phần 7).
func TestPeer_3B_Part7_LeaderChainDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, net := unitTestRaftCluster(t, 3)
	waitSingleLeader(t, rafs, 5*time.Second)

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
	if _, _, ok := leader.Start(101); !ok {
		t.Fatal("Start(101)")
	}
	waitAllServersCommitAtLeast(t, rafs, 1)

	leader1 := -1
	for i, rf := range rafs {
		if _, isL := rf.GetState(); isL {
			leader1 = i
			break
		}
	}
	unitTestDisconnectPeer(net, 3, leader1)
	time.Sleep(2 * RaftElectionTimeout)

	// Must not use the first isLeader in rafs order: the partitioned leader1 can still
	// report leader=true locally (split brain). Only trust leaders among peers != leader1.
	L2 := -1
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, rf := range rafs {
			if i == leader1 {
				continue
			}
			if _, isL := rf.GetState(); isL {
				L2 = i
				break
			}
		}
		if L2 >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if L2 < 0 {
		t.Fatal("expected leader among peers other than partitioned leader")
	}
	if _, _, ok := rafs[L2].Start(102); !ok {
		t.Fatal("Start(102)")
	}
	waitAtLeastNCommit(t, rafs, 2, 2)
	time.Sleep(RaftElectionTimeout)
	if _, _, ok := rafs[L2].Start(103); !ok {
		t.Fatal("Start(103)")
	}
	waitAtLeastNCommit(t, rafs, 3, 2)

	// Same split-brain issue: skip leader1 (still isolated, may claim leader).
	leader2 := -1
	deadline2 := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline2) {
		for i, rf := range rafs {
			if i == leader1 {
				continue
			}
			if _, isL := rf.GetState(); isL {
				leader2 = i
				break
			}
		}
		if leader2 >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if leader2 < 0 {
		t.Fatal("no current leader among connected peers")
	}
	unitTestDisconnectPeer(net, 3, leader2)

	for i := range rafs {
		rafs[i].Start(104)
	}
	time.Sleep(2 * RaftElectionTimeout)
	assertMaxCommitIndex(t, rafs, 3)
}

// findLeaderRaftSkip returns a *Raft that believes it is leader, ignoring peer skip (partition / stale leader).
func findLeaderRaftSkip(rafs []*Raft, skip int) *Raft {
	for i, rf := range rafs {
		if i == skip {
			continue
		}
		if _, isL := rf.GetState(); isL {
			return rf
		}
	}
	return nil
}

// findLeaderIndexSkip returns the index of a peer that believes it is leader (skip < 0 means no skip).
func findLeaderIndexSkip(rafs []*Raft, skip int) int {
	for i, rf := range rafs {
		if skip >= 0 && i == skip {
			continue
		}
		if _, isL := rf.GetState(); isL {
			return i
		}
	}
	return -1
}

// tryStartOnAnyLeaderExceptSkip tries Start on a leader among peers with index != skip.
func tryStartOnAnyLeaderExceptSkip(tb testing.TB, rafs []*Raft, skip int, cmd interface{}) (int, bool) {
	tb.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i, rf := range rafs {
			if i == skip {
				continue
			}
			if _, isL := rf.GetState(); isL {
				idx, _, ok := rf.Start(cmd)
				if ok {
					return idx, true
				}
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return -1, false
}

// tryStartAllPeers mirrors tester one() retry: any leader may accept Start.
func tryStartUntilOk(tb testing.TB, rafs []*Raft, cmd interface{}) bool {
	tb.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		for _, rf := range rafs {
			if _, isL := rf.GetState(); isL {
				if _, _, ok := rf.Start(cmd); ok {
					return true
				}
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return false
}

// tryStartOnAnyLeaderReturnsIndex returns the log index from the first successful Start on a leader.
func tryStartOnAnyLeaderReturnsIndex(tb testing.TB, rafs []*Raft, cmd interface{}) (int, bool) {
	tb.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, rf := range rafs {
			if _, isL := rf.GetState(); isL {
				idx, _, ok := rf.Start(cmd)
				if ok {
					return idx, true
				}
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return -1, false
}

// tryStartLeaderInSubset tries Start on each peer in peerIdx (like Test.one): followers return
// ok=false quickly. Skips stale isolated "leaders" by not calling peers outside peerIdx.
func tryStartLeaderInSubset(tb testing.TB, rafs []*Raft, peerIdx []int, cmd interface{}) (int, bool) {
	tb.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, pi := range peerIdx {
			if pi < 0 || pi >= len(rafs) {
				continue
			}
			rf := rafs[pi]
			if idx, _, ok := rf.Start(cmd); ok {
				return idx, true
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	return -1, false
}

// TestPeer_3B_Part8_FollowerReconnectAgree mirrors TestFailAgree3B (TDD Phần 8).
func TestPeer_3B_Part8_FollowerReconnectAgree(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, net := unitTestRaftCluster(t, 3)
	waitSingleLeader(t, rafs, 5*time.Second)

	if _, _, ok := findLeaderRaftSkip(rafs, -1).Start(101); !ok {
		t.Fatal("Start(101)")
	}
	waitAllServersCommitAtLeast(t, rafs, 1)

	leaderIdx := -1
	for i, rf := range rafs {
		if _, isL := rf.GetState(); isL {
			leaderIdx = i
			break
		}
	}
	if leaderIdx < 0 {
		t.Fatal("no leader")
	}
	partitioned := (leaderIdx + 1) % 3
	unitTestDisconnectPeer(net, 3, partitioned)

	cmds := []int{102, 103, 104, 105}
	for i, cmd := range cmds {
		lf := findLeaderRaftSkip(rafs, partitioned)
		if lf == nil {
			t.Fatalf("no leader (skip %d) before Start(%d)", partitioned, cmd)
		}
		if _, _, ok := lf.Start(cmd); !ok {
			t.Fatalf("Start(%d)", cmd)
		}
		waitAtLeastNCommit(t, rafs, i+2, 2)
		if cmd == 103 {
			time.Sleep(RaftElectionTimeout)
		}
	}

	unitTestReconnectPeer(net, 3, partitioned)
	waitAllServersCommitAtLeast(t, rafs, 5)

	if !tryStartUntilOk(t, rafs, 106) {
		t.Fatal("Start(106) failed on any leader")
	}
	waitAllServersCommitAtLeast(t, rafs, 6)
	time.Sleep(RaftElectionTimeout)
	if !tryStartUntilOk(t, rafs, 107) {
		t.Fatal("Start(107) failed on any leader")
	}
	waitAllServersCommitAtLeast(t, rafs, 7)
}

// TestPeer_3B_Part9_NoQuorumFivePeers mirrors TestFailNoAgree3B (TDD Phần 9).
func TestPeer_3B_Part9_NoQuorumFivePeers(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	const n = 5
	rafs, net := unitTestRaftCluster(t, n)
	waitSingleLeader(t, rafs, 8*time.Second)

	lf := findLeaderRaftSkip(rafs, -1)
	if lf == nil {
		t.Fatal("no leader")
	}
	if _, _, ok := lf.Start(10); !ok {
		t.Fatal("Start(10)")
	}
	waitAllServersCommitAtLeast(t, rafs, 1)

	leader := -1
	for i, rf := range rafs {
		if _, isL := rf.GetState(); isL {
			leader = i
			break
		}
	}
	if leader < 0 {
		t.Fatal("no leader index")
	}
	for _, off := range []int{1, 2, 3} {
		unitTestDisconnectPeer(net, n, (leader+off)%n)
	}

	idx, _, ok := rafs[leader].Start(20)
	if !ok {
		t.Fatal("leader rejected Start(20)")
	}
	if idx != 2 {
		t.Fatalf("expected index 2, got %d", idx)
	}
	time.Sleep(2 * RaftElectionTimeout)
	assertMaxCommitIndex(t, rafs, 1)

	for _, off := range []int{1, 2, 3} {
		unitTestReconnectPeer(net, n, (leader+off)%n)
	}
	time.Sleep(RaftElectionTimeout)

	leader2 := -1
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		found := false
		for i, rf := range rafs {
			if _, isL := rf.GetState(); isL {
				leader2 = i
				found = true
				break
			}
		}
		if found {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if leader2 < 0 {
		t.Fatal("no leader after repair")
	}
	i2, _, ok2 := rafs[leader2].Start(30)
	if !ok2 {
		t.Fatal("Start(30)")
	}
	if i2 < 2 || i2 > 3 {
		t.Fatalf("unexpected index %v (want 2 or 3)", i2)
	}

	idx1000, ok3 := tryStartOnAnyLeaderReturnsIndex(t, rafs, 1000)
	if !ok3 {
		t.Fatal("Start(1000) failed")
	}
	waitAllServersCommitAtLeast(t, rafs, idx1000)
}

// TestPeer_3B_Part10_ConcurrentStarts mirrors TestConcurrentStarts3B (TDD Phần 10).
func TestPeer_3B_Part10_ConcurrentStarts(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, _ := unitTestRaftCluster(t, 3)

	for try := 0; try < 5; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}
		waitSingleLeader(t, rafs, 5*time.Second)

		leader := -1
		for i, rf := range rafs {
			if _, isL := rf.GetState(); isL {
				leader = i
				break
			}
		}
		if leader < 0 {
			t.Fatal("no leader")
		}
		lr := rafs[leader]

		_, term, ok := lr.Start(1)
		if !ok {
			continue
		}

		const iters = 5
		var wg sync.WaitGroup
		idxs := make([]int, iters)
		oks := make([]bool, iters)
		terms := make([]int, iters)
		for i := 0; i < iters; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, tm, ok := lr.Start(100 + i)
				idxs[i] = idx
				terms[i] = tm
				oks[i] = ok
			}(i)
		}
		wg.Wait()

		termChanged := false
		for j := range rafs {
			tnow, _ := rafs[j].GetState()
			if tnow != term {
				termChanged = true
				break
			}
		}
		if termChanged {
			continue
		}

		allOK := true
		for i := 0; i < iters; i++ {
			if !oks[i] || terms[i] != term {
				allOK = false
				break
			}
		}
		if !allOK {
			continue
		}

		seen := make(map[int]struct{})
		for i := 0; i < iters; i++ {
			seen[idxs[i]] = struct{}{}
		}
		if len(seen) != iters {
			t.Fatalf("duplicate indices: %v", idxs)
		}
		for _, v := range idxs {
			if v < 2 || v > 6 {
				t.Fatalf("index %v out of range [2,6]: %v", v, idxs)
			}
		}

		waitAllServersCommitAtLeast(t, rafs, 6)
		return
	}
	t.Fatal("term changed too often (concurrent Start)")
}

// TestPeer_3B_Part11_RejoinPartitionedLeader mirrors TestRejoin3B (TDD Phần 11).
func TestPeer_3B_Part11_RejoinPartitionedLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	rafs, net := unitTestRaftCluster(t, 3)
	waitSingleLeader(t, rafs, 6*time.Second)

	if !tryStartUntilOk(t, rafs, 101) {
		t.Fatal("Start(101)")
	}
	waitAllServersCommitAtLeast(t, rafs, 1)

	leader1 := findLeaderIndexSkip(rafs, -1)
	if leader1 < 0 {
		t.Fatal("no leader")
	}
	unitTestDisconnectPeer(net, 3, leader1)
	rafs[leader1].Start(102)
	rafs[leader1].Start(103)
	rafs[leader1].Start(104)

	time.Sleep(RaftElectionTimeout)
	lf := findLeaderRaftSkip(rafs, leader1)
	if lf == nil {
		t.Fatal("no leader in majority partition")
	}
	idx103, _, ok := lf.Start(103)
	if !ok {
		t.Fatal("Start(103) on majority partition")
	}
	maj103 := make([]int, 0, 2)
	for i := range rafs {
		if i != leader1 {
			maj103 = append(maj103, i)
		}
	}
	waitAtLeastNCommitSubset(t, rafs, maj103, idx103, 2)

	leader2 := findLeaderIndexSkip(rafs, leader1)
	if leader2 < 0 {
		t.Fatal("no leader in majority partition")
	}
	third := -1
	for i := range rafs {
		if i != leader1 && i != leader2 {
			third = i
			break
		}
	}
	if third < 0 {
		t.Fatal("no third peer")
	}
	unitTestDisconnectPeer(net, 3, leader2)
	// Only old leader + third (not leader2): same as tester ConnectOne(leader1) with leader2 disconnected.
	unitTestConnectSubset(net, 3, []int{leader1, third})
	time.Sleep(RaftElectionTimeout)

	lf2 := findLeaderRaftSkip(rafs, leader2)
	if lf2 == nil {
		t.Fatal("no leader after old leader rejoin")
	}
	idx104, _, ok := lf2.Start(104)
	if !ok {
		t.Fatal("Start(104) after old leader rejoin")
	}
	maj104 := make([]int, 0, 2)
	for i := range rafs {
		if i != leader2 {
			maj104 = append(maj104, i)
		}
	}
	waitAtLeastNCommitSubset(t, rafs, maj104, idx104, 2)

	unitTestConnectAll(net, 3)

	idx105, ok := tryStartOnAnyLeaderReturnsIndex(t, rafs, 105)
	if !ok {
		t.Fatal("Start(105)")
	}
	waitAllServersCommitAtLeast(t, rafs, idx105)
}

// TestPeer_3B_Part12_BackupPartitionMerge mirrors TestBackup3B with smaller batches (TDD Phần 12).
func TestPeer_3B_Part12_BackupPartitionMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	const servers = 5
	const batch = 20 // course uses 50; still exercises fast backup / conflict
	rafs, net := unitTestRaftCluster(t, servers)
	waitSingleLeader(t, rafs, 10*time.Second)

	if !tryStartUntilOk(t, rafs, rand.Int()) {
		t.Fatal("initial agreement")
	}
	waitAllServersCommitAtLeast(t, rafs, 1)

	leader1 := findLeaderIndexSkip(rafs, -1)
	if leader1 < 0 {
		t.Fatal("no leader")
	}
	for _, off := range []int{2, 3, 4} {
		unitTestDisconnectPeer(net, servers, (leader1+off)%servers)
	}
	for i := 0; i < batch; i++ {
		rafs[leader1].Start(rand.Int())
	}
	time.Sleep(RaftElectionTimeout / 2)

	unitTestDisconnectPeer(net, servers, (leader1+0)%servers)
	unitTestDisconnectPeer(net, servers, (leader1+1)%servers)

	part2 := []int{(leader1 + 2) % servers, (leader1 + 3) % servers, (leader1 + 4) % servers}
	// Clique on part2 only — do not use unitTestReconnectPeer (would re-link to isolated 0/1).
	unitTestConnectSubset(net, servers, part2)
	waitLeaderInSubset(t, rafs, part2, 15*time.Second)

	for i := 0; i < batch; i++ {
		idx, ok := tryStartLeaderInSubset(t, rafs, part2, rand.Int())
		if !ok {
			t.Fatalf("Start batch2 %d", i)
		}
		waitAtLeastNCommitSubset(t, rafs, part2, idx, 3)
	}

	// Match TestBackup3B: leader2 = checkOneLeader() after batch2 (leadership may have changed).
	leader2 := waitLeaderInSubset(t, rafs, part2, 15*time.Second)
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	unitTestDisconnectPeer(net, servers, other)

	for i := 0; i < batch; i++ {
		rafs[leader2].Start(rand.Int())
	}
	time.Sleep(RaftElectionTimeout / 2)

	for i := 0; i < servers; i++ {
		unitTestDisconnectPeer(net, servers, i)
	}
	part3 := []int{(leader1 + 0) % servers, (leader1 + 1) % servers, other}
	unitTestConnectSubset(net, servers, part3)
	time.Sleep(RaftElectionTimeout / 2)
	waitLeaderInSubset(t, rafs, part3, 15*time.Second)

	var lastIdx3 int
	for i := 0; i < batch; i++ {
		idx, ok := tryStartLeaderInSubset(t, rafs, part3, rand.Int())
		if !ok {
			t.Fatalf("Start batch3 %d", i)
		}
		lastIdx3 = idx
	}
	// One wait at end: per-iteration waits can starve replication under load (idx keeps growing).
	waitAtLeastNCommitSubsetFor(t, rafs, part3, lastIdx3, 3, 120*time.Second)

	unitTestConnectAll(net, servers)
	time.Sleep(RaftElectionTimeout)
	idxFin, ok := tryStartOnAnyLeaderReturnsIndex(t, rafs, rand.Int())
	if !ok {
		t.Fatal("final Start")
	}
	// After full mesh, stragglers need time to align logs (backup test uses large index).
	waitAllServersCommitAtLeastFor(t, rafs, idxFin, 90*time.Second)
}

// TestPeer_3B_Part13_RPCCount mirrors TestCount3B RPC / index checks (TDD Phần 13).
func TestPeer_3B_Part13_RPCCount(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster uses real time")
	}
	t.Parallel()

	const servers = 3
	const iters = 10
	rafs, net := unitTestRaftCluster(t, servers)
	waitSingleLeader(t, rafs, 6*time.Second)

	leader := findLeaderIndexSkip(rafs, -1)
	if leader < 0 {
		t.Fatal("no leader")
	}
	total0 := rpcTotalIncoming(net, servers)
	if total0 > 30 || total0 < 1 {
		t.Fatalf("initial RPC count %d want in [1,30]", total0)
	}

loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}
		leader = findLeaderIndexSkip(rafs, -1)
		if leader < 0 {
			continue
		}
		lr := rafs[leader]
		total1 := rpcTotalIncoming(net, servers)
		starti, term, ok := lr.Start(1)
		if !ok {
			continue
		}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			index1, term1, ok1 := lr.Start(x)
			if term1 != term {
				continue loop
			}
			if !ok1 {
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("wrong index: want %d got %d", starti+i, index1)
			}
		}
		failed := false
		for j := range rafs {
			tnow, _ := rafs[j].GetState()
			if tnow != term {
				failed = true
				break
			}
		}
		if failed {
			continue
		}
		total2 := rpcTotalIncoming(net, servers)
		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs for agreement: %d > %d", total2-total1, (iters+1+3)*3)
		}

		time.Sleep(RaftElectionTimeout)
		total3 := rpcTotalIncoming(net, servers)
		if total3-total2 > 3*20 {
			t.Fatalf("too many idle RPCs: %d", total3-total2)
		}
		return
	}
	t.Fatal("term changed too often (RPC count test)")
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

// waitAllServersCommitAtLeast waits until every peer has commitIndex >= want (3B Part 5/6 helpers).
func waitAllServersCommitAtLeast(tb testing.TB, rafs []*Raft, want int) {
	tb.Helper()
	waitAllServersCommitAtLeastFor(tb, rafs, want, 15*time.Second)
}

func waitAllServersCommitAtLeastFor(tb testing.TB, rafs []*Raft, want int, budget time.Duration) {
	tb.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		ok := true
		for _, rf := range rafs {
			rf.mu.Lock()
			ci := rf.commitIndex
			rf.mu.Unlock()
			if ci < want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	tb.Fatalf("timeout waiting for all servers commitIndex>=%d", want)
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
