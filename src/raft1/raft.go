package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// RaftRole is this server's role in leader election (Figure 2).
type RaftRole int

const (
	RoleFollower RaftRole = iota
	RoleCandidate
	RoleLeader
)

// Timing and tuning (3A). Keep magic numbers here instead of scattered in methods.
const (
	// electionTimeout is chosen uniformly from [base, base+range) milliseconds.
	electionTimeoutBaseMs  int64 = 300
	electionTimeoutRangeMs int64 = 300

	// heartbeatInterval: lab caps heartbeats at ~10/s → interval ≥ ~100 ms.
	heartbeatInterval = 120 * time.Millisecond

	// ticker sleeps when not leader: coarse poll for election deadline.
	tickerFollowerPollSleep = 10 * time.Millisecond
	// brief yield after startElection so vote RPCs can complete before next tick.
	tickerPostElectionSleep = time.Millisecond

	// applier waits when no new commits (lab: avoid busy-loop).
	applierPollSleep = 10 * time.Millisecond

	noVote = -1 // rf.votedFor when this server has not voted in currentTerm

	// RPC names registered with labrpc (must match handler methods).
	rpcRequestVote   = "Raft.RequestVote"
	rpcAppendEntries = "Raft.AppendEntries"

	// Start() returns this index when not leader.
	startIndexNotReady = -1
)

// LogEntry is one Raft log entry (Figure 2). Index 0 is a dummy entry (term 0).
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Election-related state (3A+); see Figure 2.
	currentTerm int      // latest term this server has seen
	votedFor    int      // candidateId that received vote in current term, or -1 if none
	role        RaftRole // follower, candidate, or leader

	// log[0] is unused dummy; real entries start at 1 (3B+).
	log []LogEntry

	// 3D: last included index/term for compacted prefix; (0,0) = not compacted yet.
	// Logical log index i is stored at rf.log[i-snapLastIdx].
	snapLastIdx  int
	snapLastTerm int
	snapshot     []byte

	// Volatile state on all servers (Figure 2).
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders; reinitialized after election (Figure 2).
	nextIndex  []int // for each server, index of next log entry to send
	matchIndex []int // for each server, index of highest log entry known replicated

	// electionDeadline is when a follower/candidate may start a new election (3A ticker).
	electionDeadline time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == RoleLeader
}

// becomeFollower adopts a term >= current term and becomes follower (Figure 2).
// If newTerm > currentTerm, updates currentTerm and clears votedFor.
// If newTerm == currentTerm, only demotes to follower (e.g. valid heartbeat).
// If newTerm < currentTerm, does nothing (callers should reject stale RPCs first).
// Caller must hold rf.mu.
func (rf *Raft) becomeFollower(newTerm int) {
	if newTerm < rf.currentTerm {
		return
	}
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = noVote
		rf.persist()
	}
	rf.role = RoleFollower
}

// firstLogIndex is the smallest logical index still present in rf.log[1:] (3D).
// Caller must hold rf.mu.
func (rf *Raft) firstLogIndex() int {
	return rf.snapLastIdx + 1
}

// logTerm returns the term of the log entry at logical index logical.
// At snapLastIdx it returns snapLastTerm (entry may only exist in snapshot). Caller must hold rf.mu.
func (rf *Raft) logTerm(logical int) int {
	if logical < rf.snapLastIdx {
		return 0
	}
	if logical == rf.snapLastIdx {
		return rf.snapLastTerm
	}
	phy := logical - rf.snapLastIdx
	if phy < 0 || phy >= len(rf.log) {
		return 0
	}
	return rf.log[phy].Term
}

// lastLogIndex returns the logical index of the last log entry (>= 0). Caller must hold rf.mu.
func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.snapLastIdx
	}
	return rf.snapLastIdx + len(rf.log) - 1
}

// lastLogTerm returns the term of the last log entry. Caller must hold rf.mu.
func (rf *Raft) lastLogTerm() int {
	return rf.logTerm(rf.lastLogIndex())
}

// isLogUpToDate reports whether a candidate's log is at least as current as ours (§5.4.1).
// Caller must hold rf.mu.
func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastTerm := rf.lastLogTerm()
	myLastIdx := rf.lastLogIndex()
	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIdx
}

// resetElectionTimerLocked picks a new random election deadline. Caller must hold rf.mu.
func (rf *Raft) resetElectionTimerLocked() {
	ms := electionTimeoutBaseMs + rand.Int63n(electionTimeoutRangeMs)
	rf.electionDeadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

// initLeaderReplicationLocked sets nextIndex and matchIndex after winning election (Figure 2).
// Caller must hold rf.mu; rf.role should already be RoleLeader.
func (rf *Raft) initLeaderReplicationLocked() {
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	next := rf.lastLogIndex() + 1
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
	}
}

// advanceCommitIndexLocked sets commitIndex to the largest N > commitIndex such that
// a strict majority of servers (counting the leader) have replicated N and log[N].Term == currentTerm.
// Caller must hold rf.mu; only meaningful for RoleLeader.
func (rf *Raft) advanceCommitIndexLocked() {
	if rf.role != RoleLeader {
		return
	}
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.logTerm(n) != rf.currentTerm {
			continue
		}
		cnt := 1 // this server has the entry
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= n {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = n
			return
		}
	}
}

// applier sends committed entries to applyCh (Figure 2). Never holds rf.mu while sending.
func (rf *Raft) applier(applyCh chan raftapi.ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			time.Sleep(applierPollSleep)
			continue
		}
		rf.lastApplied++
		idx := rf.lastApplied
		phy := idx - rf.snapLastIdx
		cmd := rf.log[phy].Command
		rf.mu.Unlock()

		applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: idx,
		}
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapLastIdx)
	e.Encode(rf.snapLastTerm)
	raftstate := w.Bytes()
	snapshot := rf.snapshot
	if snapshot == nil {
		snapshot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voted int
	var log []LogEntry
	snapLastIdx := 0
	snapLastTerm := 0
	if d.Decode(&term) != nil ||
		d.Decode(&voted) != nil ||
		d.Decode(&log) != nil {
		return
	}
	// Backward-compatible with old 3C blobs that encoded only term/vote/log.
	if d.Decode(&snapLastIdx) != nil || d.Decode(&snapLastTerm) != nil {
		snapLastIdx = 0
		snapLastTerm = 0
	}
	// Log is always 1-indexed with a dummy at index 0 (term 0). Reject malformed blobs so
	// Make()'s defaults are kept instead of panicking in lastLogTerm / replication.
	if len(log) < 1 || log[0].Term != 0 {
		return
	}
	rf.currentTerm = term
	rf.votedFor = voted
	rf.log = log
	rf.snapLastIdx = snapLastIdx
	rf.snapLastTerm = snapLastTerm
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// RequestVote RPC arguments (Figure 2).
// Field names must start with capital letters for labrpc.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntries RPC arguments (heartbeat / log replication; Figure 2).
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int // unused in minimal 3B; reserved for follower redirects / tests
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply.
type AppendEntriesReply struct {
	Term    int
	Success bool
	// Used when Success is false and Term matches leader's term: speed up nextIndex.
	// ConflictTerm < 0: follower's log is shorter than PrevLogIndex; set nextIndex to ConflictIndex.
	// Otherwise: first index in follower's log with term ConflictTerm; leader may jump nextIndex.
	ConflictTerm  int
	ConflictIndex int
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == noVote || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked()
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
}

// AppendEntries RPC handler (Figure 2): log replication + heartbeat on follower.
// Stale term: reject without resetting election timer.
// Prev log mismatch: reject without resetting election timer.
// Success: truncate conflicting suffix, append entries, advance commitIndex, reset election timer.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.becomeFollower(args.Term)
	reply.Term = rf.currentTerm
	// Reset election timer for any valid leader RPC (term >= currentTerm), even if the
	// consistency check below fails. Prevents unnecessary elections during log resync.
	rf.resetElectionTimerLocked()

	// Too short / invalid prev: same logical ConflictIndex as old code (len(rf.log)) when snapLastIdx==0.
	if args.PrevLogIndex < 0 || args.PrevLogIndex > rf.lastLogIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	// Prev falls before first retained index (3D): leader needs InstallSnapshot later.
	if args.PrevLogIndex < rf.snapLastIdx {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.firstLogIndex()
		return
	}
	if rf.logTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		phy := args.PrevLogIndex - rf.snapLastIdx
		ct := rf.log[phy].Term
		reply.ConflictTerm = ct
		idx := phy
		for idx > 0 && rf.log[idx-1].Term == ct {
			idx--
		}
		reply.ConflictIndex = rf.snapLastIdx + idx
		return
	}

	// Install leader entries (Figure 2 §5.3, rules 3–4):
	//   - If an existing entry conflicts with a new one (same index, different term): delete it
	//     and all that follow, then append the remaining new entries.
	//   - If the new entry already exists with the same term, skip it (no-op).
	//   - Only truncate on an actual conflict — never truncate a matching suffix, because a
	//     stale (reordered) AE carrying a subset of already-installed entries would otherwise
	//     remove committed entries that were installed by a later AE in the same term.
	if len(args.Entries) > 0 {
		for j, entry := range args.Entries {
			logical := args.PrevLogIndex + 1 + j
			phy := logical - rf.snapLastIdx
			if phy >= len(rf.log) {
				// Past the end of our log: append remaining entries and done.
				rf.log = append(rf.log, args.Entries[j:]...)
				break
			}
			if rf.log[phy].Term != entry.Term {
				// Conflict: truncate at divergence point, append remaining entries.
				rf.log = append(rf.log[:phy], args.Entries[j:]...)
				break
			}
			// Entry already present with matching term: skip.
		}
	}

	// Rule 5 (Figure 2): advance commitIndex to min(leaderCommit, index of last new entry).
	// "Last new entry" is prevLogIndex + len(entries) — the highest index whose log-matching
	// property has been verified by this AE's term check at prevLogIndex. We MUST NOT use
	// lastLogIndex() here: the follower may have unverified stale entries from old leaders
	// beyond prevLogIndex, and advancing commitIndex into them would be a safety violation.
	lastNewEntry := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex && lastNewEntry > rf.commitIndex {
		c := args.LeaderCommit
		if c > lastNewEntry {
			c = lastNewEntry
		}
		rf.commitIndex = c
	}

	reply.Success = true
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if server < 0 || server >= len(rf.peers) || rf.peers[server] == nil {
		return false
	}
	ok := rf.peers[server].Call(rpcRequestVote, args, reply)
	return ok
}

// sendAppendEntries sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if server < 0 || server >= len(rf.peers) || rf.peers[server] == nil {
		return false
	}
	ok := rf.peers[server].Call(rpcAppendEntries, args, reply)
	return ok
}

// startElection begins a new election if the election deadline has passed (Figure 2).
// Does not hold rf.mu across RPCs.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.role == RoleLeader {
		rf.mu.Unlock()
		return
	}
	if !time.Now().After(rf.electionDeadline) {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm++
	rf.role = RoleCandidate
	rf.votedFor = rf.me
	rf.resetElectionTimerLocked()
	rf.persist()

	term := rf.currentTerm
	lastIdx := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	n := len(rf.peers)
	me := rf.me
	rf.mu.Unlock()

	var votes int32 = 1
	for peer := 0; peer < n; peer++ {
		if peer == me {
			continue
		}
		go func(p int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(p, args, reply) {
				return
			}
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.currentTerm != term || rf.role != RoleCandidate {
				return
			}
			if !reply.VoteGranted {
				return
			}
			if atomic.AddInt32(&votes, 1) > int32(n/2) {
				rf.role = RoleLeader
				rf.initLeaderReplicationLocked()
			}
		}(peer)
	}

	if n == 1 {
		rf.mu.Lock()
		if rf.currentTerm == term && rf.role == RoleCandidate {
			rf.role = RoleLeader
			rf.initLeaderReplicationLocked()
		}
		rf.mu.Unlock()
	}
}

// broadcastAppendEntries sends AppendEntries (heartbeats and/or log entries) to every other peer.
// Builds PrevLogIndex/PrevLogTerm/Entries/LeaderCommit from nextIndex and log; does not hold rf.mu across Call.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	if rf.role != RoleLeader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	me := rf.me
	n := len(rf.peers)
	lc := rf.commitIndex

	// Leader must have replication arrays (set in initLeaderReplicationLocked). Unit tests may
	// set RoleLeader on isolated Raft without init; skip RPCs instead of panicking.
	if rf.nextIndex == nil || len(rf.nextIndex) != n || rf.matchIndex == nil || len(rf.matchIndex) != n {
		rf.mu.Unlock()
		return
	}

	type aeSnap struct {
		peer int
		args *AppendEntriesArgs
	}
	var snaps []aeSnap
	for p := 0; p < n; p++ {
		if p == me {
			continue
		}
		next := rf.nextIndex[p]
		first := rf.firstLogIndex()
		if next < first {
			next = first
		}
		prev := next - 1
		prevTerm := rf.logTerm(prev)
		// Incremental replication (3B Part 6 / TestRPCBytes3B): only append suffix log[next:],
		// not the full log each heartbeat — nextIndex advances after successful AppendEntries.
		phyNext := next - rf.snapLastIdx
		if phyNext < 1 {
			phyNext = 1
		}
		rest := len(rf.log) - phyNext
		if rest < 0 {
			rest = 0
		}
		ents := make([]LogEntry, rest)
		copy(ents, rf.log[phyNext:])
		snaps = append(snaps, aeSnap{p, &AppendEntriesArgs{
			Term:         term,
			PrevLogIndex: prev,
			PrevLogTerm:  prevTerm,
			Entries:      ents,
			LeaderCommit: lc,
		}})
	}
	rf.mu.Unlock()

	for _, s := range snaps {
		peer := s.peer
		args := s.args
		go func() {
			reply := &AppendEntriesReply{}
			if !rf.sendAppendEntries(peer, args, reply) {
				return
			}
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if rf.currentTerm != term || rf.role != RoleLeader {
				return
			}
			if reply.Success {
				last := args.PrevLogIndex + len(args.Entries)
				if peer >= 0 && peer < len(rf.matchIndex) && last > rf.matchIndex[peer] {
					rf.matchIndex[peer] = last
					rf.nextIndex[peer] = last + 1
					rf.advanceCommitIndexLocked()
				}
			} else if reply.Term == rf.currentTerm {
				rf.appendEntriesFailureUpdateNextIndexLocked(peer, args, reply)
			}
		}()
	}
}

// appendEntriesFailureUpdateNextIndexLocked applies the extended-Raft log-backup optimization
// (paper ~p.7–8, lab XTerm/XIndex/XLen): follower log too short, or term mismatch at prev.
// Ignores stale replies where nextIndex[peer] no longer matches this RPC (out-of-order AE).
// Caller must hold rf.mu.
func (rf *Raft) appendEntriesFailureUpdateNextIndexLocked(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if peer >= len(rf.nextIndex) || rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}
	// Case 3 (follower log shorter than PrevLogIndex): ConflictTerm < 0, ConflictIndex = len(log).
	// Case 1 (leader has no ConflictTerm): nextIndex = ConflictIndex (first index of that term on follower).
	// Case 2 (leader has ConflictTerm): nextIndex = index after leader's last entry in that term.
	if reply.ConflictTerm < 0 {
		rf.nextIndex[peer] = reply.ConflictIndex
		if rf.nextIndex[peer] < rf.firstLogIndex() {
			rf.nextIndex[peer] = rf.firstLogIndex()
		}
		return
	}
	lastSamePhy := 0
	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term == reply.ConflictTerm {
			lastSamePhy = i
			break
		}
	}
	if lastSamePhy > 0 {
		rf.nextIndex[peer] = rf.snapLastIdx + lastSamePhy + 1
	} else {
		rf.nextIndex[peer] = reply.ConflictIndex
	}
	if rf.nextIndex[peer] < rf.firstLogIndex() {
		rf.nextIndex[peer] = rf.firstLogIndex()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.killed() {
		t := rf.currentTerm
		rf.mu.Unlock()
		return startIndexNotReady, t, false
	}
	if rf.role != RoleLeader {
		t := rf.currentTerm
		rf.mu.Unlock()
		return startIndexNotReady, t, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	idx := rf.lastLogIndex()
	t := rf.currentTerm
	rf.advanceCommitIndexLocked()
	rf.persist()
	rf.mu.Unlock()

	go rf.broadcastAppendEntries()

	return idx, t, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// sleepUntilOrKilled sleeps for at most d but returns early if Kill() was called.
// Lets ticker exit promptly instead of blocking a full heartbeat interval on Sleep.
func (rf *Raft) sleepUntilOrKilled(d time.Duration) {
	if d <= 0 {
		return
	}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if rf.killed() {
			return
		}
		rem := time.Until(deadline)
		step := tickerFollowerPollSleep
		if rem < step {
			step = rem
		}
		if step > 0 {
			time.Sleep(step)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		if role == RoleLeader {
			rf.broadcastAppendEntries()
			rf.sleepUntilOrKilled(heartbeatInterval)
			continue
		}

		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if time.Now().After(rf.electionDeadline) {
			rf.mu.Unlock()
			rf.startElection()
			rf.sleepUntilOrKilled(tickerPostElectionSleep)
			continue
		}
		rf.mu.Unlock()

		rf.sleepUntilOrKilled(tickerFollowerPollSleep)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	labgob.Register(LogEntry{})
	labgob.Register(RequestVoteArgs{})
	labgob.Register(RequestVoteReply{})
	labgob.Register(AppendEntriesArgs{})
	labgob.Register(AppendEntriesReply{})
	// Commands in Start()/log use interface{}; register concrete types tests use.
	labgob.Register(0)
	labgob.Register("")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Initialization order (3C): set in-memory defaults first—including log[0] dummy—then
	// overlay durable state from Persister. readPersist must not run before defaults: an empty
	// or corrupt blob is a no-op, and we still need a valid dummy entry for Figure-2 indexing.
	rf.currentTerm = 0
	rf.votedFor = noVote
	rf.role = RoleFollower
	rf.log = []LogEntry{{Term: 0}}
	rf.snapLastIdx = 0
	rf.snapLastTerm = 0
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Your initialization code here (3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier(applyCh)

	return rf
}
