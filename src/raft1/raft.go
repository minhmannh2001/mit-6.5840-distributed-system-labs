package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// RaftRole is this server's role in leader election (Figure 2).
type RaftRole int

const (
	RoleFollower RaftRole = iota
	RoleCandidate
	RoleLeader
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
		rf.votedFor = -1
	}
	rf.role = RoleFollower
}

// lastLogIndex returns index of last log entry (>= 0). Caller must hold rf.mu.
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// lastLogTerm returns term of last log entry. Caller must hold rf.mu.
func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
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
	ms := 300 + rand.Int63n(300)
	rf.electionDeadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term int
}

// AppendEntries RPC reply.
type AppendEntriesReply struct {
	Term    int
	Success bool
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

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked()
	} else {
		reply.VoteGranted = false
	}
}

// AppendEntries RPC handler (heartbeats in 3A; log entries in 3B).
// Valid RPC (args.Term >= currentTerm): step down to follower, reset election timer — leader is alive.
// Stale args.Term: reject so caller learns our term; do not reset the timer.
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
	reply.Success = true
	rf.resetElectionTimerLocked()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
			}
		}(peer)
	}

	if n == 1 {
		rf.mu.Lock()
		if rf.currentTerm == term && rf.role == RoleCandidate {
			rf.role = RoleLeader
		}
		rf.mu.Unlock()
	}
}

// broadcastAppendEntries sends heartbeats to all peers (3A). Does not hold rf.mu across RPCs.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != RoleLeader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	me := rf.me
	n := len(rf.peers)
	rf.mu.Unlock()

	for p := 0; p < n; p++ {
		if p == me {
			continue
		}
		peer := p
		go func() {
			args := &AppendEntriesArgs{Term: term}
			reply := &AppendEntriesReply{}
			if !rf.sendAppendEntries(peer, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
			}
		}()
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
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return -1, rf.currentTerm, rf.role == RoleLeader
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		if role == RoleLeader {
			rf.broadcastAppendEntries()
			// Lab 3A: at most ~10 heartbeats/s → ≥ ~100 ms between rounds.
			time.Sleep(120 * time.Millisecond)
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
			time.Sleep(1 * time.Millisecond)
			continue
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
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

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = RoleFollower
	rf.log = []LogEntry{{Term: 0}}

	// Your initialization code here (3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	rf.resetElectionTimerLocked()
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
