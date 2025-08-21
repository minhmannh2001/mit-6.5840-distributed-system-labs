package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// State represents the state of a Raft peer.
type State int8

const (
	State_Follower State = iota
	State_Candidate
	State_Leader
)

// Raft implements a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers:
	currentTerm int    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int    // candidateId that received vote in current term (or -1 if none)
	log         []byte // log entries; each entry contains command for state machine, and term when entry was received by leader

	// Volatile state on all servers:
	state State

	// Channels for state transitions and events
	recvCh       chan struct{} // Notifies when an AppendEntries RPC is received
	toLeaderCh   chan struct{} // Notifies when a candidate becomes a leader
	toFollowerCh chan struct{} // Notifies when a server should become a follower
	grantVoteCh  chan struct{} // Notifies when a vote is granted
}

// Make creates a new Raft server.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		votedFor:  -1,
	}

	rf.resetChannels()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// GetState returns currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == State_Leader
}

//
// Persistence
//

// persist saves Raft's persistent state to stable storage.
func (rf *Raft) persist() {
	// Your code here (3C).
}

// readPersist restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (3C).
}

// PersistBytes returns the size of the Raft's persisted state.
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

//
// State Transitions
//

// toFollower transitions the server to the Follower state.
func (rf *Raft) toFollower(term int) {
	state := rf.state
	rf.currentTerm = term
	rf.state = State_Follower
	rf.votedFor = -1

	if state != State_Follower {
		sendToChannel(rf.toFollowerCh)
	}

	DPrintf("[%d] Server %d become follower", rf.currentTerm, rf.me)
}

// toCandidate transitions the server to the Candidate state.
func (rf *Raft) toCandidate() {
	if rf.state == State_Leader {
		panic("Leader cannot be candidate")
	}

	rf.resetChannels()

	rf.currentTerm++
	rf.state = State_Candidate
	rf.votedFor = rf.me
}

// toLeader transitions the server to the Leader state.
func (rf *Raft) toLeader() {
	if rf.state != State_Candidate {
		panic("Only candidate can be leader")
	}

	rf.resetChannels()
	rf.state = State_Leader
	DPrintf("[%d] Server %d become leader", rf.currentTerm, rf.me)
}

//
// RPC arguments and replies
//

// AppendEntriesArgs represents the arguments for the AppendEntries RPC.
type AppendEntriesArgs struct {
	Term int
}

// AppendEntriesReply represents the reply for the AppendEntries RPC.
type AppendEntriesReply struct {
	Term int
}

// RequestVoteArgs represents the arguments for the RequestVote RPC.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply represents the reply for the RequestVote RPC.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// RPC handlers
//

// AppendEntries handles the AppendEntries RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	sendToChannel(rf.recvCh)
}

// RequestVote handles the RequestVote RPC.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] Server %d received vote request from %d", rf.currentTerm, rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		DPrintf("[%d] Server %d voted for %d", rf.currentTerm, rf.me, rf.votedFor)
		sendToChannel(rf.grantVoteCh)
	}
}

//
// RPC senders
//

// sendAppendEntries sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// sendRequestVote sends a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

//
// Main logic
//

// ticker is the main loop that drives the Raft state machine.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case State_Follower:
			rf.runAsFollower()
		case State_Candidate:
			rf.runAsCandidate()
		case State_Leader:
			rf.runAsLeader()
		}
	}
}

// runAsFollower implements the logic for the Follower state.
func (rf *Raft) runAsFollower() {
	ms := 300 + (rand.Int63() % 300)
	select {
	case <-rf.grantVoteCh:
	case <-rf.recvCh:
		DPrintf("[%d] Server %d recv heartbeat", rf.currentTerm, rf.me)
	case <-time.After(time.Duration(ms) * time.Millisecond):
		rf.startElection()
	}
}

// runAsCandidate implements the logic for the Candidate state.
func (rf *Raft) runAsCandidate() {
	ms := 300 + (rand.Int63() % 300)
	select {
	case <-rf.toFollowerCh:
		DPrintf("[%d] Server %d from candidate to follower", rf.currentTerm, rf.me)
	case <-rf.toLeaderCh:
		rf.mu.Lock()
		rf.toLeader()
		rf.mu.Unlock()
	case <-time.After(time.Duration(ms) * time.Millisecond):
		rf.startElection()
	}
}

// runAsLeader implements the logic for the Leader state.
func (rf *Raft) runAsLeader() {
	select {
	case <-rf.toFollowerCh:
		DPrintf("[%d] Server %d from leader to follower", rf.currentTerm, rf.me)
	case <-time.After(time.Duration(20) * time.Millisecond):
		rf.mu.Lock()
		args := &AppendEntriesArgs{
			Term: rf.currentTerm,
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				reply := &AppendEntriesReply{}
				if rf.sendAppendEntries(i, args, reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.toFollower(reply.Term)
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

// startElection starts a new election.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("[%d] Server %d start election", rf.currentTerm, rf.me)
	rf.toCandidate()
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	votes := make(chan bool)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(i, args, reply) {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.toFollower(reply.Term)
				}
				rf.mu.Unlock()
				votes <- reply.VoteGranted
			} else {
				votes <- false
			}
		}(i)
	}

	go func() {
		votedCount := 1
		for vote := range votes {
			if vote {
				votedCount++
			}
			if votedCount > len(rf.peers)/2 {
				DPrintf("[%d] Server %d recv vote %d", rf.currentTerm, rf.me, votedCount)
				sendToChannel(rf.toLeaderCh)
				return
			}
		}
	}()
}

//
// Other methods
//

// Start starts the agreement on a new log entry.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (3B).
	return index, term, isLeader
}

// Snapshot snapshots the log up to a given index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// Kill sets the server to a dead state.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed checks if the server is dead.
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// resetChannels resets the event channels.
func (rf *Raft) resetChannels() {
	rf.recvCh = make(chan struct{}, 1)
	rf.toLeaderCh = make(chan struct{}, 1)
	rf.toFollowerCh = make(chan struct{}, 1)
	rf.grantVoteCh = make(chan struct{}, 1)
}

// sendToChannel sends a non-blocking message to a channel.
func sendToChannel(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
