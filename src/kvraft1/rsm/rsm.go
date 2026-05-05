package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1


type Op struct {
	UniqueID int64
	Req      any
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type waitResult struct {
	err rpc.Err
	val any
}

type pendingOp struct {
	uniqueID int64
	ch       chan waitResult
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int
	sm           StateMachine
	nextID       int64             // atomic, unique op ID per RSM instance
	pending      map[int]pendingOp // log index → waiting Submit call
	done         chan struct{}      // closed when reader exits (Raft killed)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]pendingOp),
		done:         make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	go rsm.leaderMonitor()
	return rsm
}

// reader runs in a goroutine, reading committed ops from applyCh and calling DoOp.
// Exits when applyCh is closed (Raft killed).
func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)
		result := rsm.sm.DoOp(op.Req)

		rsm.mu.Lock()
		pw, ok := rsm.pending[msg.CommandIndex]
		delete(rsm.pending, msg.CommandIndex)
		rsm.mu.Unlock()

		if ok {
			if pw.uniqueID == op.UniqueID {
				pw.ch <- waitResult{err: rpc.OK, val: result}
			} else {
				// A different op was committed at this index — we lost leadership.
				pw.ch <- waitResult{err: rpc.ErrWrongLeader}
			}
		}
	}
	close(rsm.done)
	rsm.drainPending(rpc.ErrWrongLeader)
}

// drainPending sends err to all pending Submit callers and clears the map.
// Safe to call from any goroutine. Must NOT hold rsm.mu on entry.
func (rsm *RSM) drainPending(err rpc.Err) {
	rsm.mu.Lock()
	if len(rsm.pending) == 0 {
		rsm.mu.Unlock()
		return
	}
	old := rsm.pending
	rsm.pending = make(map[int]pendingOp)
	rsm.mu.Unlock()
	for _, pw := range old {
		pw.ch <- waitResult{err: err}
	}
}

// leaderMonitor polls Raft every 10ms and drains pending if no longer leader.
func (rsm *RSM) leaderMonitor() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-rsm.done:
			return
		case <-ticker.C:
			_, isLeader := rsm.rf.GetState()
			if !isLeader {
				rsm.drainPending(rpc.ErrWrongLeader)
			}
		}
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Kill shuts down the RSM by killing the underlying Raft instance.
// Raft's applier goroutine will then close applyCh, which causes reader()
// to exit and drain all pending Submit callers with ErrWrongLeader.
func (rsm *RSM) Kill() {
	rsm.rf.Kill()
}


// Submit wraps req in an Op, submits to Raft, and blocks until the op is committed.
// Returns ErrWrongLeader if this server is not the leader.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	id := atomic.AddInt64(&rsm.nextID, 1)
	op := Op{UniqueID: id, Req: req}

	index, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan waitResult, 1)
	rsm.mu.Lock()
	rsm.pending[index] = pendingOp{uniqueID: id, ch: ch}
	rsm.mu.Unlock()

	result := <-ch
	return result.err, result.val
}
