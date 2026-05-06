package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32
	rsm  *rsm.RSM

	mu  sync.Mutex
	kv  map[string]string
	ver map[string]rpc.Tversion
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case rpc.GetArgs:
		v, ok := kv.kv[r.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: v, Version: kv.ver[r.Key], Err: rpc.OK}
	case rpc.PutArgs:
		cur := kv.ver[r.Key]
		if r.Version != cur {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.kv[r.Key] = r.Value
		kv.ver[r.Key] = cur + 1
		return rpc.PutReply{Err: rpc.OK}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here (for Lab 4C)
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here (for Lab 4C)
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, result := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.Submit(*args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = result.(rpc.PutReply)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:  me,
		kv:  make(map[string]string),
		ver: make(map[string]rpc.Tversion),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
