package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Value, reply.Version, reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	isRetry := false
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
				isRetry = true
			} else if isRetry && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			} else {
				return reply.Err
			}
		} else {
			isRetry = true
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
