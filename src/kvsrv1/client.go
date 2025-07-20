package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

// Clerk is a client for the key-value server.
type Clerk struct {
	clnt   *tester.Clnt // The underlying RPC client connection manager.
	server string       // The name of the server to connect to.
}

// MakeClerk creates a new Clerk.
func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.
// It returns ErrNoKey if the key does not exist.
// It keeps retrying forever in the face of all other errors (e.g., network failures).
// This implementation provides at-least-once semantics for Get.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	// Keep retrying until the RPC call is successful.
	for {
		// ok is false if the network simulation drops the request or reply.
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			// The RPC was successful, break the loop and return the reply.
			break
		}
		// Wait a bit before retrying to avoid busy-spinning.
		time.Sleep(100 * time.Millisecond)
	}
	return reply.Value, reply.Version, reply.Err
}

// Put updates a key with a new value, but only if the provided version matches
// the server's current version for that key. This is a conditional Put.
// This implementation provides at-most-once semantics.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	retries := 0
	for {
		// Attempt the RPC call.
		if ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply); ok {
			// The RPC was successful (we received a reply).

			// This is the core logic for at-most-once semantics.
			// If this is a retry (retries > 0) and the server returns ErrVersion,
			// it means our original request *might* have succeeded, but its reply was lost.
			// The server processed the original request, incremented the version,
			// and now correctly rejects our retry because the version is stale.
			// Since we don't know if the original succeeded, we must return ErrMaybe.
			if retries > 0 && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			// Otherwise, return the server's definitive response.
			return reply.Err
		}
		// The RPC failed (e.g., network drop), increment retry count and wait.
		retries++
		time.Sleep(100 * time.Millisecond)
	}
}
