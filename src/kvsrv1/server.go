package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// ValueHandle stores the value and version for a key.
type ValueHandle struct {
	Value   string
	Version rpc.Tversion
}

// KVServer is the key-value server.
type KVServer struct {
	mu   sync.Mutex             // A mutex to protect concurrent access to the data map.
	data map[string]ValueHandle // The in-memory map storing keys to their values and versions.
}

// MakeKVServer creates a new KVServer.
func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.data = make(map[string]ValueHandle)
	return kv
}

// Get is an RPC handler that returns the value and version for a given key.
// It returns ErrNoKey if the key does not exist in the store.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Lock the mutex to ensure exclusive access to the data map.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = rpc.OK
	if value, ok := kv.data[args.Key]; ok {
		// The key exists, populate the reply.
		reply.Value = value.Value
		reply.Version = value.Version
	} else {
		// The key does not exist.
		reply.Err = rpc.ErrNoKey
	}
}

// Put is an RPC handler that conditionally updates the value for a key.
// The update only occurs if the client's provided version matches the server's current version.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Lock the mutex to ensure exclusive access to the data map.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = rpc.OK
	key := args.Key
	if value, ok := kv.data[key]; ok {
		// The key already exists.
		if value.Version != args.Version {
			// The client's version is stale, reject the Put.
			reply.Err = rpc.ErrVersion
		} else {
			// The versions match, perform the update.
			kv.data[key] = ValueHandle{
				Value:   args.Value,
				Version: args.Version + 1, // Increment the version for the new value.
			}
		}
	} else {
		// The key does not exist.
		// A new key can only be created if the client sends version 0.
		if args.Version == 0 {
			kv.data[key] = ValueHandle{
				Value:   args.Value,
				Version: 1, // The first version of a new key is 1.
			}
		} else {
			// The client is trying to update a non-existent key with a non-zero version.
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
