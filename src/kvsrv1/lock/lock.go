package lock

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck                  kvtest.IKVClerk
	mu                  sync.Mutex
	lockStateKey        string
	lockStateValue      string
	lockStateKeyVersion rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:           ck,
		lockStateKey: l,
	}
	for {
		err := ck.Put(l, "", 0)
		if err == rpc.OK || err == rpc.ErrVersion {
			break
		}
	}
	return lk
}

func (lk *Lock) Acquire() {
	lk.mu.Lock()
	defer func() {
		lk.mu.Unlock()
	}()
	for {
		randValue := kvtest.RandValue(8)
		state, version, err := lk.ck.Get(lk.lockStateKey)
		if err != rpc.OK {
			panic("lock state key removed")
		}
		if state == "" {
			lk.lockStateValue = randValue
			err = lk.ck.Put(lk.lockStateKey, randValue, version)
			if err == rpc.OK {
				lk.lockStateKeyVersion = version + 1
				break
			}
		} else if state == lk.lockStateValue {
			lk.lockStateKeyVersion = version
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	lk.mu.Lock()
	defer func() {
		lk.mu.Unlock()
	}()

	for {
		state, version, err := lk.ck.Get(lk.lockStateKey)
		_ = version
		if err == rpc.OK {
			if state == "" || state != lk.lockStateValue {
				break
			}
		} else {
			panic("lock state key removed")
		}
		err = lk.ck.Put(lk.lockStateKey, "", lk.lockStateKeyVersion)
		if err == rpc.ErrNoKey {
			panic("lock state key removed")
		}
		if err == rpc.ErrVersion {
			panic("cannot release lock")
		}
		if err == rpc.OK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
