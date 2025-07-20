package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	ck    kvtest.IKVClerk
	key   string // The key in the k/v store that represents the lock
	value string // The unique value for this lock holder
}

// MakeLock creates a new lock instance.
// It initializes the lock key in the store if it doesn't exist.
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:    ck,
		key:   l,
		value: kvtest.RandValue(8), // A unique ID for this client's lock instance
	}

	// Attempt to initialize the lock key in the store.
	// It's okay if it already exists (ErrVersion).
	// We loop in case of network errors.
	for {
		_, _, err := ck.Get(l)
		if err == rpc.ErrNoKey {
			// Key doesn't exist, try to create it.
			err = ck.Put(l, "", 0)
			if err == rpc.OK || err == rpc.ErrVersion {
				break // Success or another client created it.
			}
		} else if err == rpc.OK {
			// Key already exists, which is fine.
			break
		}
		// On ErrMaybe or other transient errors, just retry.
		time.Sleep(10 * time.Millisecond)
	}

	return lk
}

// Acquire attempts to acquire the lock, blocking until successful.
// It implements a test-and-set spinlock.
func (lk *Lock) Acquire() {
	for {
		// Test: Read the current lock state.
		state, version, err := lk.ck.Get(lk.key)

		// If the lock is free (state is empty), try to set it.
		if err == rpc.OK && state == "" {
			// Set: Attempt to write our unique value into the lock key.
			// This is an atomic test-and-set operation.
			putErr := lk.ck.Put(lk.key, lk.value, version)
			if putErr == rpc.OK {
				// Success! We acquired the lock.
				return
			}
			// If putErr is ErrVersion, another client got the lock first.
			// If putErr is ErrMaybe, we might have the lock. The loop will
			// re-check on the next iteration.
		} else if err == rpc.OK && state == lk.value {
			// We already hold the lock (e.g., from a previous attempt
			// where the reply was lost).
			return
		}

		// Wait before retrying to avoid busy-spinning.
		time.Sleep(10 * time.Millisecond)
	}
}

// Release releases the lock.
// It only succeeds if this client is the current lock holder.
func (lk *Lock) Release() {
	for {
		state, version, err := lk.ck.Get(lk.key)

		// Only release the lock if we are the one holding it.
		if err == rpc.OK && state == lk.value {
			putErr := lk.ck.Put(lk.key, "", version)
			if putErr == rpc.OK || putErr == rpc.ErrVersion {
				// Success, or someone else already took the lock after us.
				// In either case, we are no longer the holder.
				return
			}
			// On ErrMaybe, loop and retry to ensure it's released.
		} else if err == rpc.OK {
			// We are not the holder, so there's nothing to do.
			return
		}

		// On Get errors, wait and retry.
		time.Sleep(10 * time.Millisecond)
	}
}
