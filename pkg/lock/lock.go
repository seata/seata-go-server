package lock

import (
	"github.com/infinivision/taas/pkg/meta"
)

// ResourceLock is a lock for resource
type ResourceLock interface {
	// Lock get the lock on the resource
	// If there is conflict, returns false,conflict,nil
	Lock(resource string, gid uint64, locks ...meta.LockKey) (bool, string, error)
	// Unlock release the lock on the resource
	Unlock(resource string, locks ...meta.LockKey) error
	// Lockable returns the key is lockable on resource
	// If there is conflict, returns false,conflict,nil
	Lockable(resource string, gid uint64, locks ...meta.LockKey) (bool, string, error)
}
