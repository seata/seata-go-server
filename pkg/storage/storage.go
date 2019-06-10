package storage

import (
	"seata.io/server/pkg/meta"
)

// Storage meta storage
type Storage interface {
	// Count returns the count of the active global transaction in storage
	Count(fragmentID uint64) (uint64, error)
	// Get returns the global transaction
	Get(fragmentID uint64, gid uint64) (*meta.GlobalTransaction, error)
	// Put puts the global transaction into storage
	Put(fragmentID uint64, transaction *meta.GlobalTransaction) error
	// Remove remove the global transaction from storage
	Remove(fragmentID uint64, transaction *meta.GlobalTransaction) error
	// Load load all transaction from storage
	Load(fragmentID uint64, query meta.Query, applyFunc func(*meta.GlobalTransaction) error) error

	// PutManual put manual action into storage
	PutManual(fragmentID uint64, manual *meta.Manual) error
	// Manual process manual
	Manual(fragmentID uint64, applyFunc func(*meta.Manual) error) (int, error)

	// Lock get the lock on the resource
	// If there is conflict, returns false,conflict,nil
	Lock(resource string, gid uint64, locks ...meta.LockKey) (bool, string, error)
	// Unlock release the lock on the resource
	Unlock(resource string, locks ...meta.LockKey) error
	// Lockable returns the key is lockable on resource
	// If there is conflict, returns false,conflict,nil
	Lockable(resource string, gid uint64, locks ...meta.LockKey) (bool, string, error)
}
