package mem

import (
	"fmt"
	"math"
	"sync"

	"seata.io/server/pkg/meta"
)

// Storage storage using memory, just for testing
type Storage struct {
	lock sync.RWMutex

	m       map[uint64]*kvTree
	manuals map[uint64][]*meta.Manual
	locks   map[string]uint64 // resource_data:gid
}

// NewStorage returns a transaction storage implementation by memory
func NewStorage() *Storage {
	s := &Storage{
		m:       make(map[uint64]*kvTree),
		manuals: make(map[uint64][]*meta.Manual),
		locks:   make(map[string]uint64),
	}
	return s
}

// Count returns the count of the active global transaction in storage
func (s *Storage) Count(fragmentID uint64) (uint64, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if tree, ok := s.m[fragmentID]; ok {
		return uint64(tree.Count()), nil
	}

	return 0, nil
}

// Get returns the global transaction
func (s *Storage) Get(fragmentID uint64, gid uint64) (*meta.GlobalTransaction, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if tree, ok := s.m[fragmentID]; ok {
		return tree.Get(gid), nil
	}

	return nil, nil
}

// Put puts the global transaction into storage
func (s *Storage) Put(fragmentID uint64, transaction *meta.GlobalTransaction) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.m[fragmentID]; !ok {
		s.m[fragmentID] = newKVTree()
	}

	s.m[fragmentID].Put(transaction.ID, transaction)
	return nil
}

// Remove remove the global transaction from storage
func (s *Storage) Remove(fragmentID uint64, transaction *meta.GlobalTransaction) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if tree, ok := s.m[fragmentID]; ok {
		tree.Delete(transaction.ID)
	}

	return nil
}

// Load load all transaction from storage
func (s *Storage) Load(fragmentID uint64, query meta.Query, applyFunc func(*meta.GlobalTransaction) error) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if tree, ok := s.m[fragmentID]; ok {
		count := uint64(0)
		tree.Scan(query.After, math.MaxUint64, func(key uint64, g *meta.GlobalTransaction) (bool, error) {
			if count >= query.Limit {
				return false, nil
			}

			if query.Filter(g) {
				if err := applyFunc(g); err != nil {
					return false, err
				}

				count++
			}

			return true, nil
		})
	}

	return nil
}

// PutManual put manual action into storage
func (s *Storage) PutManual(fragmentID uint64, manual *meta.Manual) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.manuals[fragmentID] = append(s.manuals[fragmentID], manual)
	return nil
}

// Manual process manual
func (s *Storage) Manual(fragmentID uint64, applyFunc func(*meta.Manual) error) (int, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	c := 0
	if ms, ok := s.manuals[fragmentID]; ok {
		for _, m := range ms {
			err := applyFunc(m)
			if err != nil {
				delete(s.manuals, fragmentID)
				return c, err
			}

			c++
		}
	}

	delete(s.manuals, fragmentID)
	return c, nil
}

// Lock get the lock on the resource
// If there is conflict, returns false,conflict,nil
func (s *Storage) Lock(resource string, gid uint64, locks ...meta.LockKey) (bool, string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, lock := range locks {
		key := fmt.Sprintf("%s_%s", resource, lock.Value())
		if value, ok := s.locks[key]; ok && value != gid {
			return false, fmt.Sprintf("conflict with %d", value), nil
		}
	}

	for _, lock := range locks {
		key := fmt.Sprintf("%s_%s", resource, lock.Value())
		s.locks[key] = gid
	}

	return true, "", nil
}

// Unlock release the lock on the resource
func (s *Storage) Unlock(resource string, locks ...meta.LockKey) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, lock := range locks {
		key := fmt.Sprintf("%s_%s", resource, lock.Value())
		delete(s.locks, key)
	}

	return nil
}

// Lockable returns the key is lockable on resource
// If there is conflict, returns false,conflict,nil
func (s *Storage) Lockable(resource string, gid uint64, locks ...meta.LockKey) (bool, string, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, lock := range locks {
		key := fmt.Sprintf("%s_%s", resource, lock.Value())
		if value, ok := s.locks[key]; ok && value != gid {
			return false, fmt.Sprintf("conflict with %d", value), nil
		}
	}

	return true, "", nil
}
