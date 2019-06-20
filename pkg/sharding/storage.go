package sharding

import (
	"github.com/fagongzi/util/json"
	"seata.io/server/pkg/local"
	"seata.io/server/pkg/meta"
)

type storage interface {
	get(key []byte) ([]byte, error)
	set(key, value []byte) error

	countFragments() (int, error)
	loadFragments(handleFunc func(value []byte) (uint64, error)) error
	putFragment(frag meta.Fragment) error
	removeFragment(fid uint64) error
}

type defaultStorage struct {
	local local.Storage
}

func newStorage(s local.Storage) *defaultStorage {
	return &defaultStorage{
		local: s,
	}
}

func (s *defaultStorage) loadFragments(handleFunc func(value []byte) (uint64, error)) error {
	return s.local.Range(fragmentsPrefix, 0, func(key, value []byte) bool {
		handleFunc(value)
		return true
	})
}

func (s *defaultStorage) countFragments() (int, error) {
	c := 0
	err := s.local.Range(fragmentsPrefix, 0, func(key, value []byte) bool {
		c++
		return true
	})
	if err != nil {
		return 0, err
	}

	return c, nil
}

func (s *defaultStorage) putFragment(frag meta.Fragment) error {
	return s.set(getFragmentKey(frag.ID), json.MustMarshal(&frag))
}

func (s *defaultStorage) removeFragment(fid uint64) error {
	return nil
}

func (s *defaultStorage) set(key, value []byte) error {
	return s.local.Set(key, value)
}

func (s *defaultStorage) get(key []byte) ([]byte, error) {
	return s.local.Get(key)
}

// just for test
type emptyStorage struct {
}

func (s *emptyStorage) get(key []byte) ([]byte, error) { return nil, nil }
func (s *emptyStorage) set(key, value []byte) error    { return nil }
func (s *emptyStorage) countFragments() (int, error)   { return 0, nil }
func (s *emptyStorage) loadFragments(handleFunc func(value []byte) (uint64, error)) error {
	return nil
}
func (s *emptyStorage) putFragment(frag meta.Fragment) error { return nil }
func (s *emptyStorage) removeFragment(id uint64) error       { return nil }
