package local

import (
	"github.com/dgraph-io/badger"
)

type badgerStorage struct {
	db *badger.DB
}

// NewBadgerStorage returns a local storage using badger
func NewBadgerStorage(dir string) (Storage, error) {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	opts.ValueLogFileSize = 1024 * 1024 * 10
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &badgerStorage{db: db}, nil
}

func (s *badgerStorage) Set(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *badgerStorage) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}

			return err
		}

		value, err = item.Value()
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *badgerStorage) Remove(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (s *badgerStorage) Range(prefix []byte, limit uint64, fn func([]byte, []byte) bool) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		c := uint64(0)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}

			fn(k, v)
			c++
			if limit > 0 && c >= limit {
				break
			}
		}
		return nil
	})
}
