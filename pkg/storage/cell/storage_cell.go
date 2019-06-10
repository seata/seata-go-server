package cell

import (
	"fmt"

	"github.com/fagongzi/util/json"
	"github.com/garyburd/redigo/redis"
	"seata.io/server/pkg/cedis"
	"seata.io/server/pkg/meta"
)

// Storage storage using cell
type Storage struct {
	opts options
	cell *cedis.Cedis
}

// NewStorage returns a transaction storage implementation by cell
func NewStorage(cell *cedis.Cedis, opts ...Option) *Storage {
	s := &Storage{
		cell: cell,
	}

	for _, opt := range opts {
		opt(&s.opts)
	}

	return s
}

// Count returns the count of the active global transaction in storage
func (s *Storage) Count(fragmentID uint64) (uint64, error) {
	key := gCellKey(fragmentID)
	count := uint64(0)
	err := s.doWithRetry(func(conn redis.Conn) error {
		value, err := redis.Uint64(conn.Do("HLEN", key))
		count = value
		return err
	})

	return count, err
}

// Get returns the global transaction
func (s *Storage) Get(fragmentID uint64, gid uint64) (*meta.GlobalTransaction, error) {
	key := gCellKey(fragmentID)
	var g *meta.GlobalTransaction
	err := s.doWithRetry(func(conn redis.Conn) error {
		ret, err := conn.Do("HGET", key, gid)
		if err != nil {
			return err
		}

		if ret != nil {
			g = &meta.GlobalTransaction{}
			data, err := redis.Bytes(ret, err)
			if err != nil {
				return err
			}

			if len(data) != 0 {
				json.MustUnmarshal(g, data)
			}
		}

		return nil
	})

	return g, err
}

// Put puts the global transaction into elasitcell
func (s *Storage) Put(fragmentID uint64, transaction *meta.GlobalTransaction) error {
	key := gCellKey(fragmentID)
	return s.doWithRetry(func(conn redis.Conn) error {
		_, err := conn.Do("HSET", key, transaction.ID, json.MustMarshal(transaction))
		return err
	})
}

// Remove remove the global transaction from elasitcell
func (s *Storage) Remove(fragmentID uint64, transaction *meta.GlobalTransaction) error {
	key := gCellKey(fragmentID)
	return s.doWithRetry(func(conn redis.Conn) error {
		_, err := conn.Do("HDEL", key, transaction.ID)
		return err
	})
}

// Load get the all global transcations from elasticell
func (s *Storage) Load(fragmentID uint64, query meta.Query, applyFunc func(*meta.GlobalTransaction) error) error {
	key := gCellKey(fragmentID)
	if !s.opts.isCell {
		return s.loadFromRedis(key, query, applyFunc)
	}

	return s.doWithRetry(func(conn redis.Conn) error {
		count := uint64(0)
		start := []byte("")
		if query.After > 0 {
			start = []byte(fmt.Sprintf("%d", query.After))
		}

		for {
			ret, err := conn.Do("HSCANGET", key, start, query.Limit)
			if err != nil {
				return err
			}
			if ret == nil {
				return nil
			}

			values, err := redis.StringMap(ret, err)
			if err != nil {
				return err
			}

			for key, value := range values {
				if count >= query.Limit {
					return nil
				}

				g := &meta.GlobalTransaction{}
				json.MustUnmarshal(g, []byte(value))

				if query.Filter(g) {
					err = applyFunc(g)
					if err != nil {
						return err
					}

					count++
				}

				start = []byte(key)
			}

			if uint64(len(values)) < query.Limit {
				break
			}
		}

		return nil
	})
}

// PutManual put manual action into storage
func (s *Storage) PutManual(fragmentID uint64, manual *meta.Manual) error {
	key := manualCellKey(fragmentID)

	return s.doWithRetry(func(conn redis.Conn) error {
		_, err := conn.Do("RPUSH", key, json.MustMarshal(manual))
		return err
	})
}

// Manual handle manual action
func (s *Storage) Manual(fragmentID uint64, applyFunc func(*meta.Manual) error) (int, error) {
	key := manualCellKey(fragmentID)

	count := 0
	conn := s.cell.Get()
	defer conn.Close()

	for {
		value, err := conn.Do("LRANGE", key, 0, 0)
		if err != nil {
			return 0, err
		}

		data, _ := redis.ByteSlices(value, err)
		if len(data) == 0 {
			break
		}

		manual := &meta.Manual{}
		json.MustUnmarshal(manual, data[0])
		err = applyFunc(manual)
		if err != nil {
			return count, err
		}

		conn.Do("LPOP", key)
		count++
	}

	return count, nil
}

// Lockable returns the key is lockable on resource
func (s *Storage) Lockable(resource string, gid uint64, keys ...meta.LockKey) (bool, string, error) {
	if !s.opts.isCell {
		return s.lockableFromRedis(resource, gid, keys...)
	}

	if len(keys) == 0 {
		return true, "", nil
	}

	ok := false
	conflict := ""
	err := s.doWithRetry(func(conn redis.Conn) error {
		args := make([]interface{}, 0, len(keys)+2)
		args = append(args, resource)
		args = append(args, gid)
		for _, key := range keys {
			args = append(args, key.Value())
		}

		rsp, err := redis.String(conn.Do("LOCKABLE", args...))
		if err != nil {
			return err
		}

		ok = rsp == "OK"
		conflict = rsp
		return nil
	})

	return ok, conflict, err
}

// Lock get the lock on the resource
func (s *Storage) Lock(resource string, gid uint64, keys ...meta.LockKey) (bool, string, error) {
	if !s.opts.isCell {
		return s.lockFromRedis(resource, gid, keys...)
	}

	if len(keys) == 0 {
		return true, "", nil
	}

	ok := false
	conflict := ""
	err := s.doWithRetry(func(conn redis.Conn) error {
		args := make([]interface{}, 0, len(keys)+2)
		args = append(args, resource)
		args = append(args, gid)
		for _, key := range keys {
			args = append(args, key.Value())
		}

		rsp, err := redis.String(conn.Do("LOCK", args...))

		if err != nil {
			return err
		}

		ok = rsp == "OK"
		conflict = rsp
		return nil
	})

	return ok, conflict, err
}

// Unlock release the lock on the resource
func (s *Storage) Unlock(resource string, keys ...meta.LockKey) error {
	if !s.opts.isCell {
		return s.unlockFromRedis(resource, keys...)
	}

	if len(keys) == 0 {
		return nil
	}

	return s.doWithRetry(func(conn redis.Conn) error {
		args := make([]interface{}, 0, len(keys)+1)
		args = append(args, resource)
		for _, key := range keys {
			args = append(args, key.Value())
		}

		_, err := conn.Do("UNLOCK", args...)
		return err
	})
}

func (s *Storage) doWithRetry(doFunc func(conn redis.Conn) error) error {
	times := 0

	for {
		conn := s.cell.Get()
		err := doFunc(conn)
		conn.Close()

		if err == nil {
			break
		}

		if times >= s.opts.maxRetryTimes {
			return err
		}
		times++
	}

	return nil
}
