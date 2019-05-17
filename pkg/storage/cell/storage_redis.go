package cell

import (
	"fmt"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/json"
	"github.com/garyburd/redigo/redis"
	"github.com/infinivision/taas/pkg/meta"
)

func (s *Storage) loadFromRedis(key string, query meta.Query, applyFunc func(*meta.GlobalTransaction) error) error {
	return s.doWithRetry(func(conn redis.Conn) error {
		ret, err := conn.Do("HGETALL", key)
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

		count := uint64(0)
		for key, value := range values {
			if count >= query.Limit {
				return nil
			}

			if format.MustParseStrUInt64(string(key)) <= query.After {
				continue
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
		}

		return nil
	})
}

func (s *Storage) lockableFromRedis(resource string, gid uint64, keys ...meta.LockKey) (bool, string, error) {
	if len(keys) == 0 {
		return true, "", nil
	}

	ok := false
	conflict := ""
	err := s.doWithRetry(func(conn redis.Conn) error {
		args := make([]interface{}, 0, len(keys))
		args = append(args, resource)
		for _, key := range keys {
			args = append(args, fmt.Sprintf("%s_%s", resource, key.Value()))
		}

		ret, err := conn.Do("MGET", args...)
		if err != nil {
			return err
		}
		if ret == nil {
			ok = true
			return nil
		}

		values, err := redis.Strings(ret, err)
		if err != nil {
			return err
		}

		for _, value := range values {
			if value != "" && format.MustParseStrUInt64(value) != gid {
				conflict = fmt.Sprintf("conflict with %s", value)
				return nil
			}
		}

		ok = true
		return nil
	})

	return ok, conflict, err
}

func (s *Storage) lockFromRedis(resource string, gid uint64, keys ...meta.LockKey) (bool, string, error) {
	if len(keys) == 0 {
		return true, "", nil
	}

	ok := false
	conflict := ""
	err := s.doWithRetry(func(conn redis.Conn) error {
		args := make([]interface{}, 0, 2*len(keys))
		for _, key := range keys {
			args = append(args, fmt.Sprintf("%s_%s", resource, key.Value()), gid)
		}

		count, err := redis.Int(conn.Do("MSETNX", args...))
		if err != nil {
			return err
		}

		if count != 0 {
			ok = true
		}

		// check is holding by me
		args = make([]interface{}, 0, len(keys))
		for _, key := range keys {
			args = append(args, fmt.Sprintf("%s_%s", resource, key.Value()))
		}
		values, err := redis.Strings(conn.Do("MGET", args...))
		if err != nil {
			return err
		}

		c := 0
		for _, value := range values {
			if value != "" && format.MustParseStrUInt64(value) == gid {
				c++
			}
		}

		ok = c == len(keys)
		return nil
	})

	return ok, conflict, err
}

func (s *Storage) unlockFromRedis(resource string, keys ...meta.LockKey) error {
	if len(keys) == 0 {
		return nil
	}

	return s.doWithRetry(func(conn redis.Conn) error {
		args := make([]interface{}, 0, len(keys))
		for _, key := range keys {
			args = append(args, fmt.Sprintf("%s_%s", resource, key.Value()))
		}

		_, err := conn.Do("DEL", args...)
		return err
	})
}
