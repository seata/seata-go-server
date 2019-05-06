package core

import (
	"time"

	"github.com/garyburd/redigo/redis"
)

func (tc *cellTransactionCoordinator) batchLoad(key string, fn func([]byte) error) error {
	conn := tc.cell.Get()
	defer conn.Close()

	start := []byte("")
	for {
		ret, err := conn.Do("HSCANGET", key, start, batch)
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
			err = fn([]byte(value))
			if err != nil {
				return err
			}

			start = []byte(key)
		}

		if len(values) < batch {
			break
		}
	}

	return nil
}

func millisecond(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}
