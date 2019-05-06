package core

import (
	"fmt"

	"github.com/fagongzi/util/json"
	"github.com/garyburd/redigo/redis"
	"github.com/infinivision/taas/pkg/cedis"
	"github.com/infinivision/taas/pkg/meta"
)

type cellQueryAPI struct {
	cell *cedis.Cedis
}

// NewCellQueryAPI returns query API
func NewCellQueryAPI(opts ...cedis.Option) QueryAPI {
	return &cellQueryAPI{
		cell: cedis.NewCedis(opts...),
	}
}

// CountTransactions returns transaction count
func (api *cellQueryAPI) CountTransactions(fid uint64) (uint64, error) {
	conn := api.cell.Get()
	defer conn.Close()

	return redis.Uint64(conn.Do("HLEN", gCellKey(fid)))
}

// Transactions returns a transaction list
func (api *cellQueryAPI) Transactions(fid uint64, query meta.Query) ([]meta.GlobalTransaction, error) {
	conn := api.cell.Get()
	defer conn.Close()

	key := gCellKey(fid)

	start := []byte("")
	if query.After > 0 {
		start = []byte(fmt.Sprintf("%d", query.After))
	}

	var trans []meta.GlobalTransaction
	for {
		ret, err := conn.Do("HSCANGET", key, start, query.Limit)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return trans, nil
		}

		values, err := redis.StringMap(ret, err)
		if err != nil {
			return nil, err
		}

		for key, value := range values {
			if uint64(len(trans)) >= query.Limit {
				return trans, nil
			}

			v := meta.GlobalTransaction{}
			json.MustUnmarshal(&v, []byte(value))

			if query.Filter(&v) {
				trans = append(trans, v)
			}

			start = []byte(key)
		}

		if uint64(len(values)) < query.Limit {
			break
		}
	}

	return trans, nil
}

// Transaction returns the spec transaction
func (api *cellQueryAPI) Transaction(fid uint64, id uint64) (*meta.GlobalTransaction, error) {
	conn := api.cell.Get()
	defer conn.Close()

	ret, err := conn.Do("HGET", gCellKey(fid), id)
	if err != nil {
		return nil, err
	}

	if ret != nil {
		value := &meta.GlobalTransaction{}
		data, err := redis.Bytes(ret, err)
		if err != nil {
			return nil, err
		}

		if len(data) != 0 {
			json.MustUnmarshal(&value, data)
			return value, nil
		}
	}

	return nil, nil
}
