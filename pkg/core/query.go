package core

import (
	"seata.io/server/pkg/meta"
	"seata.io/server/pkg/storage"
)

// QueryAPI query api
type QueryAPI interface {
	// CountTransactions returns transaction count
	CountTransactions(fid uint64) (uint64, error)

	// Transactions returns a transaction list
	Transactions(fid uint64, query meta.Query) ([]meta.GlobalTransaction, error)

	// Transaction returns the spec transaction
	Transaction(fid uint64, gid uint64) (*meta.GlobalTransaction, error)
}

type queryAPI struct {
	storage storage.Storage
}

// NewQueryAPI returns query API
func NewQueryAPI(storage storage.Storage) QueryAPI {
	return &queryAPI{
		storage: storage,
	}
}

// CountTransactions returns transaction count
func (api *queryAPI) CountTransactions(fid uint64) (uint64, error) {
	return api.storage.Count(fid)
}

// Transactions returns a transaction list
func (api *queryAPI) Transactions(fid uint64, query meta.Query) ([]meta.GlobalTransaction, error) {
	var trans []meta.GlobalTransaction
	err := api.storage.Load(fid, query, func(g *meta.GlobalTransaction) error {
		trans = append(trans, *g)
		return nil
	})

	return trans, err
}

// Transaction returns the spec transaction
func (api *queryAPI) Transaction(fid uint64, id uint64) (*meta.GlobalTransaction, error) {
	return api.storage.Get(fid, id)
}
