package core

import (
	"github.com/infinivision/taas/pkg/meta"
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
