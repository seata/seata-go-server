package core

import (
	"github.com/fagongzi/util/json"
	"github.com/infinivision/taas/pkg/cedis"
	"github.com/infinivision/taas/pkg/meta"
)

type cellManualAPI struct {
	cell *cedis.Cedis
}

// NewcellManualAPI returns query API
func NewcellManualAPI(opts ...cedis.Option) ManualAPI {
	return &cellManualAPI{
		cell: cedis.NewCedis(opts...),
	}
}

// Commit commit global transaction
func (api *cellManualAPI) Commit(fid, gid uint64) error {
	conn := api.cell.Get()
	defer conn.Close()

	_, err := conn.Do("RPUSH", manualCellKey(fid), json.MustMarshal(&meta.Manual{
		GID:    gid,
		Action: meta.CommitAction,
	}))
	return err
}

// Rollback rollback global transaction
func (api *cellManualAPI) Rollback(fid, gid uint64) error {
	conn := api.cell.Get()
	defer conn.Close()

	_, err := conn.Do("RPUSH", manualCellKey(fid), json.MustMarshal(&meta.Manual{
		GID:    gid,
		Action: meta.RollbackAction,
	}))
	return err
}
