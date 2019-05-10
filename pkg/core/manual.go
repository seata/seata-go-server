package core

import (
	"github.com/infinivision/taas/pkg/meta"
	"github.com/infinivision/taas/pkg/storage"
)

// ManualAPI manual api
type ManualAPI interface {
	// Commit commit global transaction
	Commit(fid, gid uint64) error

	// Rollback rollback global transaction
	Rollback(fid, gid uint64) error
}

type manualAPI struct {
	storage storage.Storage
}

// NewManualAPI returns query API
func NewManualAPI(storage storage.Storage) ManualAPI {
	return &manualAPI{
		storage: storage,
	}
}

// Commit commit global transaction
func (api *manualAPI) Commit(fid, gid uint64) error {
	return api.storage.PutManual(fid, &meta.Manual{
		GID:    gid,
		Action: meta.CommitAction,
	})
}

// Rollback rollback global transaction
func (api *manualAPI) Rollback(fid, gid uint64) error {
	return api.storage.PutManual(fid, &meta.Manual{
		GID:    gid,
		Action: meta.RollbackAction,
	})
}
