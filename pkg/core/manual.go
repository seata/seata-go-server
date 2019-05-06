package core

// ManualAPI manual api
type ManualAPI interface {
	// Commit commit global transaction
	Commit(fid, gid uint64) error

	// Rollback rollback global transaction
	Rollback(fid, gid uint64) error
}
