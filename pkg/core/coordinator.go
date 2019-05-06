package core

import (
	"github.com/infinivision/taas/pkg/meta"
)

// TransactionCoordinator Taas transaction manager
type TransactionCoordinator interface {
	// Stop stop tc
	Stop()

	// ActiveGCount returns number of the actived global transaction
	ActiveGCount() int

	// IsLeader returns true if current node is leader
	IsLeader() bool

	// ChangeLeaderTo change leader to the spec peer
	ChangeLeaderTo(id uint64)

	// CurrentLeader returns the current leader
	CurrentLeader() (uint64, error)

	// RegistryGlobalTransaction registry a global transaction
	RegistryGlobalTransaction(value meta.CreateGlobalTransaction, cb func(uint64, error))

	// RegistryBranchTransaction registry a branch transaction
	RegistryBranchTransaction(value meta.CreateBranchTransaction, cb func(uint64, error))

	// ReportBranchTransactionStatus report branch transaction status, phase one
	ReportBranchTransactionStatus(value meta.ReportBranchStatus, cb func(error))

	// GlobalTransactionStatus return global status
	GlobalTransactionStatus(gid uint64, cb func(meta.GlobalStatus, error))

	// CommitGlobalTransaction commit global
	CommitGlobalTransaction(gid uint64, who string, cb func(meta.GlobalStatus, error))

	// RollbackGlobalTransaction rollback global
	RollbackGlobalTransaction(gid uint64, who string, cb func(meta.GlobalStatus, error))

	// BranchTransactionNotifyACK branch transaction commit/rollback result ack
	BranchTransactionNotifyACK(ack meta.NotifyACK)

	// Lockable returns true if can be locked
	Lockable(resource string, gid uint64, lockKeys []meta.LockKey, cb func(bool, error))
}
