package core

import (
	"seata.io/server/pkg/meta"
)

// TransactionCoordinator Seata transaction manager
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

	// HandleEvent process event
	// return false if no event
	HandleEvent() bool

	// HandleManual process manual requests
	HandleManual()

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

// EmptyTransactionCoordinator do nothing TransactionCoordinator
type EmptyTransactionCoordinator struct {
}

// Stop empty implementation
func (tc *EmptyTransactionCoordinator) Stop() {}

// ActiveGCount empty implementation
func (tc *EmptyTransactionCoordinator) ActiveGCount() int { return 0 }

// IsLeader empty implementation
func (tc *EmptyTransactionCoordinator) IsLeader() bool { return false }

// ChangeLeaderTo empty implementation
func (tc *EmptyTransactionCoordinator) ChangeLeaderTo(id uint64) {}

// CurrentLeader empty implementation
func (tc *EmptyTransactionCoordinator) CurrentLeader() (uint64, error) { return 0, nil }

// HandleEvent empty implementation
func (tc *EmptyTransactionCoordinator) HandleEvent() bool { return false }

// HandleManual empty implementation
func (tc *EmptyTransactionCoordinator) HandleManual() {}

// RegistryGlobalTransaction empty implementation
func (tc *EmptyTransactionCoordinator) RegistryGlobalTransaction(value meta.CreateGlobalTransaction, cb func(uint64, error)) {
}

// RegistryBranchTransaction empty implementation
func (tc *EmptyTransactionCoordinator) RegistryBranchTransaction(value meta.CreateBranchTransaction, cb func(uint64, error)) {
}

// ReportBranchTransactionStatus empty implementation
func (tc *EmptyTransactionCoordinator) ReportBranchTransactionStatus(value meta.ReportBranchStatus, cb func(error)) {
}

// GlobalTransactionStatus empty implementation
func (tc *EmptyTransactionCoordinator) GlobalTransactionStatus(gid uint64, cb func(meta.GlobalStatus, error)) {
}

// CommitGlobalTransaction empty implementation
func (tc *EmptyTransactionCoordinator) CommitGlobalTransaction(gid uint64, who string, cb func(meta.GlobalStatus, error)) {
}

// RollbackGlobalTransaction empty implementation
func (tc *EmptyTransactionCoordinator) RollbackGlobalTransaction(gid uint64, who string, cb func(meta.GlobalStatus, error)) {
}

// BranchTransactionNotifyACK empty implementation
func (tc *EmptyTransactionCoordinator) BranchTransactionNotifyACK(ack meta.NotifyACK) {}

// Lockable empty implementation
func (tc *EmptyTransactionCoordinator) Lockable(resource string, gid uint64, lockKeys []meta.LockKey, cb func(bool, error)) {
}
