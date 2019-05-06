package meta

import (
	"errors"
	"fmt"
)

var (
	// ErrUnknown Unknown err
	ErrUnknown = newError(0)
	// ErrLockKeyConflict LockKeyConflict err
	ErrLockKeyConflict = newError(1)
	// ErrIO io err
	ErrIO = newError(2)
	// ErrBranchRollbackFailedRetryable BranchRollbackFailedRetryable err
	ErrBranchRollbackFailedRetryable = newError(3)
	// ErrBranchRollbackFailedUnretryable BranchRollbackFailedUnRetryable err
	ErrBranchRollbackFailedUnretryable = newError(4)
	// ErrBranchRegisterFailed BranchRegisterFailed err
	ErrBranchRegisterFailed = newError(5)
	// ErrBranchReportFailed BranchReportFailed err
	ErrBranchReportFailed = newError(6)
	// ErrLockableCheckFailed LockableCheckFailed Err
	ErrLockableCheckFailed = newError(7)
	// ErrBranchTransactionNotExist BranchTransactionNotExist err
	ErrBranchTransactionNotExist = newError(8)
	// ErrGlobalTransactionNotExist GlobalTransactionNotExist err
	ErrGlobalTransactionNotExist = newError(9)
	// ErrGlobalTransactionNotActive GlobalTransactionNotActive err
	ErrGlobalTransactionNotActive = newError(10)
	// ErrGlobalTransactionStatusInvalid GlobalTransactionStatusInvalid err
	ErrGlobalTransactionStatusInvalid = newError(11)
	// ErrFailedToSendBranchCommitRequest FailedToSendBranchCommitRequest err
	ErrFailedToSendBranchCommitRequest = newError(12)
	// ErrFailedToSendBranchRollbackRequest FailedToSendBranchRollbackRequest err
	ErrFailedToSendBranchRollbackRequest = newError(13)
	// ErrFailedToAddBranch FailedToAddBranch err
	ErrFailedToAddBranch = newError(14)

	// ErrNotLeader errors is not leader
	ErrNotLeader = errors.New("is not leader")
)

// Error error
type Error struct {
	Code byte
}

func newError(code byte) *Error {
	return &Error{
		Code: code,
	}
}

// NewErrorFrom return err
func NewErrorFrom(code byte) *Error {
	switch code {
	case ErrUnknown.Code:
		return ErrUnknown
	case ErrLockKeyConflict.Code:
		return ErrLockKeyConflict
	case ErrIO.Code:
		return ErrIO
	case ErrBranchRollbackFailedRetryable.Code:
		return ErrBranchRollbackFailedRetryable
	case ErrBranchRollbackFailedUnretryable.Code:
		return ErrBranchRollbackFailedUnretryable
	case ErrBranchRegisterFailed.Code:
		return ErrBranchRegisterFailed
	case ErrBranchReportFailed.Code:
		return ErrBranchReportFailed
	case ErrLockableCheckFailed.Code:
		return ErrLockableCheckFailed
	case ErrBranchTransactionNotExist.Code:
		return ErrBranchTransactionNotExist
	case ErrGlobalTransactionNotExist.Code:
		return ErrGlobalTransactionNotExist
	case ErrGlobalTransactionNotActive.Code:
		return ErrGlobalTransactionNotActive
	case ErrGlobalTransactionStatusInvalid.Code:
		return ErrGlobalTransactionStatusInvalid
	case ErrFailedToSendBranchCommitRequest.Code:
		return ErrFailedToSendBranchCommitRequest
	case ErrFailedToSendBranchRollbackRequest.Code:
		return ErrFailedToSendBranchRollbackRequest
	case ErrFailedToAddBranch.Code:
		return ErrFailedToAddBranch
	default:
		return ErrUnknown
	}
}

// Error error
func (err *Error) Error() string {
	return fmt.Sprintf("error code: %d", err.Code)
}
