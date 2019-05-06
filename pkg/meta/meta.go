package meta

import (
	"bytes"
	"fmt"
	"time"

	"github.com/fagongzi/log"
)

// Action transaction status
type Action int

const (
	// NoneAction none action
	NoneAction = Action(0)
	// RollbackAction rollback action
	RollbackAction = Action(1)
	// CommitAction commit action
	CommitAction = Action(2)
)

// BranchType branch type
type BranchType byte

var (
	// AT auto transaction
	AT = BranchType(0)
	// TCC manual transaction
	TCC = BranchType(1)
)

// Name branch type name
func (t *BranchType) Name() string {
	switch *t {
	case AT:
		return "at"
	case TCC:
		return "tcc"
	}

	return "unknown"
}

// BranchStatus branch transaction status
type BranchStatus byte

var (
	// BranchStatusUnknown Unknown
	BranchStatusUnknown = BranchStatus(0)
	// BranchStatusRegistered Registered to TC
	BranchStatusRegistered = BranchStatus(1)
	// BranchStatusPhaseOneDone Branch logic is successfully done at phase one
	BranchStatusPhaseOneDone = BranchStatus(2)
	// BranchStatusPhaseOneFailed Branch logic is failed at phase one
	BranchStatusPhaseOneFailed = BranchStatus(3)
	// BranchStatusPhaseOneTimeout Branch logic is NOT reported for a timeout
	BranchStatusPhaseOneTimeout = BranchStatus(4)
	// BranchStatusPhaseTwoCommitted Commit logic is succeed done at phase two
	BranchStatusPhaseTwoCommitted = BranchStatus(5)
	// BranchStatusPhaseTwoCommitFailedRetriable Commit logic is failed but retriable.
	BranchStatusPhaseTwoCommitFailedRetriable = BranchStatus(6)
	// BranchStatusPhaseTwoCommitFailedUnretriable Commit logic is failed and NOT retryable.
	BranchStatusPhaseTwoCommitFailedUnretriable = BranchStatus(7)
	// BranchStatusPhaseTwoRollbacked logic is rollback succeed done at phase two.
	BranchStatusPhaseTwoRollbacked = BranchStatus(8)
	// BranchStatusPhaseTwoRollbackFailedRetriable Rollback logic is failed but retriable.
	BranchStatusPhaseTwoRollbackFailedRetriable = BranchStatus(9)
	// BranchStatusPhaseTwoRollbackFailedUnretriable Rollback logic is failed but NOT retriable.
	BranchStatusPhaseTwoRollbackFailedUnretriable = BranchStatus(10)
)

// Name returns name of the status
func (s *BranchStatus) Name() string {
	switch *s {
	case BranchStatusRegistered:
		return "registered"
	case BranchStatusPhaseOneDone:
		return "phase_1_done"
	case BranchStatusPhaseOneFailed:
		return "phase_1_failed"
	case BranchStatusPhaseOneTimeout:
		return "phase_1_timeout"
	case BranchStatusPhaseTwoCommitted:
		return "phase_2_committed"
	case BranchStatusPhaseTwoCommitFailedRetriable:
		return "phase_2_failed_retriable"
	case BranchStatusPhaseTwoCommitFailedUnretriable:
		return "phase_2_failed"
	case BranchStatusPhaseTwoRollbacked:
		return "phase_2_rollbacked"
	case BranchStatusPhaseTwoRollbackFailedRetriable:
		return "phase_2_rollback_failed_retriable"
	case BranchStatusPhaseTwoRollbackFailedUnretriable:
		return "phase_2_rollback_failed"
	default:
		return "unknown"
	}
}

// GlobalStatus global transaction status
type GlobalStatus byte

var (
	// GlobalStatusUnKnown Unknown
	GlobalStatusUnKnown = GlobalStatus(0)
	// GlobalStatusBegin PHASE 1: can accept new branch registering.
	GlobalStatusBegin = GlobalStatus(1)

	//  PHASE 2: Running Status: may be changed any time.

	// GlobalStatusCommitting Committing.
	GlobalStatusCommitting = GlobalStatus(2)
	// GlobalStatusRetryCommitting Retrying commit after a recoverable failure.
	GlobalStatusRetryCommitting = GlobalStatus(3)
	// GlobalStatusRollbacking Rollbacking
	GlobalStatusRollbacking = GlobalStatus(4)
	// GlobalStatusRetryRollbacking Retrying rollback after a recoverable failure.
	GlobalStatusRetryRollbacking = GlobalStatus(5)
	// GlobalStatusRollbackingSinceTimeout Rollbacking since timeout
	GlobalStatusRollbackingSinceTimeout = GlobalStatus(6)
	// GlobalStatusRetryRollbackingSinceTimeout Retrying rollback (since timeout) after a recoverable failure.
	GlobalStatusRetryRollbackingSinceTimeout = GlobalStatus(7)

	// PHASE 2: Final Status: will NOT change any more.

	// GlobalStatusCommitted Finally: global transaction is successfully committed.
	GlobalStatusCommitted = GlobalStatus(8)
	// GlobalStatusCommitFailed Finally: failed to commit
	GlobalStatusCommitFailed = GlobalStatus(9)
	// GlobalStatusRollbacked Finally: global transaction is successfully rollbacked.
	GlobalStatusRollbacked = GlobalStatus(10)
	// GlobalStatusRollbackFailed Finally: failed to rollback
	GlobalStatusRollbackFailed = GlobalStatus(11)
	// GlobalStatusRollbackedSinceTimeout Finally: global transaction is successfully rollbacked since timeout.
	GlobalStatusRollbackedSinceTimeout = GlobalStatus(12)
	// GlobalStatusRollbackFailedSinceTimeout Finally: failed to rollback since timeout
	GlobalStatusRollbackFailedSinceTimeout = GlobalStatus(13)
	// GlobalStatusFinished Not managed in session map any more
	GlobalStatusFinished = GlobalStatus(14)
)

// Name returns name of the status
func (s *GlobalStatus) Name() string {
	switch *s {
	case GlobalStatusUnKnown:
		return "unknown"
	case GlobalStatusBegin:
		return "begin"
	case GlobalStatusCommitting:
		return "committing"
	case GlobalStatusRetryCommitting:
		return "retry_committing"
	case GlobalStatusRollbacking:
		return "rollbacking"
	case GlobalStatusRetryRollbacking:
		return "retry_rollbacking"
	case GlobalStatusRollbackingSinceTimeout:
		return "rollbacking_by_timeout"
	case GlobalStatusRetryRollbackingSinceTimeout:
		return "retry_rollbacking_by_timeout"
	case GlobalStatusCommitted:
		return "committed"
	case GlobalStatusCommitFailed:
		return "commit_failed"
	case GlobalStatusRollbacked:
		return "rollbacked"
	case GlobalStatusRollbackFailed:
		return "rollback_failed"
	case GlobalStatusRollbackedSinceTimeout:
		return "rollbacked_by_timeout"
	case GlobalStatusRollbackFailedSinceTimeout:
		return "rollback_failed_by_timeout"
	case GlobalStatusFinished:
		return "finished"
	default:
		return "unknown"
	}
}

// Name enum name
func (s *Action) Name() string {
	switch *s {
	case NoneAction:
		return "None"
	case CommitAction:
		return "Commit"
	case RollbackAction:
		return "Rollback"
	}

	log.Fatalf("invalid action value %d", *s)
	return ""
}

// LockKey lock key
type LockKey struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
}

// NewLockKey return lock key
func NewLockKey(namespace, key string) LockKey {
	return LockKey{
		Namespace: namespace,
		Key:       key,
	}
}

// Value returns lock value
func (key *LockKey) Value() []byte {
	var buf bytes.Buffer
	buf.WriteString(key.Namespace)
	buf.WriteString("/")
	buf.WriteString(key.Key)
	return buf.Bytes()
}

// ResourceManagerSet the resource manager set
type ResourceManagerSet struct {
	ResourceManagers []ResourceManager `json:"rms"`
}

// ResourceManager the resource manager
type ResourceManager struct {
	Resource string    `json:"r"`
	ProxySID string    `json:"ps"`
	RMSID    string    `json:"cs"`
	LastHB   time.Time `json:"-"`
}

// Tag returns rm tag
func (rm *ResourceManager) Tag() string {
	return fmt.Sprintf("RM[%s/%s]",
		rm.Resource,
		rm.RMSID)
}

// GlobalTransaction global transaction
type GlobalTransaction struct {
	ID          uint64               `json:"id"`
	Name        string               `json:"n"`
	Timeout     int64                `json:"t"`
	Creator     string               `json:"c"`
	Proxy       string               `json:"p"`
	StartAt     int64                `json:"s"`
	StartAtTime time.Time            `json:"-"`
	Status      GlobalStatus         `json:"st"`
	Action      Action               `json:"a"`
	Branches    []*BranchTransaction `json:"bs"`
}

// Auto returns if all branches is auto
func (t *GlobalTransaction) Auto() bool {
	for _, b := range t.Branches {
		if b.BranchType == TCC {
			return false
		}
	}

	return true
}

// TimeoutStatus returns true if in timeout status
func (t *GlobalTransaction) TimeoutStatus() bool {
	switch t.Status {
	case GlobalStatusRollbackingSinceTimeout:
		return true
	case GlobalStatusRetryRollbackingSinceTimeout:
		return true
	case GlobalStatusRollbackedSinceTimeout:
		return true
	case GlobalStatusRollbackFailedSinceTimeout:
		return true
	default:
		return false
	}
}

// Reported returns if all branch transaction report phase_1 status
func (t *GlobalTransaction) Reported() bool {
	for _, b := range t.Branches {
		switch b.Status {
		case BranchStatusRegistered:
		case BranchStatusUnknown:
			return false
		}
	}

	return true
}

// MissingReportB returns branches that not report phase_1 status
func (t *GlobalTransaction) MissingReportB() []uint64 {
	var value []uint64
	for _, b := range t.Branches {
		switch b.Status {
		case BranchStatusRegistered:
		case BranchStatusUnknown:
			value = append(value, b.ID)
		}
	}

	return value
}

// Complete returns true if in global is already complete
func (t *GlobalTransaction) Complete() bool {
	switch t.Status {
	case GlobalStatusCommitted:
		return true
	case GlobalStatusCommitFailed:
		return true
	case GlobalStatusRollbacked:
		return true
	case GlobalStatusRollbackFailed:
		return true
	case GlobalStatusRollbackedSinceTimeout:
		return true
	case GlobalStatusRollbackFailedSinceTimeout:
		return true
	case GlobalStatusFinished:
		return true
	default:
		return false
	}
}

// BranchTransaction branch transaction
type BranchTransaction struct {
	ID         uint64       `json:"id"`
	GID        uint64       `json:"g"`
	RMSID      string       `json:"r"`
	Resource   string       `json:"rs"`
	LockKeys   []LockKey    `json:"l"`
	StartAt    int64        `json:"s"`
	ReportAt   int64        `json:"ra"`
	NotifyAt   int64        `json:"na"`
	Status     BranchStatus `json:"st"`
	BranchType BranchType   `json:"bt"`

	StartAtTime  time.Time `json:"-"`
	ReportAtTime time.Time `json:"-"`
	NotifyAtTime time.Time `json:"-"`
}

// Complete is branch transaction complete
func (bt *BranchTransaction) Complete() bool {
	switch bt.Status {
	case BranchStatusPhaseTwoCommitted:
		return true
	case BranchStatusPhaseTwoRollbacked:
		return true
	case BranchStatusPhaseTwoCommitFailedUnretriable:
		return true
	case BranchStatusPhaseTwoRollbackFailedUnretriable:
		return true
	default:
		return false
	}
}

// PhaseOneCommitted is branch transaction has been comitted at phase one
func (bt *BranchTransaction) PhaseOneCommitted() bool {
	switch bt.Status {
	case BranchStatusPhaseOneDone:
		return true
	default:
		return false
	}
}

// TagGlobalTransaction reutrns a global transaction
func TagGlobalTransaction(gid uint64, action string) string {
	return fmt.Sprintf("GT[%d]-%s", gid, action)
}

// TagBranchTransaction reutrns a branch transaction
func TagBranchTransaction(gid, bid uint64, action string) string {
	return fmt.Sprintf("GT[%d/%d]-%s", gid, bid, action)
}

// Notify notify
type Notify struct {
	Resource   string      `json:"r"`
	XID        FragmentXID `json:"x"`
	BID        uint64      `json:"b"`
	Action     Action      `json:"a"`
	BranchType BranchType  `json:"t"`
	id         string
}

// ID returns identified id
func (nt *Notify) ID() string {
	if nt.id == "" {
		nt.id = fmt.Sprintf("%d/%d", nt.XID.GID, nt.BID)
	}

	return nt.id
}

// NotifyACK notify ack
type NotifyACK struct {
	From    string       `json:"f"`
	GID     uint64       `json:"g"`
	BID     uint64       `json:"b"`
	Status  BranchStatus `json:"s"`
	Succeed bool         `json:"su"`
	id      string
}

// ID returns identified id
func (nt *NotifyACK) ID() string {
	if nt.id == "" {
		nt.id = fmt.Sprintf("%d/%d", nt.GID, nt.BID)
	}

	return nt.id
}

// Manual manual schedule
type Manual struct {
	GID    uint64 `json:"g"`
	Action Action `json:"a"`
}
