package client

import (
	"time"

	"github.com/infinivision/taas/pkg/meta"
)

// Batch batch operation
type Batch interface {
	CreateGlobal(name string, timeout time.Duration)
	RegisterBranch(xid meta.FragmentXID, resource string, branchType meta.BranchType, locks string, applicationData string)
	ReportBranchStatus(xid meta.FragmentXID, bid uint64, status meta.BranchStatus, branchType meta.BranchType, resource string, applicationData string)
	CommitGlobal(xid meta.FragmentXID, extraData string)
	RollbackGlobal(xid meta.FragmentXID, extraData string)
}

type batchOperation struct {
	ops *meta.MergedWarpMessage
}

func newBatch() Batch {
	return &batchOperation{
		ops: meta.AcquireMergedWarpMessage(),
	}
}

func (op *batchOperation) CreateGlobal(name string, timeout time.Duration) {
	req := meta.AcquireGlobalBeginRequest()
	req.Timeout = int(timeout / time.Millisecond)
	req.TransactionName = name

	op.ops.Msgs = append(op.ops.Msgs, req)
}

func (op *batchOperation) RegisterBranch(xid meta.FragmentXID, resource string, branchType meta.BranchType, locks string, applicationData string) {
	req := meta.AcquireBranchRegisterRequest()
	req.BranchType = branchType
	req.XID = xid
	req.ResourceID = resource
	req.LockKey = locks
	req.ApplicationData = applicationData

	op.ops.Msgs = append(op.ops.Msgs, req)
}

func (op *batchOperation) ReportBranchStatus(xid meta.FragmentXID, bid uint64, status meta.BranchStatus, branchType meta.BranchType, resource string, applicationData string) {
	req := meta.AcquireBranchReportRequest()
	req.XID = xid
	req.BranchID = bid
	req.BranchStatus = status
	req.ResourceID = resource
	req.BranchType = branchType
	req.ApplicationData = applicationData

	op.ops.Msgs = append(op.ops.Msgs, req)
}

func (op *batchOperation) CommitGlobal(xid meta.FragmentXID, extraData string) {
	req := meta.AcquireGlobalCommitRequest()
	req.XID = xid
	req.ExtraData = extraData

	op.ops.Msgs = append(op.ops.Msgs, req)
}

func (op *batchOperation) RollbackGlobal(xid meta.FragmentXID, extraData string) {
	req := meta.AcquireGlobalRollbackRequest()
	req.XID = xid
	req.ExtraData = extraData

	op.ops.Msgs = append(op.ops.Msgs, req)
}
