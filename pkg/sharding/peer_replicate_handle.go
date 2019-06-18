package sharding

import (
	"seata.io/server/pkg/meta"
)

func (pr *PeerReplicate) handleGlobalBeginRequest(s *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.GlobalBeginRequest)
	rsp := meta.AcquireGlobalBeginResponse()
	rsp.ResultCode = meta.Failed
	rsp.Err = meta.ErrIO

	pr.tc.RegistryGlobalTransaction(req.ToCreateGlobalTransaction(msg.RMSID, msg.ProxyAddr), func(id uint64, err error) {
		if err != nil {
			if err == meta.ErrNotLeader {
				s.cb(msg, nil, err)
				return
			}

			if e, ok := err.(*meta.Error); ok {
				rsp.Err = e
			}
			rsp.Msg = err.Error()
			s.cb(msg, rsp, nil)
			return
		}

		rsp.ResultCode = meta.Succeed
		rsp.XID = meta.NewFragmentXID(id, pr.id)
		s.cb(msg, rsp, nil)
	})
}

func (pr *PeerReplicate) handleBranchRegisterRequest(s *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.BranchRegisterRequest)

	value, err := req.ToCreateBranchTransaction(msg.RMSID)
	if err != nil {
		s.cb(msg, nil, err)
		return
	}

	rsp := meta.AcquireBranchRegisterResponse()
	rsp.ResultCode = meta.Failed

	pr.tc.RegistryBranchTransaction(value, func(id uint64, err error) {
		if err != nil {
			if err == meta.ErrNotLeader {
				s.cb(msg, nil, err)
				return
			}

			if e, ok := err.(*meta.Error); ok {
				rsp.Err = e
			}
			rsp.Msg = err.Error()
			s.cb(msg, rsp, nil)
			return
		}

		rsp.ResultCode = meta.Succeed
		rsp.BranchID = id

		s.cb(msg, rsp, nil)
	})
}

func (pr *PeerReplicate) handleGlobalCommitRequest(s *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.GlobalCommitRequest)

	rsp := meta.AcquireGlobalCommitResponse()
	rsp.ResultCode = meta.Failed

	pr.tc.CommitGlobalTransaction(req.XID.GID, msg.RMSID, func(status meta.GlobalStatus, err error) {
		if err != nil {
			if err == meta.ErrNotLeader {
				s.cb(msg, nil, err)
				return
			}

			if e, ok := err.(*meta.Error); ok {
				rsp.Err = e
			}
			rsp.Msg = err.Error()
			rsp.GlobalStatus = status
			s.cb(msg, rsp, nil)
			return
		}

		rsp.ResultCode = meta.Succeed
		rsp.GlobalStatus = status
		s.cb(msg, rsp, nil)
	})
}

func (pr *PeerReplicate) handleGlobalRollbackRequest(s *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.GlobalRollbackRequest)

	rsp := meta.AcquireGlobalRollbackResponse()
	rsp.ResultCode = meta.Failed

	pr.tc.RollbackGlobalTransaction(req.XID.GID, msg.RMSID, func(status meta.GlobalStatus, err error) {
		if err != nil {
			if err == meta.ErrNotLeader {
				s.cb(msg, nil, err)
				return
			}

			if e, ok := err.(*meta.Error); ok {
				rsp.Err = e
			}
			rsp.Msg = err.Error()
			rsp.GlobalStatus = status
			s.cb(msg, rsp, nil)
			return
		}

		rsp.ResultCode = meta.Succeed
		rsp.GlobalStatus = status
		s.cb(msg, rsp, nil)
		return
	})
}

func (pr *PeerReplicate) handleBranchReportRequest(s *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.BranchReportRequest)

	rsp := meta.AcquireBranchReportResponse()
	rsp.ResultCode = meta.Failed

	pr.tc.ReportBranchTransactionStatus(req.ToReportBranchStatus(msg.RMSID), func(err error) {
		if err != nil {
			if err == meta.ErrNotLeader {
				s.cb(msg, nil, err)
				return
			}

			if e, ok := err.(*meta.Error); ok {
				rsp.Err = e
			}
			rsp.Msg = err.Error()
			s.cb(msg, rsp, nil)
			return
		}

		rsp.ResultCode = meta.Succeed
		s.cb(msg, rsp, nil)
	})
}

func (pr *PeerReplicate) handleGlobalStatusRequest(s *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.GlobalStatusRequest)

	rsp := meta.AcquireGlobalStatusResponse()
	rsp.ResultCode = meta.Failed

	pr.tc.GlobalTransactionStatus(req.XID.GID, func(status meta.GlobalStatus, err error) {
		if err != nil {
			if err == meta.ErrNotLeader {
				s.cb(msg, nil, err)
				return
			}

			if e, ok := err.(*meta.Error); ok {
				rsp.Err = e
			}
			rsp.Msg = err.Error()
			rsp.GlobalStatus = status
			s.cb(msg, rsp, nil)
			return
		}

		rsp.ResultCode = meta.Succeed
		rsp.GlobalStatus = status
		s.cb(msg, rsp, nil)
	})
}

func (pr *PeerReplicate) handleBranchCommitResponse(s *session, msg *meta.RouteableMessage) {
	resp := msg.ReadOriginMsg().(*meta.BranchCommitResponse)
	pr.tc.BranchTransactionNotifyACK(meta.NotifyACK{
		From:    msg.RMSID,
		Succeed: resp.Succeed(),
		GID:     resp.XID.GID,
		BID:     resp.BranchID,
		Status:  resp.BranchStatus,
	})
}

func (pr *PeerReplicate) handleBranchRollbackResponse(s *session, msg *meta.RouteableMessage) {
	resp := msg.ReadOriginMsg().(*meta.BranchRollbackResponse)
	pr.tc.BranchTransactionNotifyACK(meta.NotifyACK{
		From:    msg.RMSID,
		Succeed: resp.Succeed(),
		GID:     resp.XID.GID,
		BID:     resp.BranchID,
		Status:  resp.BranchStatus,
	})
}

func (pr *PeerReplicate) handleGlobalLockQueryRequest(s *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.GlobalLockQueryRequest)

	rsp := meta.AcquireGlobalLockQueryResponse()
	rsp.ResultCode = meta.Failed
	rsp.Lockable = false

	locks, err := meta.ParseLockKeys(req.LockKey)
	if err != nil {
		rsp.Err = meta.ErrLockableCheckFailed
		rsp.Msg = err.Error()
		s.cb(msg, rsp, nil)
		return
	}

	pr.tc.Lockable(req.ResourceID, req.XID.GID, locks, func(lockable bool, err error) {
		if err != nil {
			if err == meta.ErrNotLeader {
				s.cb(msg, nil, err)
				return
			}

			if e, ok := err.(*meta.Error); ok {
				rsp.Err = e
			}
			rsp.Msg = err.Error()
			s.cb(msg, rsp, nil)
			return
		}

		rsp.ResultCode = meta.Succeed
		rsp.Lockable = lockable
		s.cb(msg, rsp, nil)
	})
}
