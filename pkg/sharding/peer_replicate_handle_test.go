package sharding

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/meta"
)

var (
	errOther = errors.New("other errors")
)

func TestHandleGlobalBeginRequest(t *testing.T) {
	tc := newTestTC()
	s := &session{}

	var resultRsp meta.Message
	var resultErr error
	s.cbFunc = func(routeMsg *meta.RouteableMessage, rsp meta.Message, err error) {
		resultRsp = rsp
		resultErr = err
	}

	req := &meta.GlobalBeginRequest{}
	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()
	pr := new(PeerReplicate)
	pr.tc = tc

	tc.rspErr = meta.ErrNotLeader
	pr.handleGlobalBeginRequest(s, msg)
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check pr handle bengin failed")
	assert.Nil(t, resultRsp, "check pr handle bengin failed")

	tc.rspErr = errOther
	pr.handleGlobalBeginRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle bengin failed")
	assert.NotNil(t, resultRsp, "check pr handle bengin failed")

	realRsp, ok := resultRsp.(*meta.GlobalBeginResponse)
	assert.True(t, ok, "check pr handle bengin failed")
	assert.Equal(t, errOther.Error(), realRsp.Msg, "check pr handle bengin failed")

	tc.rspErr = nil
	pr.handleGlobalBeginRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle bengin failed")
	assert.NotNil(t, resultRsp, "check pr handle bengin failed")

	realRsp, ok = resultRsp.(*meta.GlobalBeginResponse)
	assert.True(t, ok, "check pr handle bengin failed")
	assert.Equal(t, "", realRsp.Msg, "check pr handle bengin failed")
}

func TestHandleBranchRegisterRequest(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	var resultRsp meta.Message
	var resultErr error
	s := &session{}
	s.cbFunc = func(routeMsg *meta.RouteableMessage, rsp meta.Message, err error) {
		resultRsp = rsp
		resultErr = err
	}

	req := &meta.BranchRegisterRequest{}
	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()

	tc.rspErr = meta.ErrNotLeader
	pr.handleBranchRegisterRequest(s, msg)
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check pr handle failed")
	assert.Nil(t, resultRsp, "check pr handle failed")

	tc.rspErr = errOther
	pr.handleBranchRegisterRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok := resultRsp.(*meta.BranchRegisterResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, errOther.Error(), realRsp.Msg, "check pr handle failed")

	tc.rspErr = nil
	pr.handleBranchRegisterRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok = resultRsp.(*meta.BranchRegisterResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, "", realRsp.Msg, "check pr handle failed")
}

func TestHandleGlobalCommitRequest(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	var resultRsp meta.Message
	var resultErr error
	s := &session{}
	s.cbFunc = func(routeMsg *meta.RouteableMessage, rsp meta.Message, err error) {
		resultRsp = rsp
		resultErr = err
	}

	req := &meta.GlobalCommitRequest{}
	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()

	tc.rspErr = meta.ErrNotLeader
	pr.handleGlobalCommitRequest(s, msg)
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check pr handle failed")
	assert.Nil(t, resultRsp, "check pr handle failed")

	tc.rspErr = errOther
	pr.handleGlobalCommitRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok := resultRsp.(*meta.GlobalCommitResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, errOther.Error(), realRsp.Msg, "check pr handle failed")

	tc.rspErr = nil
	pr.handleGlobalCommitRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok = resultRsp.(*meta.GlobalCommitResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, "", realRsp.Msg, "check pr handle failed")
}

func TestHandleGlobalRollbackRequest(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	var resultRsp meta.Message
	var resultErr error
	s := &session{}
	s.cbFunc = func(routeMsg *meta.RouteableMessage, rsp meta.Message, err error) {
		resultRsp = rsp
		resultErr = err
	}

	req := &meta.GlobalRollbackRequest{}
	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()

	tc.rspErr = meta.ErrNotLeader
	pr.handleGlobalRollbackRequest(s, msg)
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check pr handle failed")
	assert.Nil(t, resultRsp, "check pr handle failed")

	tc.rspErr = errOther
	pr.handleGlobalRollbackRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok := resultRsp.(*meta.GlobalRollbackResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, errOther.Error(), realRsp.Msg, "check pr handle failed")

	tc.rspErr = nil
	pr.handleGlobalRollbackRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok = resultRsp.(*meta.GlobalRollbackResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, "", realRsp.Msg, "check pr handle failed")
}

func TestHandleBranchReportRequest(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	var resultRsp meta.Message
	var resultErr error
	s := &session{}
	s.cbFunc = func(routeMsg *meta.RouteableMessage, rsp meta.Message, err error) {
		resultRsp = rsp
		resultErr = err
	}

	req := &meta.BranchReportRequest{}
	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()

	tc.rspErr = meta.ErrNotLeader
	pr.handleBranchReportRequest(s, msg)
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check pr handle failed")
	assert.Nil(t, resultRsp, "check pr handle failed")

	tc.rspErr = errOther
	pr.handleBranchReportRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok := resultRsp.(*meta.BranchReportResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, errOther.Error(), realRsp.Msg, "check pr handle failed")

	tc.rspErr = nil
	pr.handleBranchReportRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok = resultRsp.(*meta.BranchReportResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, "", realRsp.Msg, "check pr handle failed")
}

func TestHandleGlobalStatusRequest(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	var resultRsp meta.Message
	var resultErr error
	s := &session{}
	s.cbFunc = func(routeMsg *meta.RouteableMessage, rsp meta.Message, err error) {
		resultRsp = rsp
		resultErr = err
	}

	req := &meta.BranchReportRequest{}
	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()

	tc.rspErr = meta.ErrNotLeader
	pr.handleBranchReportRequest(s, msg)
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check pr handle failed")
	assert.Nil(t, resultRsp, "check pr handle failed")

	tc.rspErr = errOther
	pr.handleBranchReportRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok := resultRsp.(*meta.BranchReportResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, errOther.Error(), realRsp.Msg, "check pr handle failed")

	tc.rspErr = nil
	pr.handleBranchReportRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok = resultRsp.(*meta.BranchReportResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, "", realRsp.Msg, "check pr handle failed")
}

func TestHandleBranchCommitResponse(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	s := &session{}
	req := &meta.BranchCommitResponse{}
	req.ResultCode = meta.Succeed
	req.BranchStatus = meta.BranchStatusPhaseTwoCommitted

	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()

	pr.handleBranchCommitResponse(s, msg)
	assert.True(t, tc.ack.Succeed, "check pr handle failed")
	assert.Equal(t, meta.BranchStatusPhaseTwoCommitted, tc.ack.Status, "check pr handle failed")
}

func TestHandleBranchRollbackResponse(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	s := &session{}
	req := &meta.BranchRollbackResponse{}
	req.ResultCode = meta.Succeed
	req.BranchStatus = meta.BranchStatusPhaseTwoRollbacked

	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()

	pr.handleBranchRollbackResponse(s, msg)
	assert.True(t, tc.ack.Succeed, "check pr handle failed")
	assert.Equal(t, meta.BranchStatusPhaseTwoRollbacked, tc.ack.Status, "check pr handle failed")
}

func TestHandleGlobalLockQueryRequest(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	var resultRsp meta.Message
	var resultErr error
	s := &session{}
	s.cbFunc = func(routeMsg *meta.RouteableMessage, rsp meta.Message, err error) {
		resultRsp = rsp
		resultErr = err
	}

	req := &meta.GlobalLockQueryRequest{}
	msg := &meta.RouteableMessage{}
	msg.MsgType = req.Type()
	_, msg.Data, _ = req.Encode().ReadAll()

	tc.rspErr = meta.ErrNotLeader
	pr.handleGlobalLockQueryRequest(s, msg)
	assert.Equal(t, meta.ErrNotLeader, resultErr, "check pr handle failed")
	assert.Nil(t, resultRsp, "check pr handle failed")

	tc.rspErr = errOther
	pr.handleGlobalLockQueryRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok := resultRsp.(*meta.GlobalLockQueryResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, errOther.Error(), realRsp.Msg, "check pr handle failed")

	tc.rspErr = nil
	tc.lockable = true
	pr.handleGlobalLockQueryRequest(s, msg)
	assert.Nil(t, resultErr, "check pr handle failed")
	assert.NotNil(t, resultRsp, "check pr handle failed")

	realRsp, ok = resultRsp.(*meta.GlobalLockQueryResponse)
	assert.True(t, ok, "check pr handle failed")
	assert.Equal(t, "", realRsp.Msg, "check pr handle failed")
	assert.True(t, realRsp.Lockable, "check pr handle failed")
}
