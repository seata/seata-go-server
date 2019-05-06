package meta

// AcquireRPCMessage acquire a value from pool
func AcquireRPCMessage() *RPCMessage {
	return &RPCMessage{}
}

// AcquireRetryNotLeaderMessage acquire RetryNotLeaderMessage from pool
func AcquireRetryNotLeaderMessage() *RetryNotLeaderMessage {
	return &RetryNotLeaderMessage{}
}

// AcquireRouteableMessageForNotify acquire a value from pool
func AcquireRouteableMessageForNotify(sid string, msgID uint64, origin Message) *RouteableMessage {
	value := &RouteableMessage{}
	value.RMSID = sid
	value.MsgID = msgID
	value.MsgType = origin.Type()

	buf := origin.Encode()
	_, data, _ := buf.ReadAll()
	buf.Release()
	value.Data = data
	return value
}

// AcquireRouteableMessageForResponse acquire a value from pool
func AcquireRouteableMessageForResponse(msg *RouteableMessage, origin Message) *RouteableMessage {
	value := &RouteableMessage{}
	value.FID = msg.FID
	value.RMSID = msg.RMSID
	value.Seq = msg.Seq
	value.Internal = msg.Internal
	value.Batch = msg.Batch
	value.MsgID = msg.MsgID
	value.MsgType = origin.Type()

	buf := origin.Encode()
	_, data, _ := buf.ReadAll()
	buf.Release()
	value.Data = data
	return value
}

// AcquireRPCMessageForResponseByMsgID acquire a value from pool
func AcquireRPCMessageForResponseByMsgID(msgID uint64, body Message) *RPCMessage {
	value := &RPCMessage{}
	value.Header.MsgID = msgID
	value.Header.MsgType = body.Type()
	value.Header.Flag |= FlagSeataCodec
	if value.Header.MsgType == TypeHeartbeat {
		value.Header.Flag |= FlagHeartbeat
	}

	value.Body = body
	return value
}

// AcquireRPCMessageForResponse acquire a value from pool
func AcquireRPCMessageForResponse(msg *RPCMessage, body Message) *RPCMessage {
	value := &RPCMessage{}
	value.Header.MsgID = msg.Header.MsgID
	value.Header.MsgType = body.Type()
	value.Header.Flag |= FlagSeataCodec
	if value.Header.MsgType == TypeHeartbeat {
		value.Header.Flag |= FlagHeartbeat
	}

	value.Body = body
	return value
}

// AcquireGlobalBeginRequest acquire a value from pool
func AcquireGlobalBeginRequest() *GlobalBeginRequest {
	return &GlobalBeginRequest{}
}

// AcquireGlobalCommitRequest acquire a value from pool
func AcquireGlobalCommitRequest() *GlobalCommitRequest {
	return &GlobalCommitRequest{}
}

// AcquireGlobalCommitResponse acquire a value from pool
func AcquireGlobalCommitResponse() *GlobalCommitResponse {
	return &GlobalCommitResponse{}
}

// AcquireGlobalRollbackRequest acquire a value from pool
func AcquireGlobalRollbackRequest() *GlobalRollbackRequest {
	return &GlobalRollbackRequest{}
}

// AcquireGlobalRollbackResponse acquire a value from pool
func AcquireGlobalRollbackResponse() *GlobalRollbackResponse {
	return &GlobalRollbackResponse{}
}

// AcquireGlobalStatusRequest acquire a value from pool
func AcquireGlobalStatusRequest() *GlobalStatusRequest {
	return &GlobalStatusRequest{}
}

// AcquireGlobalStatusResponse acquire a value from pool
func AcquireGlobalStatusResponse() *GlobalStatusResponse {
	return &GlobalStatusResponse{}
}

// AcquireGlobalLockQueryRequest acquire a value from pool
func AcquireGlobalLockQueryRequest() *GlobalLockQueryRequest {
	return &GlobalLockQueryRequest{}
}

// AcquireGlobalLockQueryResponse acquire a value from pool
func AcquireGlobalLockQueryResponse() *GlobalLockQueryResponse {
	return &GlobalLockQueryResponse{}
}

// AcquireBranchRegisterRequest acquire a value from pool
func AcquireBranchRegisterRequest() *BranchRegisterRequest {
	return &BranchRegisterRequest{}
}

// AcquireBranchRegisterResponse acquire a value from pool
func AcquireBranchRegisterResponse() *BranchRegisterResponse {
	return &BranchRegisterResponse{}
}

// AcquireBranchReportRequest acquire a value from pool
func AcquireBranchReportRequest() *BranchReportRequest {
	return &BranchReportRequest{}
}

// AcquireBranchReportResponse acquire a value from pool
func AcquireBranchReportResponse() *BranchReportResponse {
	return &BranchReportResponse{}
}

// AcquireRegisterTMRequest acquire a value from pool
func AcquireRegisterTMRequest() *RegisterTMRequest {
	return &RegisterTMRequest{}
}

// AcquireRegisterTMResponse acquire a value from pool
func AcquireRegisterTMResponse() *RegisterTMResponse {
	return &RegisterTMResponse{}
}

// AcquireRegisterRMRequest acquire a value from pool
func AcquireRegisterRMRequest() *RegisterRMRequest {
	return &RegisterRMRequest{}
}

// AcquireRegisterRMResponse acquire a value from pool
func AcquireRegisterRMResponse() *RegisterRMResponse {
	return &RegisterRMResponse{}
}

// AcquireGlobalBeginResponse acquire a value from pool
func AcquireGlobalBeginResponse() *GlobalBeginResponse {
	return &GlobalBeginResponse{}
}

// AcquireMergedWarpMessage acquire a value from pool
func AcquireMergedWarpMessage() *MergedWarpMessage {
	return &MergedWarpMessage{}
}

// AcquireMergeResultMessage acquire a value from pool
func AcquireMergeResultMessage() *MergeResultMessage {
	return &MergeResultMessage{}
}

// AcquireBranchCommitRequest acquire a value from pool
func AcquireBranchCommitRequest() *BranchCommitRequest {
	return &BranchCommitRequest{}
}

// AcquireBranchCommitResponse acquire a value from pool
func AcquireBranchCommitResponse() *BranchCommitResponse {
	return &BranchCommitResponse{}
}

// AcquireBranchRollbackRequest acquire a value from pool
func AcquireBranchRollbackRequest() *BranchRollbackRequest {
	return &BranchRollbackRequest{}
}

// AcquireBranchRollbackResponse acquire a value from pool
func AcquireBranchRollbackResponse() *BranchRollbackResponse {
	return &BranchRollbackResponse{}
}
