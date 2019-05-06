package meta

import (
	"math"
	"strings"
	"time"

	"github.com/fagongzi/goetty"
)

const (
	// Failed failed
	Failed byte = 0
	// Succeed Succeed
	Succeed byte = 1
)

const (
	// TypeHeartbeat msg type
	TypeHeartbeat uint16 = 0
	// TypeGlobalBegin msg type
	TypeGlobalBegin uint16 = 1
	// TypeGlobalBeginResult msg type
	TypeGlobalBeginResult uint16 = 2
	// TypeBranchCommit msg type
	TypeBranchCommit uint16 = 3
	// TypeBranchCommitResult msg type
	TypeBranchCommitResult uint16 = 4
	// TypeBranchRollback msg type
	TypeBranchRollback uint16 = 5
	// TypeBranchRollbackResult msg type
	TypeBranchRollbackResult uint16 = 6
	// TypeGlobalCommit msg type
	TypeGlobalCommit uint16 = 7
	// TypeGlobalCommitResult msg type
	TypeGlobalCommitResult uint16 = 8
	// TypeGlobalRollback msg type
	TypeGlobalRollback uint16 = 9
	// TypeGlobalRollbackResult msg type
	TypeGlobalRollbackResult uint16 = 10
	// TypeBranchRegister msg type
	TypeBranchRegister uint16 = 11
	// TypeBranchRegisterResult msg type
	TypeBranchRegisterResult uint16 = 12
	// TypeBranchStatusReport msg type
	TypeBranchStatusReport uint16 = 13
	// TypeBranchStatusReportResult msg type
	TypeBranchStatusReportResult uint16 = 14
	// TypeGlobalStatus msg type
	TypeGlobalStatus uint16 = 15
	// TypeGlobalStatusResult msg type
	TypeGlobalStatusResult uint16 = 16
	// TypeGlobalLockQuery msg type
	TypeGlobalLockQuery uint16 = 21
	// TypeGlobalLockQueryResult msg type
	TypeGlobalLockQueryResult uint16 = 22
	// TypeSeataMerge msg type
	TypeSeataMerge uint16 = 59
	// TypeSeataMergeResult msg type
	TypeSeataMergeResult uint16 = 60
	// TypeRegClt msg type
	TypeRegClt uint16 = 101
	// TypeRegCltResult msg type
	TypeRegCltResult uint16 = 102
	// TypeRegRM msg type
	TypeRegRM uint16 = 103
	// TypeRegRMResult msg type
	TypeRegRMResult uint16 = 104
	// TypeRetryNotLeader for sharding
	TypeRetryNotLeader uint16 = math.MaxUint16
)

// Message message
type Message interface {
	Type() uint16
	Decode(buf *goetty.ByteBuf)
	Encode() *goetty.ByteBuf
	BatchEncode(*goetty.ByteBuf)
}

// RetryNotLeaderMessage not leader
type RetryNotLeaderMessage struct {
	FID            uint64
	NewLeader      uint64
	NewLeaderStore uint64
	RetryData      []byte
}

// Type message type
func (msg *RetryNotLeaderMessage) Type() uint16 {
	return TypeRetryNotLeader
}

// Encode encode message
func (msg *RetryNotLeaderMessage) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(256)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *RetryNotLeaderMessage) BatchEncode(buf *goetty.ByteBuf) {
	buf.WriteUint64(msg.FID)
	buf.WriteUint64(msg.NewLeader)
	buf.WriteUint64(msg.NewLeaderStore)
	WriteSlice(msg.RetryData, buf)
}

// Decode decode message from byte buffer
func (msg *RetryNotLeaderMessage) Decode(buf *goetty.ByteBuf) {
	msg.FID = ReadUInt64(buf)
	msg.NewLeader = ReadUInt64(buf)
	msg.NewLeaderStore = ReadUInt64(buf)
	msg.RetryData = ReadSlice(buf)
}

// HeartbeatMessage heart
type HeartbeatMessage struct {
}

// Type message type
func (msg *HeartbeatMessage) Type() uint16 {
	return TypeHeartbeat
}

// Encode encode message
func (msg *HeartbeatMessage) Encode() *goetty.ByteBuf {
	return nil
}

// BatchEncode encode message
func (msg *HeartbeatMessage) BatchEncode(buf *goetty.ByteBuf) {
	return
}

// Decode decode message from byte buffer
func (msg *HeartbeatMessage) Decode(buf *goetty.ByteBuf) {
	return
}

// GlobalBeginRequest begin a global request
type GlobalBeginRequest struct {
	Timeout         int
	TransactionName string
}

// ToCreateGlobalTransaction returns create info
func (msg *GlobalBeginRequest) ToCreateGlobalTransaction(creator, proxy string) CreateGlobalTransaction {
	return CreateGlobalTransaction{
		Creator: creator,
		Proxy:   proxy,
		Name:    msg.TransactionName,
		Timeout: time.Duration(msg.Timeout) * time.Millisecond,
	}
}

// Type returns message type
func (msg *GlobalBeginRequest) Type() uint16 {
	return TypeGlobalBegin
}

// Encode encode message
func (msg *GlobalBeginRequest) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(64)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *GlobalBeginRequest) BatchEncode(buf *goetty.ByteBuf) {
	buf.WriteInt(msg.Timeout)
	WriteString(msg.TransactionName, buf)
}

// Decode decode message from byte buffer
func (msg *GlobalBeginRequest) Decode(buf *goetty.ByteBuf) {
	msg.Timeout = ReadInt(buf)
	msg.TransactionName = ReadString(buf)
}

type abstractBranchEndRequest struct {
	XID             FragmentXID
	BranchID        uint64
	BranchType      BranchType
	ResourceID      string
	ApplicationData string
}

// Encode encode message
func (msg *abstractBranchEndRequest) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(1024)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode batch encode
func (msg *abstractBranchEndRequest) BatchEncode(buf *goetty.ByteBuf) {
	WriteXID(msg.XID, buf)
	buf.WriteUint64(msg.BranchID)
	buf.WriteByte(byte(msg.BranchType))
	WriteString(msg.ResourceID, buf)
	WriteBigString(msg.ApplicationData, buf)
}

// Decode decode message from byte buffer
func (msg *abstractBranchEndRequest) Decode(buf *goetty.ByteBuf) {
	msg.XID = ReadXID(buf)
	msg.BranchID = ReadUInt64(buf)
	msg.BranchType = BranchType(ReadByte(buf))
	msg.ResourceID = ReadString(buf)
	msg.ApplicationData = ReadBigString(buf)
}

// MaybeDecode maybe decode message from byte buffer
func (msg *abstractBranchEndRequest) MaybeDecode(buf *goetty.ByteBuf) bool {
	xid, ok := MaybeReadXID(buf)
	if !ok {
		return false
	}
	msg.XID = xid

	if buf.Readable() < 9 {
		return false
	}
	msg.BranchID = ReadUInt64(buf)
	msg.BranchType = BranchType(ReadByte(buf))

	value, ok := MaybeReadString(buf)
	if !ok {
		return false
	}
	msg.ResourceID = value

	value, ok = MaybeReadBigString(buf)
	if !ok {
		return false
	}
	msg.ApplicationData = value

	return true
}

// BranchCommitRequest commit branch request
type BranchCommitRequest struct {
	abstractBranchEndRequest
}

// Type returns message type
func (msg *BranchCommitRequest) Type() uint16 {
	return TypeBranchCommit
}

// BranchRollbackRequest rollback branch request
type BranchRollbackRequest struct {
	abstractBranchEndRequest
}

// Type returns message type
func (msg *BranchRollbackRequest) Type() uint16 {
	return TypeBranchRollback
}

type globalActionRequest struct {
	XID       FragmentXID
	ExtraData string
}

// Encode encode message
func (msg *globalActionRequest) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(256)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode batch encode
func (msg *globalActionRequest) BatchEncode(buf *goetty.ByteBuf) {
	WriteXID(msg.XID, buf)
	WriteString(msg.ExtraData, buf)
}

// Decode decode message from byte buffer
func (msg *globalActionRequest) Decode(buf *goetty.ByteBuf) {
	msg.XID = ReadXID(buf)
	msg.ExtraData = ReadString(buf)
}

// GlobalCommitRequest commit global transaction request
type GlobalCommitRequest struct {
	globalActionRequest
}

// Type returns message type
func (msg *GlobalCommitRequest) Type() uint16 {
	return TypeGlobalCommit
}

// GlobalRollbackRequest rollback global transaction request
type GlobalRollbackRequest struct {
	globalActionRequest
}

// Type returns message type
func (msg *GlobalRollbackRequest) Type() uint16 {
	return TypeGlobalRollback
}

// BranchRegisterRequest register a branch request
type BranchRegisterRequest struct {
	XID             FragmentXID
	BranchType      BranchType
	ResourceID      string
	LockKey         string // table1:pk1,pk2;table2:pk1,pk2
	ApplicationData string
}

// ToCreateBranchTransaction adapter method to createBranchTransaction
func (msg *BranchRegisterRequest) ToCreateBranchTransaction(rmSID string) (CreateBranchTransaction, error) {
	locks, err := ParseLockKeys(msg.LockKey)
	if err != nil {
		return CreateBranchTransaction{}, err
	}

	return CreateBranchTransaction{
		ResourceID: msg.ResourceID,
		RMSID:      rmSID,
		GID:        msg.XID.GID,
		BranchType: msg.BranchType,
		LockKeys:   locks,
	}, nil
}

// Type returns message type
func (msg *BranchRegisterRequest) Type() uint16 {
	return TypeBranchRegister
}

// Encode encode message
func (msg *BranchRegisterRequest) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(1024)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode batch encode
func (msg *BranchRegisterRequest) BatchEncode(buf *goetty.ByteBuf) {
	WriteXID(msg.XID, buf)
	buf.WriteByte(byte(msg.BranchType))
	WriteString(msg.ResourceID, buf)
	WriteBigString(msg.LockKey, buf)
	WriteBigString(msg.ApplicationData, buf)
}

// Decode decode message from byte buffer
func (msg *BranchRegisterRequest) Decode(buf *goetty.ByteBuf) {
	msg.XID = ReadXID(buf)
	msg.BranchType = BranchType(ReadByte(buf))
	msg.ResourceID = ReadString(buf)
	msg.LockKey = ReadBigString(buf)
	msg.ApplicationData = ReadBigString(buf)
}

// BranchReportRequest branch transaction report request
type BranchReportRequest struct {
	XID             FragmentXID
	BranchID        uint64
	BranchType      BranchType
	ResourceID      string
	BranchStatus    BranchStatus
	ApplicationData string
}

// ToReportBranchStatus adapter method
func (msg *BranchReportRequest) ToReportBranchStatus(rmSID string) ReportBranchStatus {
	return ReportBranchStatus{
		ResourceID: msg.ResourceID,
		GID:        msg.XID.GID,
		BID:        msg.BranchID,
		Status:     msg.BranchStatus,
		RMSID:      rmSID,
	}
}

// Type returns message type
func (msg *BranchReportRequest) Type() uint16 {
	return TypeBranchStatusReport
}

// Encode encode message
func (msg *BranchReportRequest) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(1024)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode batch encode
func (msg *BranchReportRequest) BatchEncode(buf *goetty.ByteBuf) {
	WriteXID(msg.XID, buf)
	buf.WriteUInt64(msg.BranchID)
	buf.WriteByte(byte(msg.BranchStatus))
	WriteString(msg.ResourceID, buf)
	WriteBigString(msg.ApplicationData, buf)
	buf.WriteByte(byte(msg.BranchType))
}

// Decode decode message from byte buffer
func (msg *BranchReportRequest) Decode(buf *goetty.ByteBuf) {
	msg.XID = ReadXID(buf)
	msg.BranchID = ReadUInt64(buf)
	msg.BranchStatus = BranchStatus(ReadByte(buf))
	msg.ResourceID = ReadString(buf)
	msg.ApplicationData = ReadBigString(buf)
	msg.BranchType = BranchType(ReadByte(buf))
}

// GlobalStatusRequest global status request
type GlobalStatusRequest struct {
	globalActionRequest
}

// Type returns message type
func (msg *GlobalStatusRequest) Type() uint16 {
	return TypeGlobalStatus
}

// GlobalLockQueryRequest query global lock request
type GlobalLockQueryRequest struct {
	BranchRegisterRequest
}

// Type returns message type
func (msg *GlobalLockQueryRequest) Type() uint16 {
	return TypeGlobalLockQuery
}

// MergedWarpMessage branch transaction report request
type MergedWarpMessage struct {
	Msgs   []Message
	MsgIDs []uint64
}

// Type returns message type
func (msg *MergedWarpMessage) Type() uint16 {
	return TypeSeataMerge
}

// Encode encode message
func (msg *MergedWarpMessage) Encode() *goetty.ByteBuf {
	count := len(msg.Msgs)
	buf := goetty.NewByteBuf(1024 * count)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode batch encode
func (msg *MergedWarpMessage) BatchEncode(buf *goetty.ByteBuf) {
	start := buf.GetWriteIndex()
	written := buf.Readable()

	count := len(msg.Msgs)
	buf.WriteInt(0)
	buf.WriteUInt16(uint16(count))
	for i := 0; i < count; i++ {
		buf.WriteUInt16(msg.Msgs[i].Type())
		msg.Msgs[i].BatchEncode(buf)
	}
	writeIndex := buf.GetWriteIndex()

	size := buf.Readable() - written - 4
	buf.SetWriterIndex(start)
	buf.WriteInt(size)
	buf.SetWriterIndex(writeIndex)
}

// Decode decode message from byte buffer
func (msg *MergedWarpMessage) Decode(buf *goetty.ByteBuf) {
	count := int(ReadUInt16(buf))
	for i := 0; i < count; i++ {
		m := newMessageByType(ReadUInt16(buf))
		m.Decode(buf)
		msg.Msgs = append(msg.Msgs, m)
	}
}

type abstractIdentifyRequest struct {
	Version                 string
	ApplicationID           string
	TransactionServiceGroup string
	ExtraData               string
}

// Encode encode message
func (msg *abstractIdentifyRequest) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(10 * 1024)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode batch encode
func (msg *abstractIdentifyRequest) BatchEncode(buf *goetty.ByteBuf) {
	WriteString(msg.Version, buf)
	WriteString(msg.ApplicationID, buf)
	WriteString(msg.TransactionServiceGroup, buf)
	WriteString(msg.ExtraData, buf)
}

// Decode decode message from byte buffer
func (msg *abstractIdentifyRequest) Decode(buf *goetty.ByteBuf) {
	msg.Version = ReadString(buf)
	msg.ApplicationID = ReadString(buf)
	msg.TransactionServiceGroup = ReadString(buf)
	msg.ExtraData = ReadString(buf)
}

// MaybeDecode maybe decode message from byte buffer
func (msg *abstractIdentifyRequest) MaybeDecode(buf *goetty.ByteBuf) bool {
	value, ok := MaybeReadString(buf)
	if !ok {
		return false
	}
	msg.Version = value

	value, ok = MaybeReadString(buf)
	if !ok {
		return false
	}
	msg.ApplicationID = value

	value, ok = MaybeReadString(buf)
	if !ok {
		return false
	}
	msg.TransactionServiceGroup = value

	value, ok = MaybeReadString(buf)
	if !ok {
		return false
	}
	msg.ExtraData = value
	return true
}

// RegisterTMRequest register TM request
type RegisterTMRequest struct {
	abstractIdentifyRequest
}

// Type returns message type
func (msg *RegisterTMRequest) Type() uint16 {
	return TypeRegClt
}

// RegisterRMRequest register RM request
type RegisterRMRequest struct {
	abstractIdentifyRequest
	ResourceIDs string
}

// ToResourceSet returns resource manager set
func (msg *RegisterRMRequest) ToResourceSet(proxySID, rmSID string) ResourceManagerSet {
	rms := ResourceManagerSet{}
	for _, resource := range strings.Split(msg.ResourceIDs, ",") {
		rms.ResourceManagers = append(rms.ResourceManagers, ResourceManager{
			Resource: resource,
			ProxySID: proxySID,
			RMSID:    rmSID,
		})
	}

	return rms
}

// Type returns message type
func (msg *RegisterRMRequest) Type() uint16 {
	return TypeRegRM
}

// Encode encode message
func (msg *RegisterRMRequest) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(10 * 1024)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode batch encode
func (msg *RegisterRMRequest) BatchEncode(buf *goetty.ByteBuf) {
	msg.abstractIdentifyRequest.BatchEncode(buf)
	WriteBigString(msg.ResourceIDs, buf)
}

// Decode decode message from byte buffer
func (msg *RegisterRMRequest) Decode(buf *goetty.ByteBuf) {
	msg.abstractIdentifyRequest.Decode(buf)
	msg.ResourceIDs = ReadBigString(buf)
}

// MaybeDecode decode message from byte buffer
func (msg *RegisterRMRequest) MaybeDecode(buf *goetty.ByteBuf) bool {
	if !msg.abstractIdentifyRequest.MaybeDecode(buf) {
		return false
	}

	value, ok := MaybeReadBigString(buf)
	if !ok {
		return false
	}

	msg.ResourceIDs = value
	return true
}

type abstractResultMessage struct {
	ResultCode byte
	Msg        string
}

// Succeed is this message is operation succeed
func (msg *abstractResultMessage) Succeed() bool {
	return msg.ResultCode == Succeed
}

func (msg *abstractResultMessage) BatchEncode(buf *goetty.ByteBuf) {
	buf.WriteByte(msg.ResultCode)
	if msg.ResultCode == Failed {
		if msg.Msg != "" {
			size := len(msg.Msg)
			if size > 400 {
				size = 400
			}

			buf.WriteUInt16(uint16(size))
			buf.WriteString(msg.Msg[0:size])
		} else {
			buf.WriteUInt16(0)
		}
	}
}

// Decode decode message from byte buffer
func (msg *abstractResultMessage) Decode(buf *goetty.ByteBuf) {
	msg.ResultCode = ReadByte(buf)
	if msg.ResultCode == Failed {
		msg.Msg = ReadString(buf)
	}
}

// MaybeDecode decode message from byte buffer
func (msg *abstractResultMessage) MaybeDecode(buf *goetty.ByteBuf) bool {
	if buf.Readable() < 1 {
		return false
	}
	msg.ResultCode = ReadByte(buf)

	if msg.ResultCode == Failed {
		value, ok := MaybeReadString(buf)
		if !ok {
			return false
		}
		msg.Msg = value
	}

	return true
}

type abstractTransactionResponse struct {
	abstractResultMessage
	Err *Error
}

// BatchEncode encode message
func (msg *abstractTransactionResponse) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(512)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *abstractTransactionResponse) BatchEncode(buf *goetty.ByteBuf) {
	msg.abstractResultMessage.BatchEncode(buf)
	if msg.Err != nil {
		buf.WriteByte(msg.Err.Code)
	} else {
		buf.WriteByte(ErrUnknown.Code)
	}
}

// Decode decode message from byte buffer
func (msg *abstractTransactionResponse) Decode(buf *goetty.ByteBuf) {
	msg.abstractResultMessage.Decode(buf)
	msg.Err = NewErrorFrom(ReadByte(buf))
}

// MaybeDecode decode message from byte buffer
func (msg *abstractTransactionResponse) MaybeDecode(buf *goetty.ByteBuf) bool {
	if !msg.abstractResultMessage.MaybeDecode(buf) {
		return false
	}

	if buf.Readable() < 1 {
		return false
	}

	msg.Err = NewErrorFrom(ReadByte(buf))
	return true
}

// GlobalBeginResponse begin a global transaction response
type GlobalBeginResponse struct {
	abstractTransactionResponse
	XID       FragmentXID
	ExtraData string
}

// Type returns message type
func (msg *GlobalBeginResponse) Type() uint16 {
	return TypeGlobalBeginResult
}

// Encode encode message
func (msg *GlobalBeginResponse) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(64)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *GlobalBeginResponse) BatchEncode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.BatchEncode(buf)
	WriteXID(msg.XID, buf)
	WriteString(msg.ExtraData, buf)
}

// Decode decode message from byte buffer
func (msg *GlobalBeginResponse) Decode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.Decode(buf)
	msg.XID = ReadXID(buf)
	msg.ExtraData = ReadString(buf)
}

type abstractBranchEndResponse struct {
	abstractTransactionResponse
	XID          FragmentXID
	BranchID     uint64
	BranchStatus BranchStatus
}

// Encode encode message
func (msg *abstractBranchEndResponse) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(64)
	msg.BatchEncode(buf)
	return buf
}

func (msg *abstractBranchEndResponse) BatchEncode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.BatchEncode(buf)
	WriteXID(msg.XID, buf)
	buf.WriteUint64(msg.BranchID)
	buf.WriteByte(byte(msg.BranchStatus))
}

// Decode decode message from byte buffer
func (msg *abstractBranchEndResponse) Decode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.Decode(buf)
	msg.XID = ReadXID(buf)
	msg.BranchID = ReadUInt64(buf)
	msg.BranchStatus = BranchStatus(ReadByte(buf))
}

// MaybeDecode maybe decode message from byte buffer
func (msg *abstractBranchEndResponse) MaybeDecode(buf *goetty.ByteBuf) bool {
	if !msg.abstractTransactionResponse.MaybeDecode(buf) {
		return false
	}

	xid, ok := MaybeReadXID(buf)
	if !ok {
		return false
	}
	msg.XID = xid

	if buf.Readable() < 8 {
		return false
	}
	msg.BranchID = ReadUInt64(buf)

	if buf.Readable() < 1 {
		return false
	}
	msg.BranchStatus = BranchStatus(ReadByte(buf))
	return true
}

// BranchCommitResponse commit a branch transaction response
type BranchCommitResponse struct {
	abstractBranchEndResponse
}

// Type returns message type
func (msg *BranchCommitResponse) Type() uint16 {
	return TypeBranchCommitResult
}

// BranchRollbackResponse rollback a branch transaction response
type BranchRollbackResponse struct {
	abstractBranchEndResponse
}

// Type returns message type
func (msg *BranchRollbackResponse) Type() uint16 {
	return TypeBranchRollbackResult
}

type abstractGlobalEndResponse struct {
	abstractTransactionResponse
	GlobalStatus GlobalStatus
}

// Encode encode message
func (msg *abstractGlobalEndResponse) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(64)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *abstractGlobalEndResponse) BatchEncode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.BatchEncode(buf)
	buf.WriteByte(byte(msg.GlobalStatus))
}

// Decode decode message from byte buffer
func (msg *abstractGlobalEndResponse) Decode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.Decode(buf)
	msg.GlobalStatus = GlobalStatus(ReadByte(buf))
}

// GlobalCommitResponse commit a global transaction response
type GlobalCommitResponse struct {
	abstractGlobalEndResponse
}

// Type returns message type
func (msg *GlobalCommitResponse) Type() uint16 {
	return TypeGlobalCommitResult
}

// GlobalRollbackResponse rollback a global transaction response
type GlobalRollbackResponse struct {
	abstractGlobalEndResponse
}

// Type returns message type
func (msg *GlobalRollbackResponse) Type() uint16 {
	return TypeGlobalRollbackResult
}

// BranchRegisterResponse register a branch transaction response
type BranchRegisterResponse struct {
	abstractTransactionResponse
	BranchID uint64
}

// Type returns message type
func (msg *BranchRegisterResponse) Type() uint16 {
	return TypeBranchRegisterResult
}

// Encode encode message
func (msg *BranchRegisterResponse) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(64)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *BranchRegisterResponse) BatchEncode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.BatchEncode(buf)
	buf.WriteUInt64(msg.BranchID)
}

// Decode decode message from byte buffer
func (msg *BranchRegisterResponse) Decode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.Decode(buf)
	msg.BranchID = ReadUInt64(buf)
}

// BranchReportResponse report branch transaction status response
type BranchReportResponse struct {
	abstractTransactionResponse
}

// Type returns message type
func (msg *BranchReportResponse) Type() uint16 {
	return TypeBranchStatusReportResult
}

// GlobalStatusResponse global status response
type GlobalStatusResponse struct {
	abstractGlobalEndResponse
}

// Decode decode message from byte buffer
func (msg *BranchReportResponse) Decode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.Decode(buf)
}

// Type returns message type
func (msg *GlobalStatusResponse) Type() uint16 {
	return TypeGlobalStatusResult
}

// GlobalLockQueryResponse query global lock response
type GlobalLockQueryResponse struct {
	abstractTransactionResponse
	Lockable bool
}

// Type returns message type
func (msg *GlobalLockQueryResponse) Type() uint16 {
	return TypeGlobalLockQueryResult
}

// Encode encode message
func (msg *GlobalLockQueryResponse) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(512)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *GlobalLockQueryResponse) BatchEncode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.BatchEncode(buf)
	WriteBool(msg.Lockable, buf)
}

// Decode decode message from byte buffer
func (msg *GlobalLockQueryResponse) Decode(buf *goetty.ByteBuf) {
	msg.abstractTransactionResponse.Decode(buf)
	msg.Lockable = ReadBool(buf)
}

// MergeResultMessage merge message result
type MergeResultMessage struct {
	Msgs []Message
}

// Type returns message type
func (msg *MergeResultMessage) Type() uint16 {
	return TypeSeataMergeResult
}

// Encode encode message
func (msg *MergeResultMessage) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(1024 * len(msg.Msgs))
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *MergeResultMessage) BatchEncode(buf *goetty.ByteBuf) {
	start := buf.GetWriteIndex()
	written := buf.Readable()

	count := len(msg.Msgs)
	buf.WriteInt(0)
	buf.WriteUInt16(uint16(count))
	for _, m := range msg.Msgs {
		buf.WriteUInt16(m.Type())
		m.BatchEncode(buf)
	}
	writeIndex := buf.GetWriteIndex()

	size := buf.Readable() - written - 4
	buf.SetWriterIndex(start)
	buf.WriteInt(size)
	buf.SetWriterIndex(writeIndex)
}

// Decode decode message from byte buffer
func (msg *MergeResultMessage) Decode(buf *goetty.ByteBuf) {
	cnt := ReadUInt16(buf)
	for i := uint16(0); i < cnt; i++ {
		m := newMessageByType(ReadUInt16(buf))
		m.Decode(buf)
		msg.Msgs = append(msg.Msgs, m)
	}
}

type abstractIdentifyResponse struct {
	abstractResultMessage
	Version    string
	ExtraData  string
	Identified bool
}

// Encode encode message
func (msg *abstractIdentifyResponse) Encode() *goetty.ByteBuf {
	buf := goetty.NewByteBuf(512)
	msg.BatchEncode(buf)
	return buf
}

// BatchEncode encode message
func (msg *abstractIdentifyResponse) BatchEncode(buf *goetty.ByteBuf) {
	WriteBool(msg.Identified, buf)
	WriteString(msg.Version, buf)
}

// Decode decode message from byte buffer
func (msg *abstractIdentifyResponse) Decode(buf *goetty.ByteBuf) {
	msg.Identified = ReadBool(buf)
	msg.Version = ReadString(buf)
}

// MaybeDecode decode message from byte buffer
func (msg *abstractIdentifyResponse) MaybeDecode(buf *goetty.ByteBuf) bool {
	if buf.Readable() < 1 {
		return false
	}
	msg.Identified = ReadBool(buf)

	if value, ok := MaybeReadString(buf); ok {
		msg.Version = value
		return true
	}

	return false
}

// RegisterRMResponse register RM response
type RegisterRMResponse struct {
	abstractIdentifyResponse
}

// Type returns message type
func (msg *RegisterRMResponse) Type() uint16 {
	return TypeRegRMResult
}

// RegisterTMResponse register TM response
type RegisterTMResponse struct {
	abstractIdentifyResponse
}

// Type returns message type
func (msg *RegisterTMResponse) Type() uint16 {
	return TypeRegCltResult
}
