package meta

import (
	"fmt"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
)

// alibaba seata mesage format
// header: magic number(2 bytes) + flag(2 bytes) + mesageType(2 bytes) + message id(8 bytes) + body

const (
	magic        uint16 = 0xdada
	headerLength        = 14

	// FlagRequest flag request
	FlagRequest uint16 = 0x80
	// FlagAsync flag async
	FlagAsync uint16 = 0x40
	// FlagHeartbeat flag hb
	FlagHeartbeat uint16 = 0x20
	// FlagSeataCodec seata codec
	FlagSeataCodec uint16 = 0x10
)

var (
	fc = &seataCodec{}
	// SeataDecoder seata decoder
	SeataDecoder = fc
	// SeataEncoder seata encoder
	SeataEncoder = fc
	// SeataHB heartbeat msg
	SeataHB = &HeartbeatMessage{}
)

type seataCodec struct {
}

func (c *seataCodec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	if in.Readable() < headerLength {
		return false, nil, nil
	}

	buf := in.RawBuf()
	if goetty.Byte2UInt16(buf) != magic {
		return false, nil, fmt.Errorf("codec decode not found magic offset")
	}

	index := in.GetReaderIndex()

	// skip magic
	in.Skip(2)
	flag := ReadUInt16(in)
	msgType := ReadUInt16(in)
	msgID := ReadUInt64(in)

	if checkFlag(flag, FlagHeartbeat) {
		msg := AcquireRPCMessage()
		msg.Header.Flag = flag
		msg.Header.MsgID = msgID
		msg.Header.MsgType = msgType
		msg.Body = SeataHB
		return true, msg, nil
	}

	msg := AcquireRPCMessage()
	msg.Header.Flag = flag
	msg.Header.MsgID = msgID
	msg.Header.MsgType = msgType

	switch msgType {
	case TypeSeataMerge:
		if !enoughN(in, 4, index) {
			return false, nil, nil
		}

		size := ReadInt(in)
		if !enoughN(in, size, index) {
			return false, nil, nil
		}

		msg.Body = AcquireMergedWarpMessage()
		msg.Body.Decode(in)
		break
	case TypeSeataMergeResult:
		if !enoughN(in, 4, index) {
			return false, nil, nil
		}

		size := ReadInt(in)
		if !enoughN(in, size, index) {
			return false, nil, nil
		}

		msg.Body = AcquireMergeResultMessage()
		msg.Body.Decode(in)
		break
	case TypeRegClt:
		req := AcquireRegisterTMRequest()
		if !req.MaybeDecode(in) {
			in.SetReaderIndex(index)
			return false, nil, nil
		}

		msg.Body = req
		break
	case TypeRegRM:
		req := AcquireRegisterRMRequest()
		if !req.MaybeDecode(in) {
			in.SetReaderIndex(index)
			return false, nil, nil
		}

		msg.Body = req
		break
	case TypeRegRMResult:
		req := AcquireRegisterRMResponse()
		if !req.MaybeDecode(in) {
			in.SetReaderIndex(index)
			return false, nil, nil
		}

		msg.Body = req
		break
	case TypeBranchRollback:
		req := AcquireBranchRollbackRequest()
		if !req.MaybeDecode(in) {
			in.SetReaderIndex(index)
			return false, nil, nil
		}

		msg.Body = req
		break
	case TypeBranchRollbackResult:
		req := AcquireBranchRollbackResponse()
		if !req.MaybeDecode(in) {
			in.SetReaderIndex(index)
			return false, nil, nil
		}

		msg.Body = req
	case TypeBranchCommit:
		req := AcquireBranchCommitRequest()
		if !req.MaybeDecode(in) {
			in.SetReaderIndex(index)
			return false, nil, nil
		}

		msg.Body = req
		break
	case TypeBranchCommitResult:
		req := AcquireBranchCommitResponse()
		if !req.MaybeDecode(in) {
			in.SetReaderIndex(index)
			return false, nil, nil
		}

		msg.Body = req
	default:
		return false, nil, fmt.Errorf("unsupport msg type %d", msgType)
	}

	return true, msg, nil
}

func (c *seataCodec) Encode(data interface{}, out *goetty.ByteBuf) error {
	if msg, ok := data.(*RPCMessage); ok {
		out.WriteUInt16(magic)
		out.WriteUInt16(msg.Header.Flag)
		out.WriteUInt16(msg.Header.MsgType)
		out.WriteUInt64(msg.Header.MsgID)
		if msg.Header.MsgType != TypeHeartbeat {
			msg.Body.BatchEncode(out)
		}
		return nil
	}

	log.Fatalf("unsupport codec type %T", data)
	return nil
}

func enoughN(in *goetty.ByteBuf, n, backup int) bool {
	if in.Readable() < n {
		in.SetReaderIndex(backup)
		return false
	}

	return true
}

func checkFlag(value uint16, expect uint16) bool {
	return value&expect > 0
}

// RPCMessage seata rpc message
type RPCMessage struct {
	Header Header
	Body   Message
}

// Header seata message header
type Header struct {
	Flag    uint16
	MsgType uint16
	MsgID   uint64
}

func newMessageByType(messageType uint16) Message {
	switch messageType {
	case TypeRegRM:
		return AcquireRegisterRMRequest()
	case TypeRegRMResult:
		return AcquireRegisterRMResponse()
	case TypeRegClt:
		return AcquireRegisterTMRequest()
	case TypeRegCltResult:
		return AcquireRegisterTMResponse()
	case TypeGlobalBegin:
		return AcquireGlobalBeginRequest()
	case TypeGlobalBeginResult:
		return AcquireGlobalBeginResponse()
	case TypeGlobalCommit:
		return AcquireGlobalCommitRequest()
	case TypeGlobalCommitResult:
		return AcquireGlobalCommitResponse()
	case TypeGlobalRollback:
		return AcquireGlobalRollbackRequest()
	case TypeGlobalRollbackResult:
		return AcquireGlobalRollbackResponse()
	case TypeGlobalStatus:
		return AcquireGlobalStatusRequest()
	case TypeGlobalStatusResult:
		return AcquireGlobalStatusResponse()
	case TypeGlobalLockQuery:
		return AcquireGlobalLockQueryRequest()
	case TypeGlobalLockQueryResult:
		return AcquireGlobalLockQueryResponse()
	case TypeBranchRegister:
		return AcquireBranchRegisterRequest()
	case TypeBranchRegisterResult:
		return AcquireBranchRegisterResponse()
	case TypeBranchStatusReport:
		return AcquireBranchReportRequest()
	case TypeBranchStatusReportResult:
		return AcquireBranchReportResponse()
	case TypeBranchRollback:
		return AcquireBranchRollbackRequest()
	case TypeBranchRollbackResult:
		return AcquireBranchRollbackResponse()
	case TypeBranchCommit:
		return AcquireBranchRollbackRequest()
	case TypeBranchCommitResult:
		return AcquireBranchCommitResponse()
	case TypeRetryNotLeader:
		return AcquireRetryNotLeaderMessage()
	default:
		log.Fatalf("%d not support", messageType)
		return nil
	}
}
