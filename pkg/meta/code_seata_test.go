package meta

import (
	"testing"

	"github.com/fagongzi/goetty"
)

func TestMaybe(t *testing.T) {
	buf := goetty.NewByteBuf(128)
	buf.Write([]byte{1, 0, 0, 14, 50, 54, 58, 54, 51, 51, 53, 49, 52, 56, 56, 53, 49, 51, 0, 0, 0, 0, 0})
	req := AcquireBranchCommitResponse()
	value := req.MaybeDecode(buf)
	if value {
		t.Errorf("error decode")
	}
}

func TestWrap(t *testing.T) {
	buf := goetty.NewByteBuf(512)

	rpc := AcquireRPCMessage()
	rpc.Header.MsgID = 100
	rpc.Header.Flag |= FlagRequest
	rpc.Header.Flag |= FlagSeataCodec
	msg := AcquireMergedWarpMessage()
	req := AcquireBranchReportRequest()
	req.BranchID = 1
	msg.MsgIDs = append(msg.MsgIDs, 1)
	msg.Msgs = append(msg.Msgs, req)
	rpc.Header.MsgType = msg.Type()
	rpc.Body = msg
	err := SeataEncoder.Encode(rpc, buf)
	if err != nil {
		t.Errorf("encode wrap msg failed with %+v", err)
		return
	}

	ok, _, err := SeataEncoder.Decode(buf)
	if err != nil {
		t.Errorf("decode wrap msg failed with %+v", err)
		return
	}

	if !ok {
		t.Errorf("decode wrap failed with data not enough")
		return
	}
}
