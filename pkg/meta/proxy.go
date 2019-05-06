package meta

import (
	"github.com/fagongzi/goetty"
)

// RouteableMessage wrap seata message for routing
type RouteableMessage struct {
	// ToStore direct send to this store
	ToStore uint64
	// RMSID which session
	RMSID string
	// ProxyAddr proxy addr
	ProxyAddr string
	// Seq seq
	Seq uint64
	// FID which fragment
	FID uint64
	// Internal server internal
	Internal bool
	// Batch is in batch
	Batch bool
	// MsgID msgID
	MsgID uint64
	// MsgType msg type
	MsgType uint16
	// Data seata origin message
	Data []byte
}

// ReadOriginMsg returns the origin msg
func (rm *RouteableMessage) ReadOriginMsg() Message {
	buf := goetty.NewByteBuf(len(rm.Data))
	buf.Write(rm.Data)
	msg := newMessageByType(rm.MsgType)
	msg.Decode(buf)
	buf.Release()
	return msg
}

// Encode returns bytes
func (rm *RouteableMessage) Encode() []byte {
	buf := goetty.NewByteBuf(256)
	pc.Encode(rm, buf)
	_, data, _ := buf.ReadAll()
	buf.Release()
	return data
}
