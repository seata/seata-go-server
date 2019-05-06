package meta

import (
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
)

var (
	pc = &proxyCodec{}
	// ProxyDecoder proxy routeable message decoder
	ProxyDecoder = goetty.NewIntLengthFieldBasedDecoder(pc)
	// ProxyEncoder proxy routeable message encoder
	ProxyEncoder = goetty.NewIntLengthFieldBasedEncoder(pc)
)

type proxyCodec struct {
}

func (c *proxyCodec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	msg := &RouteableMessage{}
	msg.RMSID = ReadString(in)
	msg.ProxyAddr = ReadString(in)
	msg.FID = ReadUInt64(in)
	msg.Seq = ReadUInt64(in)
	msg.Internal = ReadBool(in)
	msg.Batch = ReadBool(in)
	msg.MsgID = ReadUInt64(in)
	msg.MsgType = ReadUInt16(in)
	msg.Data = ReadSlice(in)

	return true, msg, nil
}

func (c *proxyCodec) Encode(data interface{}, out *goetty.ByteBuf) error {
	if msg, ok := data.(*RouteableMessage); ok {
		WriteString(msg.RMSID, out)
		WriteString(msg.ProxyAddr, out)
		out.WriteUInt64(msg.FID)
		out.WriteUInt64(msg.Seq)
		WriteBool(msg.Internal, out)
		WriteBool(msg.Batch, out)
		out.WriteUInt64(msg.MsgID)
		out.WriteUInt16(msg.MsgType)
		WriteSlice(msg.Data, out)
		return nil
	} else if msg, ok := data.([]byte); ok {
		out.Write(msg)
		return nil
	}

	log.Fatalf("unsupport codec type %T", data)
	return nil
}
