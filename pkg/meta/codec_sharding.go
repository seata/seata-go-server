package meta

import (
	"fmt"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
)

const (
	hb     byte = 0
	hbACK  byte = 1
	remove byte = 2
)

var (
	// ShardingEncoder sharding encode
	ShardingEncoder = goetty.NewIntLengthFieldBasedEncoder(&shardingCodec{})
	// ShardingDecoder sharding decoder
	ShardingDecoder = goetty.NewIntLengthFieldBasedDecoder(&shardingCodec{})
)

type shardingCodec struct {
}

func (c *shardingCodec) Decode(in *goetty.ByteBuf) (bool, interface{}, error) {
	t, _ := in.ReadByte()
	switch t {
	case hb:
		msg := &HBMsg{}
		msg.Frag.ID = ReadUInt64(in)
		msg.Frag.Version = ReadUInt64(in)
		msg.Frag.Peers = ReadPeers(in)
		msg.Frag.DisableGrow = ReadBool(in)
		return true, msg, nil
	case hbACK:
		return true, &HBACKMsg{
			ID:      ReadUInt64(in),
			Version: ReadUInt64(in),
			Peer:    ReadPeer(in),
		}, nil
	case remove:
		return true, &RemoveMsg{
			ID: ReadUInt64(in),
		}, nil
	}

	return false, nil, fmt.Errorf("%d not support", t)
}

func (c *shardingCodec) Encode(data interface{}, out *goetty.ByteBuf) error {
	if msg, ok := data.(*HBMsg); ok {
		out.WriteByte(hb)
		out.WriteUInt64(msg.Frag.ID)
		out.WriteUInt64(msg.Frag.Version)
		WritePeers(msg.Frag.Peers, out)
		WriteBool(msg.Frag.DisableGrow, out)
	} else if msg, ok := data.(*HBACKMsg); ok {
		out.WriteByte(hbACK)
		out.WriteUInt64(msg.ID)
		out.WriteUInt64(msg.Version)
		WritePeer(msg.Peer, out)
	} else if msg, ok := data.(*RemoveMsg); ok {
		out.WriteByte(remove)
		out.WriteUInt64(msg.ID)
	} else {
		log.Fatalf("not support msg %T %+v",
			data,
			data)
	}

	return nil
}
