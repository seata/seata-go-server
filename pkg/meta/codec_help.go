package meta

import (
	"fmt"
	"strings"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/infinivision/prophet"
)

// WriteString write string value
func WriteString(value string, buf *goetty.ByteBuf) {
	if value != "" {
		buf.WriteUInt16(uint16(len(value)))
		buf.WriteString(value)
	} else {
		buf.WriteUInt16(0)
	}
}

// ReadString read string value
func ReadString(buf *goetty.ByteBuf) string {
	size := ReadUInt16(buf)
	if size == 0 {
		return ""
	}

	_, value, _ := buf.ReadBytes(int(size))
	return hack.SliceToString(value)
}

// MaybeReadString maybe read string value
func MaybeReadString(buf *goetty.ByteBuf) (string, bool) {
	if buf.Readable() < 2 {
		return "", false
	}

	size := ReadUInt16(buf)
	if size == 0 {
		return "", true
	}

	if buf.Readable() < int(size) {
		return "", false
	}

	_, value, _ := buf.ReadBytes(int(size))
	return hack.SliceToString(value), true
}

// WriteXID write xid value
func WriteXID(value FragmentXID, buf *goetty.ByteBuf) {
	WriteString(fmt.Sprintf("%d:%d", value.FragmentID, value.GID), buf)
}

// ReadXID read xid value
func ReadXID(buf *goetty.ByteBuf) FragmentXID {
	return parseXID(ReadString(buf))
}

// MaybeReadXID maybe read xid value
func MaybeReadXID(buf *goetty.ByteBuf) (FragmentXID, bool) {
	var value FragmentXID

	data, ok := MaybeReadString(buf)
	if !ok {
		return value, false
	}

	return parseXID(data), true
}

func parseXID(data string) FragmentXID {
	var value FragmentXID

	if data != "" {
		values := strings.Split(data, ":")
		value.FragmentID = format.MustParseStrUInt64(values[0])
		value.GID = format.MustParseStrUInt64(values[1])
	}

	return value
}

// WriteBigString write big string
func WriteBigString(value string, buf *goetty.ByteBuf) {
	if value != "" {
		buf.WriteInt(len(value))
		buf.WriteString(value)
	} else {
		buf.WriteInt(0)
	}
}

// ReadBigString read big string
func ReadBigString(buf *goetty.ByteBuf) string {
	size := ReadInt(buf)
	if size == 0 {
		return ""
	}

	_, value, _ := buf.ReadBytes(size)
	return hack.SliceToString(value)
}

// MaybeReadBigString maybe read string value
func MaybeReadBigString(buf *goetty.ByteBuf) (string, bool) {
	if buf.Readable() < 4 {
		return "", false
	}

	size := ReadInt(buf)
	if size == 0 {
		return "", true
	}

	if buf.Readable() < size {
		return "", false
	}

	_, value, _ := buf.ReadBytes(int(size))
	return hack.SliceToString(value), true
}

// ReadUInt64 read uint64 value
func ReadUInt64(buf *goetty.ByteBuf) uint64 {
	value, _ := buf.ReadUInt64()
	return value
}

// ReadUInt16 read uint16 value
func ReadUInt16(buf *goetty.ByteBuf) uint16 {
	value, _ := buf.ReadUInt16()
	return value
}

// ReadInt read int value
func ReadInt(buf *goetty.ByteBuf) int {
	value, _ := buf.ReadInt()
	return value
}

// ReadByte read byte value
func ReadByte(buf *goetty.ByteBuf) byte {
	value, _ := buf.ReadByte()
	return value
}

// ReadBytes read bytes value
func ReadBytes(n int, buf *goetty.ByteBuf) []byte {
	_, value, _ := buf.ReadBytes(n)
	return value
}

// WriteBool write bool value
func WriteBool(value bool, out *goetty.ByteBuf) {
	out.WriteByte(boolToByte(value))
}

// WritePeers write peers
func WritePeers(peers []prophet.Peer, out *goetty.ByteBuf) {
	out.WriteInt(len(peers))
	for _, peer := range peers {
		WritePeer(peer, out)
	}
}

// WritePeer write peer
func WritePeer(peer prophet.Peer, out *goetty.ByteBuf) {
	out.WriteUInt64(peer.ID)
	out.WriteUInt64(peer.ContainerID)
}

// ReadBool read bool
func ReadBool(in *goetty.ByteBuf) bool {
	value, _ := in.ReadByte()
	return byteToBool(value)
}

// ReadPeers read peers
func ReadPeers(in *goetty.ByteBuf) []prophet.Peer {
	var peers []prophet.Peer
	c, _ := in.ReadInt()
	for i := 0; i < c; i++ {
		peers = append(peers, ReadPeer(in))
	}

	return peers
}

// ReadPeer read peer
func ReadPeer(in *goetty.ByteBuf) prophet.Peer {
	peerID, _ := in.ReadUInt64()
	storeID, _ := in.ReadUInt64()
	return prophet.Peer{
		ID:          peerID,
		ContainerID: storeID,
	}
}

// WriteSlice write slice value
func WriteSlice(value []byte, buf *goetty.ByteBuf) {
	buf.WriteUInt16(uint16(len(value)))
	if len(value) > 0 {
		buf.Write(value)
	}
}

// ReadSlice read slice value
func ReadSlice(buf *goetty.ByteBuf) []byte {
	l, _ := buf.ReadUInt16()
	if l == 0 {
		return nil
	}

	_, data, _ := buf.ReadBytes(int(l))
	return data
}

func boolToByte(value bool) byte {
	if value {
		return 1
	}

	return 0
}

func byteToBool(value byte) bool {
	if value == 1 {
		return true
	}

	return false
}

// ParseLockKeys parse lock key
func ParseLockKeys(value string) ([]LockKey, error) {
	if value == "" {
		return nil, nil
	}

	tables := strings.Split(value, ";")
	if len(tables) == 0 {
		return nil, fmt.Errorf("wrong format of lock keys: %s", value)
	}

	var locks []LockKey
	for _, table := range tables {
		idx := strings.Index(table, ":")
		if idx <= 0 || idx == len(table)-1 {
			return nil, fmt.Errorf("wrong format of lock keys: %s", value)
		}

		pks := strings.Split(table[idx+1:], ",")
		if len(pks) == 0 {
			return nil, fmt.Errorf("wrong format of lock keys: %s", value)
		}

		for _, pk := range pks {
			locks = append(locks, NewLockKey(table[:idx], pk))
		}
	}

	return locks, nil
}
