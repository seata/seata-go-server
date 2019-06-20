package sharding

import (
	"github.com/fagongzi/goetty"
)

// local is in (0x01, 0x03);
var (
	localPrefix    byte = 0x01
	localPrefixKey      = []byte{localPrefix}
)

// store
var (
	storeKey = []byte{localPrefix, 0x01}
)

// fragments is in (0x01 0x02, 0x01 0x03)
var (
	fragmentsPrefix = []byte{localPrefix, 0x02}
)

func getFragmentKey(id uint64) []byte {
	buf := goetty.NewByteBuf(len(fragmentsPrefix) + 8)
	buf.Write(fragmentsPrefix)
	buf.WriteUInt64(id)
	_, value, _ := buf.ReadAll()
	buf.Release()
	return value
}
