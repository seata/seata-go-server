package sharding

// local is in (0x01, 0x02);
var (
	localPrefix    byte = 0x01
	localPrefixKey      = []byte{localPrefix}
)

// store
var (
	storeKey = []byte{localPrefix, 0x01}
)
