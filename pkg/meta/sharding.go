package meta

import (
	"github.com/infinivision/prophet"
)

// HBMsg sharding hb message
type HBMsg struct {
	Frag Fragment
}

// HBACKMsg sharding hb ack message
type HBACKMsg struct {
	ID      uint64
	Version uint64
	Peer    prophet.Peer
}

// RemoveMsg remove msg
type RemoveMsg struct {
	ID uint64
}

// Fragment A shard is a logical processing unit.
// Each shard handles a certain number of global transactions.
// Exceeding the processing power of the logical unit,
// a new shard is generated to process.
type Fragment struct {
	ID          uint64         `json:"id"`
	Peers       []prophet.Peer `json:"peers"`
	Version     uint64         `json:"version"`
	DisableGrow bool           `json:"disableGrow"`
}

// StoreMeta is the container of Fragments
type StoreMeta struct {
	ID         uint64         `json:"id"`
	Addr       string         `json:"addr"`
	ClientAddr string         `json:"cliAddr"`
	Labels     []prophet.Pair `json:"lables"`
}
