package prophet

// ResourceKind distinguishes different kinds of resources.
type ResourceKind int

const (
	// LeaderKind leader
	LeaderKind ResourceKind = iota
	// ReplicaKind replica of resource
	ReplicaKind
)

// State is the state
type State int

const (
	// UP is normal state
	UP State = iota
	// Down is the unavailable state
	Down
	// Tombstone is the destory state
	Tombstone
)

// Serializable serializable
type Serializable interface {
}

type codecSerializable interface {
	Serializable
	Prepare() error
	Init(adapter Adapter) error
}

// Peer is the resource peer
type Peer struct {
	ID          uint64
	ContainerID uint64
}

// Clone returns a clone value
func (p *Peer) Clone() *Peer {
	return &Peer{
		ID:          p.ID,
		ContainerID: p.ContainerID,
	}
}

// PeerStats peer stats
type PeerStats struct {
	Peer        *Peer
	DownSeconds uint64
}

// Clone returns a clone value
func (ps *PeerStats) Clone() *PeerStats {
	return &PeerStats{
		Peer:        ps.Peer.Clone(),
		DownSeconds: ps.DownSeconds,
	}
}

// Resource resource
type Resource interface {
	Serializable

	ID() uint64
	Peers() []*Peer
	SetPeers(peers []*Peer)
	Stale(other Resource) bool
	Changed(other Resource) bool
	Clone() Resource

	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

// Pair key value pair
type Pair struct {
	Key, Value string
}

// Container is the resource container, the resource is running on the container
// the container is usually a node
type Container interface {
	Serializable

	ID() uint64
	Lables() []Pair
	State() State

	Clone() Container

	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}
