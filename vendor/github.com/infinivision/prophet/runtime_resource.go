package prophet

// ResourceRuntime resource runtime info
type ResourceRuntime struct {
	meta Resource

	leaderPeer   *Peer
	downPeers    []*PeerStats
	pendingPeers []*Peer
}

func newResourceRuntime(meta Resource, leader *Peer) *ResourceRuntime {
	return &ResourceRuntime{
		meta:       meta,
		leaderPeer: leader,
	}
}

// Clone returns a clone resource runtime
func (res *ResourceRuntime) Clone() *ResourceRuntime {
	value := &ResourceRuntime{}
	value.meta = res.meta.Clone()
	if res.leaderPeer != nil {
		value.leaderPeer = res.leaderPeer.Clone()
	}
	value.downPeers = make([]*PeerStats, len(res.downPeers))
	for idx, ps := range res.downPeers {
		value.downPeers[idx] = ps.Clone()
	}
	value.pendingPeers = make([]*Peer, len(res.pendingPeers))
	for idx, p := range res.pendingPeers {
		value.pendingPeers[idx] = p.Clone()
	}
	return value
}

// RemoveContainerPeer remove container peer
func (res *ResourceRuntime) RemoveContainerPeer(id uint64) {
	var peers []*Peer
	for _, peer := range res.meta.Peers() {
		if peer.ContainerID != id {
			peers = append(peers, peer)
		}
	}

	res.meta.SetPeers(peers)
}

// GetPendingPeer returns pending peer
func (res *ResourceRuntime) GetPendingPeer(peerID uint64) *Peer {
	for _, peer := range res.pendingPeers {
		if peer.ID == peerID {
			return peer
		}
	}
	return nil
}

// GetPeer return the peer
func (res *ResourceRuntime) GetPeer(peerID uint64) *Peer {
	for _, peer := range res.meta.Peers() {
		if peer.ID == peerID {
			return peer
		}
	}
	return nil
}

// GetContainerPeer returns the peer in the container
func (res *ResourceRuntime) GetContainerPeer(containerID uint64) *Peer {
	for _, peer := range res.meta.Peers() {
		if peer.ContainerID == containerID {
			return peer
		}
	}
	return nil
}

// GetContainerIDs returns all container id
func (res *ResourceRuntime) GetContainerIDs() map[uint64]struct{} {
	peers := res.meta.Peers()
	ids := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		ids[peer.ContainerID] = struct{}{}
	}
	return ids
}

func (res *ResourceRuntime) getFollowers() map[uint64]*Peer {
	peers := res.meta.Peers()
	followers := make(map[uint64]*Peer, len(peers))
	for _, p := range peers {
		if res.leaderPeer == nil || res.leaderPeer.ID != p.ID {
			followers[p.ContainerID] = p
		}
	}

	return followers
}
