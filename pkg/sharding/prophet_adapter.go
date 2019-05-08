package sharding

import (
	"fmt"
	"time"

	"github.com/fagongzi/log"
	"github.com/fagongzi/util/json"
	"github.com/infinivision/prophet"
	"github.com/infinivision/taas/pkg/meta"
	"github.com/infinivision/taas/pkg/metrics"
	"github.com/infinivision/taas/pkg/util"
)

// ProphetAdapter adapter prophet
type ProphetAdapter struct {
	store *Store
}

// NewResource return a new resource
func (pa *ProphetAdapter) NewResource() prophet.Resource {
	return &ResourceAdapter{}
}

// NewContainer return a new container
func (pa *ProphetAdapter) NewContainer() prophet.Container {
	return &ContainerAdapter{}
}

// FetchResourceHB fetch resource HB
func (pa *ProphetAdapter) FetchResourceHB(id uint64) *prophet.ResourceHeartbeatReq {
	pr := pa.store.getFragment(id, true)
	if pr == nil {
		return nil
	}

	return getResourceHB(pr)
}

// FetchLeaderResources fetch all local leader resources
func (pa *ProphetAdapter) FetchLeaderResources() []uint64 {
	var values []uint64
	pa.store.replicates.Range(func(key, value interface{}) bool {
		pr := value.(*PeerReplicate)
		if pr.isLeader() {
			values = append(values, pr.id)
		}

		return true
	})

	return values
}

// FetchContainerHB fetch container HB
func (pa *ProphetAdapter) FetchContainerHB() *prophet.ContainerHeartbeatReq {
	stats, err := util.MemStats()
	if err != nil {
		log.Errorf("fetch store heartbeat failed with %+v",
			err)
		return nil
	}

	st := pa.store.getFragmentStatus()
	req := new(prophet.ContainerHeartbeatReq)
	req.Container = &ContainerAdapter{meta: pa.store.meta}
	req.StorageCapacity = stats.Total
	req.StorageAvailable = stats.Available
	req.StorageCapacity = 100
	req.StorageAvailable = 100
	req.LeaderCount = st.fragmentLeaderCount
	req.ReplicaCount = st.fragmentCount
	req.Busy = false

	metrics.FragmentGauge.WithLabelValues(metrics.RoleLeader).Set(float64(st.fragmentLeaderCount))
	metrics.FragmentGauge.WithLabelValues(metrics.RoleFollower).Set(float64(st.fragmentCount - st.fragmentLeaderCount))
	return req
}

// ResourceHBInterval fetch resource HB interface
func (pa *ProphetAdapter) ResourceHBInterval() time.Duration {
	return pa.store.cfg.FragHBInterval
}

// ContainerHBInterval fetch container HB interface
func (pa *ProphetAdapter) ContainerHBInterval() time.Duration {
	return pa.store.cfg.StoreHBInterval
}

// HBHandler HB hander
func (pa *ProphetAdapter) HBHandler() prophet.HeartbeatHandler {
	return pa
}

// ChangeLeader prophet adapter
func (pa *ProphetAdapter) ChangeLeader(resourceID uint64, newLeader *prophet.Peer) {
	log.Infof("[frag-%d]: schedule change leader to peer %+v ",
		resourceID,
		newLeader)

	pr := pa.store.getFragment(resourceID, true)
	if pr != nil {
		pr.tc.ChangeLeaderTo(newLeader.ID)
	}
}

// ChangePeer prophet adapter
func (pa *ProphetAdapter) ChangePeer(resourceID uint64, peer *prophet.Peer, changeType prophet.ChangePeerType) {
	if changeType == prophet.AddPeer {
		log.Infof("[frag-%d]: schedule add peer %+v ",
			resourceID,
			peer)
		pa.store.addPeer(resourceID, *peer)
	} else if changeType == prophet.RemovePeer {
		log.Infof("[frag-%d]: schedule remove peer %+v ",
			resourceID,
			peer)
		pa.store.removePeer(resourceID, *peer)
	}
}

// ContainerAdapter adapter for prophet's container and store
type ContainerAdapter struct {
	meta meta.StoreMeta
}

// ID adapter prophet
func (c *ContainerAdapter) ID() uint64 {
	return c.meta.ID
}

// Lables adapter prophet
func (c *ContainerAdapter) Lables() []prophet.Pair {
	return c.meta.Labels
}

// State adapter prophet
func (c *ContainerAdapter) State() prophet.State {
	return prophet.UP
}

// Clone adapter prophet
func (c *ContainerAdapter) Clone() prophet.Container {
	value := meta.StoreMeta{}
	json.MustUnmarshal(&value, json.MustMarshal(&c.meta))

	return &ContainerAdapter{
		meta: value,
	}
}

// Marshal adapter prophet
func (c *ContainerAdapter) Marshal() ([]byte, error) {
	return json.MustMarshal(&c.meta), nil
}

// Unmarshal adapter prophet
func (c *ContainerAdapter) Unmarshal(data []byte) error {
	json.MustUnmarshal(&c.meta, data)
	return nil
}

// ResourceAdapter adapter for prophet's resource and db
type ResourceAdapter struct {
	meta meta.Fragment
}

// ID adapter prophet
func (r *ResourceAdapter) ID() uint64 {
	return r.meta.ID
}

// Peers adapter prophet
func (r *ResourceAdapter) Peers() []*prophet.Peer {
	var values []*prophet.Peer
	for _, peer := range r.meta.Peers {
		p := peer
		values = append(values, &p)
	}
	return values
}

// SetPeers adapter prophet
func (r *ResourceAdapter) SetPeers(peers []*prophet.Peer) {
	var values []prophet.Peer
	for _, peer := range peers {
		values = append(values, *peer)
	}
	r.meta.Peers = values
}

// Stale adapter prophet
func (r *ResourceAdapter) Stale(other prophet.Resource) bool {
	otherVersion := other.(*ResourceAdapter).meta.Version
	return otherVersion < r.meta.Version
}

// Changed adapter prophet
func (r *ResourceAdapter) Changed(other prophet.Resource) bool {
	otherVersion := other.(*ResourceAdapter).meta.Version
	return otherVersion > r.meta.Version
}

// Clone adapter prophet
func (r *ResourceAdapter) Clone() prophet.Resource {
	value := meta.Fragment{}
	json.MustUnmarshal(&value, json.MustMarshal(&r.meta))

	return &ResourceAdapter{
		meta: value,
	}
}

// Marshal adapter prophet
func (r *ResourceAdapter) Marshal() ([]byte, error) {
	return json.MustMarshal(&r.meta), nil
}

// Unmarshal adapter prophet
func (r *ResourceAdapter) Unmarshal(data []byte) error {
	json.MustUnmarshal(&r.meta, data)
	return nil
}

func getResourceHB(pr *PeerReplicate) *prophet.ResourceHeartbeatReq {
	pr.RLock()
	defer pr.RUnlock()

	req := new(prophet.ResourceHeartbeatReq)
	req.Resource = &ResourceAdapter{meta: pr.frag}
	req.LeaderPeer = &pr.peer
	req.PendingPeers = pr.collectPendingPeers()
	req.DownPeers = pr.collectDownPeers(pr.store.cfg.MaxPeerDownDuration)

	metrics.FragmentPeersGauge.WithLabelValues(fmt.Sprintf("%d", pr.id)).Set(float64(len(pr.frag.Peers)))
	return req
}

type fragmentStatus struct {
	fragmentCount, fragmentLeaderCount uint64
}

func (s *Store) getFragmentStatus() fragmentStatus {
	st := fragmentStatus{}
	s.replicates.Range(func(key, value interface{}) bool {
		pr := value.(*PeerReplicate)
		st.fragmentCount++

		if pr.isLeader() {
			st.fragmentLeaderCount++
		}

		return true
	})

	return st
}
