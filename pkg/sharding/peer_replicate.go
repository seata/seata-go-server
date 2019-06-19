package sharding

import (
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/prophet"
	"seata.io/server/pkg/core"
	"seata.io/server/pkg/meta"
)

// PeerReplicate is the fragment peer replicatation.
// Every Fragment has N replicatation in N stores.
type PeerReplicate struct {
	sync.RWMutex

	tag           string
	id            uint64
	workerID      uint64
	store         Store
	peer          prophet.Peer
	frag          meta.Fragment
	pendingPeers  []prophet.Peer
	heartbeatsMap *sync.Map
	tc            core.TransactionCoordinator
	tasks         []uint64

	overloadTimes, overloadCheckTimes uint64
}

func createPeerReplicate(store Store, frag meta.Fragment) (*PeerReplicate, error) {
	peer, ok := findPeer(frag.Peers, store.Meta().ID)
	if !ok {
		return nil, fmt.Errorf("find no peer for store %d in frag %v",
			store.Meta().ID,
			frag)
	}

	return newPeerReplicate(store, frag, peer), nil
}

func newPeerReplicate(store Store, frag meta.Fragment, peer prophet.Peer) *PeerReplicate {
	tag := fmt.Sprintf("[frag-%d]:", frag.ID)
	if peer.ID == 0 {
		log.Fatalf("%s invalid peer id 0",
			tag)
	}

	pr := new(PeerReplicate)
	pr.tag = tag
	pr.id = frag.ID
	pr.frag = frag
	pr.peer = peer
	pr.store = store
	pr.heartbeatsMap = &sync.Map{}

	if store.Cfg().tc != nil {
		pr.tc = store.Cfg().tc
	} else {
		opts := append(store.Cfg().CoreOptions, core.WithConcurrency(store.Cfg().Concurrency))
		tc, err := core.NewCellTransactionCoordinator(frag.ID, peer.ID, store.Transport(), opts...)
		if err != nil {
			log.Fatalf("%s init failed with %+v",
				pr.tag,
				err)
		}
		pr.tc = tc
	}

	log.Infof("%s created with %+v",
		pr.tag,
		pr.frag)
	return pr
}

func (pr *PeerReplicate) isLeader() bool {
	return pr.tc.IsLeader()
}

func (pr *PeerReplicate) addPeer(peer prophet.Peer) {
	_, ok := findPeer(pr.frag.Peers, peer.ContainerID)
	if ok {
		return
	}

	pr.heartbeatsMap.Store(peer.ID, time.Now())
	pr.frag.Peers = append(pr.frag.Peers, peer)
	pr.pendingPeers = append(pr.pendingPeers, peer)
	pr.frag.Version++
}

func (pr *PeerReplicate) removePeer(peer prophet.Peer) {
	removed := false
	var values []prophet.Peer
	for _, p := range pr.frag.Peers {
		if p.ID != peer.ID {
			values = append(values, p)
		} else {
			removed = true
		}
	}

	if removed {
		pr.frag.Peers = values
		pr.heartbeatsMap.Delete(peer.ID)
		pr.frag.Version++
	}
}

func (pr *PeerReplicate) removePendingPeer(peer prophet.Peer) {
	var values []prophet.Peer
	for _, p := range pr.pendingPeers {
		if p.ID != peer.ID {
			values = append(values, p)
		}
	}

	pr.pendingPeers = values
}

func (pr *PeerReplicate) destroy() {
	pr.tc.Stop()
}

func (pr *PeerReplicate) collectPendingPeers() []*prophet.Peer {
	var values []*prophet.Peer
	for _, peer := range pr.pendingPeers {
		p := peer
		values = append(values, &p)
	}
	return values
}

func (pr *PeerReplicate) collectDownPeers(maxDuration time.Duration) []*prophet.PeerStats {
	now := time.Now()
	var downPeers []*prophet.PeerStats
	for _, p := range pr.frag.Peers {
		if p.ID == pr.peer.ID {
			continue
		}

		if last, ok := pr.heartbeatsMap.Load(p.ID); ok {
			missing := now.Sub(last.(time.Time))
			if missing >= maxDuration {
				state := &prophet.PeerStats{}
				state.Peer = &p
				state.DownSeconds = uint64(missing.Seconds())
				downPeers = append(downPeers, state)
			}
		}
	}
	return downPeers
}

func (pr *PeerReplicate) doHB() {
	pr.RLock()
	defer pr.RUnlock()

	for _, p := range pr.frag.Peers {
		if p.ContainerID != pr.store.Meta().ID {
			pr.store.ShardingTransport().Send(p.ContainerID, &meta.HBMsg{
				Frag: pr.frag,
			})
		}
	}
}

func (pr *PeerReplicate) doCheckConcurrency() {
	if pr.frag.DisableGrow {
		log.Infof("%s disable grow", pr.tag)
		return
	}

	log.Debugf("%s start check concurrency task", pr.tag)

	if pr.tc.ActiveGCount() > pr.store.Cfg().Concurrency {
		pr.overloadTimes++
	}

	pr.overloadCheckTimes++
	if pr.overloadCheckTimes == pr.store.Cfg().OverloadPeriod {
		if pr.maybeGrow() {
			log.Infof("%s already grow, check concurrency task stopped",
				pr.tag)
			return
		}
		pr.overloadCheckTimes = 0
	}
}

func (pr *PeerReplicate) maybeGrow() bool {
	if pr.overloadTimes == 0 {
		return false
	}

	if pr.overloadTimes*100/pr.store.Cfg().OverloadPeriod <= pr.store.Cfg().OverloadPercentage {
		return false
	}

	pr.Lock()
	defer pr.Unlock()

	pr.overloadTimes = 0
	pr.frag.DisableGrow = true
	pr.frag.Version++
	pr.store.MustUpdateFragment(pr.peer.ContainerID, pr.frag)

	frag := pr.store.CreateFragment()
	newPR, err := createPeerReplicate(pr.store, frag)
	if err != nil {
		log.Fatalf("%s: new pr on grow failed with %+v",
			pr.tag,
			err)
	}
	pr.store.AddReplicate(newPR)
	return true
}

func findPeer(peers []prophet.Peer, storeID uint64) (prophet.Peer, bool) {
	for _, peer := range peers {
		if peer.ContainerID == storeID {
			return peer, true
		}
	}

	return prophet.Peer{}, false
}
