package sharding

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/prophet"
	"github.com/infinivision/taas/pkg/core"
	"github.com/infinivision/taas/pkg/meta"
)

// PeerReplicate is the peer replicate. Every Fragment replicate has a PeerReplicate.
type PeerReplicate struct {
	sync.RWMutex

	tag           string
	id            uint64
	store         *Store
	peer          prophet.Peer
	frag          meta.Fragment
	pendingPeers  []prophet.Peer
	heartbeatsMap *sync.Map
	tc            core.TransactionCoordinator
	tasks         []uint64

	overloadTimes uint64
}

func createPeerReplicate(store *Store, frag meta.Fragment) (*PeerReplicate, error) {
	peer, ok := findPeer(frag.Peers, store.meta.ID)
	if !ok {
		return nil, fmt.Errorf("find no peer for store %d in frag %v",
			store.meta.ID,
			frag)
	}

	return newPeerReplicate(store, frag, peer), nil
}

func newPeerReplicate(store *Store, frag meta.Fragment, peer prophet.Peer) *PeerReplicate {
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

	opts := append(store.cfg.CoreOptions, core.WithStatusChangeAware(pr.becomeLeader, pr.becomeFollower))
	opts = append(opts, core.WithConcurrency(store.cfg.Concurrency))
	tc, err := core.NewCellTransactionCoordinator(frag.ID, peer.ID, store.seataTrans, opts...)
	if err != nil {
		log.Fatalf("%s init failed with %+v",
			pr.tag,
			err)
	}
	pr.tc = tc
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
	var values []prophet.Peer
	for _, p := range pr.frag.Peers {
		if p.ID != peer.ID {
			values = append(values, p)
		}
	}

	pr.frag.Peers = values
	pr.heartbeatsMap.Delete(peer.ID)
	pr.frag.Version++
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
	pr.stopTasks(false)
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

func (pr *PeerReplicate) becomeLeader() {
	pr.stopTasks(true)
	// TODO: too many concurrency
	pr.addTask(pr.startHB)
	pr.addTask(pr.startCheckConcurrency)
}

func (pr *PeerReplicate) becomeFollower() {
	pr.stopTasks(true)
	log.Infof("[frag-%d]: all tasks stopped",
		pr.id)
}

func (pr *PeerReplicate) stopTasks(lock bool) {
	if lock {
		pr.Lock()
		defer pr.Unlock()
	}

	for _, id := range pr.tasks {
		pr.store.runner.StopCancelableTask(id)
	}
}

func (pr *PeerReplicate) addTask(task func(ctx context.Context)) {
	pr.Lock()
	defer pr.Unlock()

	id, err := pr.store.runner.RunCancelableTask(task)
	if err != nil {
		log.Fatalf("%s add task failed with %+v",
			pr.tag,
			err)
	}
	pr.tasks = append(pr.tasks, id)
}

func (pr *PeerReplicate) startHB(ctx context.Context) {
	ticker := time.NewTicker(pr.store.cfg.MaxPeerDownDuration / 5)
	defer ticker.Stop()

	log.Infof("%s start hb task", pr.tag)
	for {
		select {
		case <-ctx.Done():
			log.Infof("%s hb task stopped", pr.tag)
			return
		case <-ticker.C:
			pr.doHB()
		}
	}
}

func (pr *PeerReplicate) doHB() {
	pr.RLock()
	defer pr.RUnlock()

	for _, p := range pr.frag.Peers {
		if p.ContainerID != pr.store.meta.ID {
			pr.store.trans.sendMsg(p.ContainerID, &meta.HBMsg{
				Frag: pr.frag,
			})
		}
	}
}

func (pr *PeerReplicate) startCheckConcurrency(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	count := uint64(0)
	log.Infof("%s start check concurrency task", pr.tag)
	for {
		if pr.frag.DisableGrow {
			log.Infof("%s disable grow", pr.tag)
			return
		}

		select {
		case <-ctx.Done():
			log.Infof("%s check concurrency task stopped", pr.tag)
			return
		case <-ticker.C:
			pr.doCheck()
		}

		count++
		if count == pr.store.cfg.OverloadPeriod {
			if pr.maybeGrow() {
				log.Infof("%s already grow, check concurrency task stopped",
					pr.tag)
				return
			}
			count = 0
		}
	}
}

func (pr *PeerReplicate) doCheck() {
	if pr.tc.ActiveGCount() > pr.store.cfg.Concurrency {
		pr.overloadTimes++
	}
}

func (pr *PeerReplicate) maybeGrow() bool {
	if pr.overloadTimes == 0 {
		return false
	}

	if pr.overloadTimes*100/pr.store.cfg.OverloadPeriod <= pr.store.cfg.OverloadPercentage {
		return false
	}

	pr.Lock()
	defer pr.Unlock()

	pr.overloadTimes = 0
	pr.frag.DisableGrow = true
	pr.frag.Version++
	pr.store.mustUpdateFragmentOnStore(pr.frag, pr.peer)

	frag := pr.store.createFristFragment()
	newPR, err := createPeerReplicate(pr.store, frag)
	if err != nil {
		log.Fatalf("%s: new pr on grow failed with %+v",
			pr.tag,
			err)
	}
	pr.store.doAddPR(newPR)
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
