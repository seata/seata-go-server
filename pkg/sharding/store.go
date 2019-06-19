package sharding

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/log"
	"github.com/fagongzi/util/json"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/prophet"
	"seata.io/server/pkg/meta"
	"seata.io/server/pkg/transport"
)

// Store is a container of fragments, which maintains a set of fragments
type Store interface {
	// Meta returns the current store's metadata
	Meta() meta.StoreMeta
	// Cfg returns the configuration
	Cfg() Cfg
	// Start start all fragments managed by the store
	Start()

	// FragmentsState returns the state of the fragments
	FragmentsState() FragmentsState
	// GetStoreAddr returns the store address
	GetStoreAddr(storeID uint64) (string, error)

	// LeaderPeer returns the fragment's leader peer
	LeaderPeer(fid uint64) (prophet.Peer, error)
	// CreateFragment create a new fragment and save it to the local data
	CreateFragment() meta.Fragment
	// MustUpdateFragment update the store's fragment metadata
	MustUpdateFragment(storeID uint64, frag meta.Fragment)
	// GetFragment returns a fragment replicatation from the store,
	// when `leader` is true, only return the leader replicatation
	GetFragment(fid uint64, leader bool) *PeerReplicate

	// ForeachReplicate do something on every `replicatations`, break if the funcation return false
	ForeachReplicate(func(*PeerReplicate) bool)
	// AddReplicate add a replicatation
	AddReplicate(*PeerReplicate)
	// AddPeer add a peer to the exist fragment
	AddPeer(fid uint64, peer prophet.Peer)
	// RemovePeer remove the peer from the exist fragment
	RemovePeer(uint64, prophet.Peer)

	// Transport returns the seata message transport
	Transport() transport.Transport
	// ShardingTransport returns the sharding message transport
	ShardingTransport() Transport

	// HandleShardingMsg handle the sharding message, maybe returns a response.
	HandleShardingMsg(data interface{}) interface{}

	// AddRM add a resource manager
	AddRM(rms meta.ResourceManagerSet)
	// RenewRMLease renew the resource manager's lease
	RenewRMLease(pid, sid string)
}

type store struct {
	sync.RWMutex

	cfg        Cfg
	meta       meta.StoreMeta
	replicates *sync.Map
	pd         *prophet.Prophet
	bootOnce   *sync.Once
	pdStartedC chan struct{}
	runner     *task.Runner

	storage    storage
	trans      Transport
	seataTrans transport.Transport

	resources map[string][]meta.ResourceManager
	ops       uint64
}

// NewStore returns store with cfg
func NewStore(cfg Cfg) Store {
	s := new(store)
	s.cfg = cfg
	s.meta = meta.StoreMeta{
		Addr:       cfg.ShardingAddr,
		ClientAddr: cfg.Addr,
		Labels:     cfg.Labels,
	}

	s.resources = make(map[string][]meta.ResourceManager)
	s.replicates = &sync.Map{}
	s.bootOnce = &sync.Once{}

	s.runner = task.NewRunner()

	if s.cfg.storeID != 0 {
		s.meta.ID = s.cfg.storeID
	}

	if s.cfg.storage != nil {
		s.storage = s.cfg.storage
	}

	if cfg.shardingTrans != nil {
		s.trans = cfg.shardingTrans
	} else {
		s.trans = newShardingTransport(s)
	}

	if cfg.seataTrans != nil {
		s.seataTrans = s.cfg.seataTrans
	} else {
		s.seataTrans = transport.NewTransport(cfg.TransWorkerCount, s.rmAddrDetecter, cfg.TransSendCB)
	}

	return s
}

func (s *store) Meta() meta.StoreMeta {
	return s.meta
}

func (s *store) Cfg() Cfg {
	return s.cfg
}

func (s *store) LeaderPeer(fid uint64) (prophet.Peer, error) {
	pr := s.GetFragment(fid, false)
	if pr == nil {
		return prophet.Peer{}, nil
	}

	leader, err := pr.tc.CurrentLeader()
	if err != nil {
		return prophet.Peer{}, err
	}

	var storeID uint64
	for _, p := range pr.frag.Peers {
		if p.ID == leader {
			storeID = p.ContainerID
			break
		}
	}

	return prophet.Peer{ID: leader, ContainerID: storeID}, nil
}

func (s *store) GetStoreAddr(storeID uint64) (string, error) {
	c, err := s.pd.GetStore().GetContainer(storeID)
	if err != nil {
		return "", err
	}

	return c.(*ContainerAdapter).meta.Addr, nil
}

func (s *store) ForeachReplicate(fn func(*PeerReplicate) bool) {
	s.replicates.Range(func(key, value interface{}) bool {
		return fn(value.(*PeerReplicate))
	})
}

func (s *store) Transport() transport.Transport {
	return s.seataTrans
}

func (s *store) ShardingTransport() Transport {
	return s.trans
}

func (s *store) Start() {
	s.startProphet()
	log.Infof("begin to start store %d", s.meta.ID)

	s.trans.Start()
	log.Infof("peer transport start at %s", s.cfg.ShardingAddr)

	s.seataTrans.Start()
	log.Infof("seata transport start")

	s.startFragments()
	log.Infof("fragments started")

	_, err := s.runner.RunCancelableTask(s.runGCRMTask)
	if err != nil {
		log.Fatalf("run gc task failed with %+v", err)
	}

	_, err = s.runner.RunCancelableTask(s.runManualTask)
	if err != nil {
		log.Fatalf("run manual task failed with %+v", err)
	}

	_, err = s.runner.RunCancelableTask(s.runHBTask)
	if err != nil {
		log.Fatalf("run hb task failed with %+v", err)
	}

	_, err = s.runner.RunCancelableTask(s.runCheckConcurrencyTask)
	if err != nil {
		log.Fatalf("run check concurrency task failed with %+v", err)
	}

	for i := 0; i < s.cfg.PRWorkerCount; i++ {
		idx := uint64(i)
		_, err = s.runner.RunCancelableTask(func(ctx context.Context) {
			s.runPRTask(ctx, idx)
		})
		if err != nil {
			log.Fatalf("run pr event loop task failed with %+v", err)
		}
	}
}

func (s *store) startFragments() error {
	err := s.storage.loadFragments(s.meta.ID, func(value []byte) (uint64, error) {
		frag := meta.Fragment{}
		json.MustUnmarshal(&frag, value)

		pr, err := createPeerReplicate(s, frag)
		if err != nil {
			return 0, err
		}

		s.AddReplicate(pr)
		return frag.ID, nil
	})
	if err != nil {
		log.Fatalf("load fragments failed with %+v", err)
	}

	return nil
}

func (s *store) AddReplicate(pr *PeerReplicate) {
	pr.workerID = uint64(s.cfg.PRWorkerCount-1) & pr.id
	s.replicates.Store(pr.id, pr)
}

func (s *store) doRemovePR(id uint64) {
	s.replicates.Delete(id)
}

func (s *store) GetFragment(id uint64, leader bool) *PeerReplicate {
	if pr, ok := s.replicates.Load(id); ok {
		p := pr.(*PeerReplicate)
		if !leader ||
			(leader && p.isLeader()) {
			return p
		}

		return nil
	}

	return nil
}

func (s *store) AddPeer(id uint64, peer prophet.Peer) {
	pr := s.GetFragment(id, true)
	if nil == pr {
		return
	}

	pr.Lock()
	defer pr.Unlock()

	pr.addPeer(peer)
	s.MustUpdateFragment(peer.ContainerID, pr.frag)

	log.Infof("%s new peer %+v added",
		pr.tag,
		peer)
}

func (s *store) RemovePeer(id uint64, peer prophet.Peer) {
	pr := s.GetFragment(id, true)
	if nil == pr {
		return
	}

	pr.Lock()
	defer pr.Unlock()

	pr.removePeer(peer)
	s.mustRemoveFragmentOnStore(pr.frag, peer)

	s.trans.Send(peer.ContainerID, &meta.RemoveMsg{
		ID: id,
	})

	log.Infof("%s peer %+v removed",
		pr.tag,
		peer)
}

func (s *store) mustCreateFragment(frag meta.Fragment, peer prophet.Peer) {
	err := s.storage.createFragment(frag, peer)
	if err != nil {
		log.Fatalf("save frag %+v failed with %+v",
			frag,
			err)
	}
}

func (s *store) mustRemoveFragmentOnStore(frag meta.Fragment, peer prophet.Peer) {
	err := s.storage.removeFragmentOnStore(frag, peer)
	if err != nil {
		log.Fatalf("remove frag %+v peer %+v failed with %+v",
			frag,
			peer,
			err)
	}
}

func (s *store) MustUpdateFragment(storeID uint64, frag meta.Fragment) {
	err := s.storage.updateFragment(storeID, frag)
	if err != nil {
		log.Fatalf("update frag %+v on store %d failed with %+v",
			frag,
			storeID,
			err)
	}
}

func (s *store) mustRemoveFragment(id uint64) {
	err := s.storage.removeFragment(id)
	if err != nil {
		log.Fatalf("remove frag %d failed with %+v",
			id,
			err)
	}
}

func (s *store) AddRM(rms meta.ResourceManagerSet) {
	s.Lock()

	now := time.Now()
	for _, rm := range rms.ResourceManagers {
		rm.LastHB = now
		values := s.resources[rm.Resource]
		values = append(values, rm)
		s.resources[rm.Resource] = values
		log.Infof("%s added", rm.Tag())
	}

	s.Unlock()
}

func (s *store) RenewRMLease(pid, sid string) {
	for _, rms := range s.resources {
		for idx, rm := range rms {
			if rm.RMSID == sid {
				rms[idx].ProxySID = pid
				rms[idx].LastHB = time.Now()
			}
		}
	}
}

func (s *store) rmAddrDetecter(fid uint64, resource string) (meta.ResourceManager, error) {
	s.RLock()

	rms, ok := s.resources[resource]
	if !ok {
		s.RUnlock()
		return meta.ResourceManager{}, errors.New("no available RM, not registered")
	}

	all := len(rms)

	log.Debugf("resource %s take available resource manager from %d resources",
		resource,
		all)

	if all == 0 {
		s.RUnlock()
		return meta.ResourceManager{}, errors.New("no available RM, rms == 0")
	}

	now := time.Now()
	c := 0
	for {
		rm := rms[int(atomic.AddUint64(&s.ops, 1))%all]
		if now.Sub(rm.LastHB) <= s.cfg.RMLease {
			s.RUnlock()
			return rm, nil
		}
		c++

		log.Debugf("resource %s take available resource manager of %s was not available",
			resource,
			rm.Tag())

		if c >= all {
			break
		}
	}

	s.RUnlock()
	return meta.ResourceManager{}, errors.New("has no available RM")
}

// just for test
type emptyStore struct {
}

func (s *emptyStore) Meta() meta.StoreMeta                                  { return meta.StoreMeta{} }
func (s *emptyStore) Cfg() Cfg                                              { return Cfg{} }
func (s *emptyStore) Start()                                                {}
func (s *emptyStore) FragmentsState() FragmentsState                        { return FragmentsState{} }
func (s *emptyStore) GetStoreAddr(storeID uint64) (string, error)           { return "", nil }
func (s *emptyStore) LeaderPeer(fid uint64) (prophet.Peer, error)           { return prophet.Peer{}, nil }
func (s *emptyStore) CreateFragment() meta.Fragment                         { return meta.Fragment{} }
func (s *emptyStore) MustUpdateFragment(storeID uint64, frag meta.Fragment) {}
func (s *emptyStore) GetFragment(fid uint64, leader bool) *PeerReplicate    { return nil }
func (s *emptyStore) ForeachReplicate(func(*PeerReplicate) bool)            {}
func (s *emptyStore) AddReplicate(*PeerReplicate)                           {}
func (s *emptyStore) AddPeer(fid uint64, peer prophet.Peer)                 {}
func (s *emptyStore) RemovePeer(uint64, prophet.Peer)                       {}
func (s *emptyStore) Transport() transport.Transport                        { return nil }
func (s *emptyStore) ShardingTransport() Transport                          { return nil }
func (s *emptyStore) HandleShardingMsg(data interface{}) interface{}        { return nil }
func (s *emptyStore) AddRM(rms meta.ResourceManagerSet)                     {}
func (s *emptyStore) RenewRMLease(pid, sid string)                          {}
