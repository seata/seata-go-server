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
	"github.com/infinivision/taas/pkg/meta"
	"github.com/infinivision/taas/pkg/transport"
)

// Store is a container of fragment, which maintains a set of fragments,
// in which the state of the leader is the external service.
type Store struct {
	sync.RWMutex

	cfg        Cfg
	meta       meta.StoreMeta
	storage    *storage
	replicates *sync.Map
	pd         *prophet.Prophet
	bootOnce   *sync.Once
	pdStartedC chan struct{}
	runner     *task.Runner
	trans      *shardingTransport
	seataTrans transport.Transport

	resources map[string][]meta.ResourceManager
	ops       uint64
}

// NewStore returns store with cfg
func NewStore(cfg Cfg) *Store {
	s := new(Store)
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
	s.trans = newShardingTransport(s)
	s.seataTrans = transport.NewTransport(cfg.TransWorkerCount, s.rmAddrDetecter, cfg.TransSendCB)
	return s
}

// Start start sharing store
func (s *Store) Start() {
	s.startProphet()
	log.Infof("begin to start store %d", s.meta.ID)

	s.trans.start()
	log.Infof("peer transport start at %s", s.cfg.ShardingAddr)

	s.seataTrans.Start()
	log.Infof("seata transport start")

	s.startFragments()
	log.Infof("fragments started")

	_, err := s.runner.RunCancelableTask(s.runGCRMTask)
	if err != nil {
		log.Fatalf("run gc task failed with %+v", err)
	}
}

func (s *Store) startFragments() error {
	err := s.storage.loadFragments(s.meta.ID, func(value []byte) (uint64, error) {
		frag := meta.Fragment{}
		json.MustUnmarshal(&frag, value)

		pr, err := createPeerReplicate(s, frag)
		if err != nil {
			return 0, err
		}

		s.doAddPR(pr)
		return frag.ID, nil
	})
	if err != nil {
		log.Fatalf("load fragments failed with %+v", err)
	}

	return nil
}

func (s *Store) doAddPR(pr *PeerReplicate) {
	s.replicates.Store(pr.id, pr)
}

func (s *Store) doRemovePR(id uint64) {
	s.replicates.Delete(id)
}

func (s *Store) foreachFragments(doFunc func(pr *PeerReplicate)) {
	s.replicates.Range(func(key, value interface{}) bool {
		doFunc(value.(*PeerReplicate))
		return false
	})
}

func (s *Store) getFragment(id uint64, leader bool) *PeerReplicate {
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

func (s *Store) addPeer(id uint64, peer prophet.Peer) {
	pr := s.getFragment(id, true)
	if nil == pr {
		return
	}

	pr.Lock()
	defer pr.Unlock()

	pr.addPeer(peer)
	s.mustUpdateFragmentOnStore(pr.frag, peer)

	log.Infof("%s new peer %+v added",
		pr.tag,
		peer)
}

func (s *Store) removePeer(id uint64, peer prophet.Peer) {
	pr := s.getFragment(id, true)
	if nil == pr {
		return
	}

	pr.Lock()
	defer pr.Unlock()

	pr.removePeer(peer)
	s.mustRemoveFragmentOnStore(pr.frag, peer)

	s.trans.sendMsg(peer.ContainerID, &meta.RemoveMsg{
		ID: id,
	})

	log.Infof("%s peer %+v removed",
		pr.tag,
		peer)
}

func (s *Store) mustCreateFragment(frag meta.Fragment, peer prophet.Peer) {
	err := s.storage.createFragment(frag, peer)
	if err != nil {
		log.Fatalf("save frag %+v failed with %+v",
			frag,
			err)
	}
}

func (s *Store) mustRemoveFragmentOnStore(frag meta.Fragment, peer prophet.Peer) {
	err := s.storage.removeFragmentOnStore(frag, peer)
	if err != nil {
		log.Fatalf("remove frag %+v peer %+v failed with %+v",
			frag,
			peer,
			err)
	}
}

func (s *Store) mustUpdateFragmentOnStore(frag meta.Fragment, peer prophet.Peer) {
	err := s.storage.updateFragmentOnStore(frag, peer)
	if err != nil {
		log.Fatalf("update frag %+v peer %+v failed with %+v",
			frag,
			peer,
			err)
	}
}

func (s *Store) mustRemoveFragment(id uint64) {
	err := s.storage.removeFragment(id)
	if err != nil {
		log.Fatalf("remove frag %d failed with %+v",
			id,
			err)
	}
}

func (s *Store) handleRegisterTM(ss *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.RegisterTMRequest)

	rsp := meta.AcquireRegisterTMResponse()
	rsp.Identified = true
	rsp.Version = req.Version
	ss.cb(msg, rsp, nil)
}

func (s *Store) handleRegisterRM(ss *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.RegisterRMRequest)

	rsp := meta.AcquireRegisterRMResponse()
	rsp.Identified = false
	rsp.ResultCode = meta.Failed
	rsp.Version = req.Version

	if req.ResourceIDs == "" {
		log.Fatalf("%s register RM failed, missing resource, using unknown resource to instead", ss.id)
	}

	rms := req.ToResourceSet(ss.id, msg.RMSID)
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

	rsp.Identified = true
	rsp.ResultCode = meta.Succeed
	ss.cb(msg, rsp, nil)
}

func (s *Store) handleRenewRMLease(pid, sid string) {
	for _, rms := range s.resources {
		for idx, rm := range rms {
			if rm.RMSID == sid {
				rms[idx].ProxySID = pid
				rms[idx].LastHB = time.Now()
			}
		}
	}
}

func (s *Store) runGCRMTask(ctx context.Context) {
	gcRMticker := time.NewTicker(s.cfg.RMLease)
	defer gcRMticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("[GC-RM]: task exit")
			return
		case <-gcRMticker.C:
			s.doGCRM()
		}
	}
}

func (s *Store) doGCRM() {
	log.Debugf("[GC-RM]: start")

	now := time.Now()
	newResources := make(map[string][]meta.ResourceManager, len(s.resources))

	s.Lock()
	defer s.Unlock()

	var values []string
	for key, rms := range s.resources {
		var newRMS []meta.ResourceManager
		for _, rm := range rms {
			if now.Sub(rm.LastHB) > s.cfg.RMLease {
				values = append(values, rm.RMSID)
			} else {
				newRMS = append(newRMS, rm)
			}
		}
		s.resources[key] = newRMS
	}

	if len(values) == 0 {
		return
	}

	for _, rm := range values {
		log.Infof("[GC-RM]: %s removed by timeout",
			rm)
	}

	s.resources = newResources
	log.Infof("[GC-RM]: complete with %d timeout rm",
		len(values))
	return
}

func (s *Store) rmAddrDetecter(fid uint64, resource string) (meta.ResourceManager, error) {
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
