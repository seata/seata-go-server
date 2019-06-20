package sharding

import (
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/prophet"
	"seata.io/server/pkg/core"
	"seata.io/server/pkg/election"
	"seata.io/server/pkg/local"
	"seata.io/server/pkg/meta"
)

func (s *store) startProphet() {
	log.Infof("start prophet")

	if s.storage == nil {
		ls, err := local.NewBadgerStorage(s.cfg.DataPath)
		if err != nil {
			log.Fatalf("create badger failed with %+v", err)
		}
		s.storage = newStorage(ls)
	}

	adapter := &ProphetAdapter{store: s}
	s.cfg.CoreOptions = append(s.cfg.CoreOptions,
		core.WithElectorOptions(election.WithEtcd(s.pd.GetEtcdClient())))

	s.pdStartedC = make(chan struct{})
	s.cfg.ProphetOptions = append(s.cfg.ProphetOptions, prophet.WithRoleChangeHandler(s))
	s.pd = prophet.NewProphet(s.cfg.ProphetName, s.cfg.ProphetAddr, adapter, s.cfg.ProphetOptions...)
	s.pd.Start()
	<-s.pdStartedC
}

// BecomeLeader this node is become prophet leader
func (s *store) BecomeLeader() {
	log.Infof("*********BecomeLeader prophet*********")
	s.bootOnce.Do(func() {
		s.doBootstrapCluster()
		s.pdStartedC <- struct{}{}
	})
	log.Infof("*********BecomeLeader prophet complete*********")
}

// BecomeFollower this node is become prophet follower
func (s *store) BecomeFollower() {
	log.Infof("*********BecomeFollower prophet*********")
	s.bootOnce.Do(func() {
		s.doBootstrapCluster()
		s.pdStartedC <- struct{}{}
	})
	log.Infof("*********BecomeFollower prophet complete*********")
}

func (s *store) doBootstrapCluster() {
	data, err := s.storage.get(storeKey)
	if err != nil {
		log.Fatalf("load current store meta failed, errors:%+v", err)
	}

	if len(data) > 0 {
		s.meta.ID = goetty.Byte2UInt64(data)
		if s.meta.ID > 0 {
			log.Infof("load from local, store is %d", s.meta.ID)
			return
		}
	}

	s.meta.ID = s.allocID()
	log.Infof("init store with id: %d", s.meta.ID)

	count, err := s.storage.countFragments()
	if err != nil {
		log.Fatalf("bootstrap store failed, errors:\n %+v", err)
	}
	if count > 0 {
		log.Fatal("store is not empty and has already had data")
	}

	err = s.storage.set(storeKey, goetty.Uint64ToBytes(s.meta.ID))
	if err != nil {
		log.Fatal("save current store id failed, errors:%+v", err)
	}

	frag := s.CreateFragment()
	ok, err := s.pd.GetStore().PutBootstrapped(&ContainerAdapter{meta: s.meta},
		&ResourceAdapter{meta: frag})
	if err != nil {
		s.mustRemoveFragment(frag.ID)

		log.Fatal("bootstrap cluster failed, errors:%+v", err)
	}
	if !ok {
		log.Info("the cluster is already bootstrapped")

		s.mustRemoveFragment(frag.ID)
		log.Info("the first Fragment is already removed from store")
	} else {
		s.createInitFragments()
	}

	s.pd.GetRPC().TiggerContainerHeartbeat()
}

func (s *store) CreateFragment() meta.Fragment {
	frag := meta.Fragment{
		ID: s.allocID(),
	}
	frag.Peers = append(frag.Peers, prophet.Peer{
		ID:          s.allocID(),
		ContainerID: s.meta.ID,
	})

	s.MustPutFragment(frag)
	return frag
}

func (s *store) createInitFragments() {
	for i := 0; i < s.cfg.InitFragments-1; i++ {
		frag := meta.Fragment{
			ID: s.allocID(),
		}
		peer := prophet.Peer{
			ID:          s.allocID(),
			ContainerID: s.meta.ID,
		}
		frag.Peers = append(frag.Peers, peer)
		s.MustPutFragment(frag)
	}
}

func (s *store) allocID() uint64 {
	id, err := s.pd.GetRPC().AllocID()
	if err != nil {
		log.Fatalf("alloc id failed, errors:%+v", err)
	}
	return id
}
