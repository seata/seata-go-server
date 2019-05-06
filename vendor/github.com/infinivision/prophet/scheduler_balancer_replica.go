package prophet

type balanceReplicaScheduler struct {
	limit       uint64
	freezeCache *resourceFreezeCache
	selector    Selector
	cfg         *Cfg
}

func newBalanceReplicaScheduler(cfg *Cfg) Scheduler {
	freezeCache := newResourceFreezeCache(cfg.MaxFreezeScheduleInterval, 4*cfg.MaxFreezeScheduleInterval)

	var filters []Filter
	filters = append(filters, NewCacheFilter(freezeCache))
	filters = append(filters, NewStateFilter(cfg))
	filters = append(filters, NewHealthFilter(cfg))
	filters = append(filters, NewStorageThresholdFilter(cfg))
	filters = append(filters, NewSnapshotCountFilter(cfg))

	freezeCache.startGC()
	return &balanceReplicaScheduler{
		cfg:         cfg,
		freezeCache: freezeCache,
		limit:       1,
		selector:    newBalanceSelector(ReplicaKind, filters),
	}
}

func (s *balanceReplicaScheduler) Name() string {
	return "balance-replica-scheduler"
}

func (s *balanceReplicaScheduler) ResourceKind() ResourceKind {
	return ReplicaKind
}

func (s *balanceReplicaScheduler) ResourceLimit() uint64 {
	return minUint64(s.limit, s.cfg.MaxRebalanceReplica)
}

func (s *balanceReplicaScheduler) Prepare(rt *Runtime) error { return nil }

func (s *balanceReplicaScheduler) Cleanup(rt *Runtime) {}

func (s *balanceReplicaScheduler) Schedule(rt *Runtime) Operator {
	// Select a peer from the container with most resources.
	res, oldPeer := scheduleRemovePeer(rt, s.selector)
	if res == nil {
		return nil
	}

	// We don't schedule resource with abnormal number of replicas.
	if len(res.meta.Peers()) != int(s.cfg.CountResourceReplicas) {
		return nil
	}

	op := s.transferPeer(rt, res, oldPeer)
	if op == nil {
		// We can't transfer peer from this container now, so we add it to the cache
		// and skip it for a while.
		s.freezeCache.set(oldPeer.ContainerID, nil)
	}

	return op
}

func (s *balanceReplicaScheduler) transferPeer(rt *Runtime, res *ResourceRuntime, oldPeer *Peer) Operator {
	// scoreGuard guarantees that the distinct score will not decrease.
	containers := rt.ResourceContainers(res)
	source := rt.Container(oldPeer.ContainerID)
	scoreGuard := NewDistinctScoreFilter(s.cfg, containers, source)

	checker := newReplicaChecker(s.cfg, rt)
	newPeer, _ := checker.selectBestPeer(res, false, scoreGuard)
	if newPeer == nil {
		return nil
	}

	target := rt.Container(newPeer.ContainerID)
	if !shouldBalance(source, target, s.ResourceKind()) {
		return nil
	}

	id, err := checker.rt.p.store.AllocID()
	if err != nil {
		log.Errorf("prophet: allocate peer failure, %+v", err)
		return nil
	}
	newPeer.ID = id

	s.limit = adjustBalanceLimit(rt, s.ResourceKind())
	return newTransferPeerAggregationOp(s.cfg, res, oldPeer, newPeer)
}

// scheduleRemovePeer schedules a resource to remove the peer.
func scheduleRemovePeer(rt *Runtime, s Selector, filters ...Filter) (*ResourceRuntime, *Peer) {
	containers := rt.Containers()

	source := s.SelectSource(containers, filters...)
	if source == nil {
		return nil, nil
	}

	target := rt.RandFollowerResource(source.meta.ID())
	if target == nil {
		target = rt.RandLeaderResource(source.meta.ID())
	}
	if target == nil {
		return nil, nil
	}

	return target, target.GetContainerPeer(source.meta.ID())
}
