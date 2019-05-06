package prophet

import (
	"math"
)

type balanceResourceLeaderScheduler struct {
	cfg      *Cfg
	limit    uint64
	selector Selector
}

func newBalanceResourceLeaderScheduler(cfg *Cfg) Scheduler {
	var filters []Filter
	filters = append(filters, NewBlockFilter())
	filters = append(filters, NewStateFilter(cfg))
	filters = append(filters, NewHealthFilter(cfg))

	return &balanceResourceLeaderScheduler{
		cfg:      cfg,
		limit:    1,
		selector: newBalanceSelector(LeaderKind, filters),
	}
}

func (l *balanceResourceLeaderScheduler) Name() string {
	return "scheduler-rebalance-leader"
}

func (l *balanceResourceLeaderScheduler) ResourceKind() ResourceKind {
	return LeaderKind
}

func (l *balanceResourceLeaderScheduler) ResourceLimit() uint64 {
	return minUint64(l.limit, l.cfg.MaxRebalanceLeader)
}

func (l *balanceResourceLeaderScheduler) Prepare(rt *Runtime) error { return nil }

func (l *balanceResourceLeaderScheduler) Cleanup(rt *Runtime) {}

func (l *balanceResourceLeaderScheduler) Schedule(rt *Runtime) Operator {
	res, newLeader := scheduleTransferLeader(rt, l.selector)
	if res == nil {
		return nil
	}

	source := rt.Container(res.leaderPeer.ContainerID)
	target := rt.Container(newLeader.ContainerID)
	if !shouldBalance(source, target, l.ResourceKind()) {
		return nil
	}
	l.limit = adjustBalanceLimit(rt, l.ResourceKind())

	return newTransferLeaderAggregationOp(l.cfg, res, newLeader)
}

// scheduleTransferLeader schedules a resource to transfer leader to the peer.
func scheduleTransferLeader(rt *Runtime, s Selector, filters ...Filter) (*ResourceRuntime, *Peer) {
	containers := rt.Containers()
	if len(containers) == 0 {
		return nil, nil
	}

	var averageLeader float64
	for _, container := range containers {
		averageLeader += container.LeaderScore() / float64(len(containers))
	}

	mostLeaderContainer := s.SelectSource(containers, filters...)
	leastLeaderContainer := s.SelectTarget(containers, filters...)

	var mostLeaderDistance, leastLeaderDistance float64
	if mostLeaderContainer != nil {
		mostLeaderDistance = math.Abs(mostLeaderContainer.LeaderScore() - averageLeader)
	}
	if leastLeaderContainer != nil {
		leastLeaderDistance = math.Abs(leastLeaderContainer.LeaderScore() - averageLeader)
	}

	if mostLeaderDistance == 0 && leastLeaderDistance == 0 {
		return nil, nil
	}

	if mostLeaderDistance > leastLeaderDistance {
		// Transfer a leader out of mostLeaderContainer.
		res := rt.RandLeaderResource(mostLeaderContainer.meta.ID())
		if res == nil {
			return nil, nil
		}

		targetContainers := rt.ResourceFollowerContainers(res)
		target := s.SelectTarget(targetContainers)
		if target == nil {
			return nil, nil
		}

		return res, res.GetContainerPeer(target.meta.ID())
	}

	// Transfer a leader into leastLeaderContainer.
	res := rt.RandFollowerResource(leastLeaderContainer.meta.ID())
	if res == nil {
		return nil, nil
	}
	return res, res.GetContainerPeer(leastLeaderContainer.meta.ID())
}
