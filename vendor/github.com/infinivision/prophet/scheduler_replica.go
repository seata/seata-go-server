package prophet

import (
	"math"
)

type replicaChecker struct {
	cfg     *Cfg
	rt      *Runtime
	filters []Filter
}

func newReplicaChecker(cfg *Cfg, rt *Runtime, filters ...Filter) *replicaChecker {
	return &replicaChecker{
		cfg:     cfg,
		rt:      rt,
		filters: filters,
	}
}

// Check return the Operator
func (r *replicaChecker) Check(target *ResourceRuntime) Operator {
	if op := r.checkDownPeer(target); op != nil {
		return op
	}
	if op := r.checkOfflinePeer(target); op != nil {
		return op
	}

	currReplicasCount := len(target.meta.Peers())
	if currReplicasCount < r.cfg.CountResourceReplicas {
		newPeer, _ := r.selectBestPeer(target, true, r.filters...)
		if newPeer == nil {
			return nil
		}

		return newAddPeerAggregationOp(r.cfg, target, newPeer)
	}

	if currReplicasCount > r.cfg.CountResourceReplicas {
		oldPeer, _ := r.selectWorstPeer(target)
		if oldPeer == nil {
			return nil
		}

		return newRemovePeerOp(target.meta.ID(), oldPeer)
	}

	return r.checkBestReplacement(target)
}

func (r *replicaChecker) checkDownPeer(target *ResourceRuntime) Operator {
	for _, stats := range target.downPeers {
		peer := stats.Peer
		container := r.rt.Container(peer.ContainerID)

		if nil != container && container.Downtime() < r.cfg.MaxAllowContainerDownDuration {
			continue
		}

		if nil != container && stats.DownSeconds < uint64(r.cfg.MaxAllowContainerDownDuration.Seconds()) {
			continue
		}

		return newRemovePeerOp(target.meta.ID(), peer)
	}

	return nil
}

func (r *replicaChecker) checkOfflinePeer(target *ResourceRuntime) Operator {
	for _, peer := range target.meta.Peers() {
		container := r.rt.Container(peer.ContainerID)

		if container != nil && container.IsUp() {
			continue
		}

		newPeer, _ := r.selectBestPeer(target, true)
		if newPeer == nil {
			return nil
		}

		return newTransferPeerAggregationOp(r.cfg, target, peer, newPeer)
	}

	return nil
}

// selectWorstPeer returns the worst peer in the resource.
func (r *replicaChecker) selectWorstPeer(target *ResourceRuntime, filters ...Filter) (*Peer, float64) {
	var (
		worstContainer *ContainerRuntime
		worstScore     float64
	)

	// Select the container with lowest distinct score.
	// If the scores are the same, select the container with maximal resource score.
	containers := r.rt.ResourceContainers(target)
	for _, container := range containers {
		if filterSource(container, filters) {
			continue
		}
		score := r.cfg.getDistinctScore(containers, container)
		if worstContainer == nil || compareContainerScore(container, score, worstContainer, worstScore) < 0 {
			worstContainer = container
			worstScore = score
		}
	}

	if worstContainer == nil || filterSource(worstContainer, r.filters) {
		return nil, 0
	}

	return target.GetContainerPeer(worstContainer.meta.ID()), worstScore
}

// selectBestPeer returns the best peer in other containers.
func (r *replicaChecker) selectBestPeer(target *ResourceRuntime, allocPeerID bool, filters ...Filter) (*Peer, float64) {
	// Add some must have filters.
	filters = append(filters, NewStateFilter(r.cfg))
	filters = append(filters, NewStorageThresholdFilter(r.cfg))
	filters = append(filters, NewExcludedFilter(nil, target.GetContainerIDs()))

	var (
		bestContainer *ContainerRuntime
		bestScore     float64
	)

	// Select the container with best distinct score.
	// If the scores are the same, select the container with minimal replica score.
	containers := r.rt.ResourceContainers(target)
	for _, container := range r.rt.Containers() {
		if filterTarget(container, filters) {
			continue
		}

		score := getDistinctScore(r.cfg, containers, container)
		if bestContainer == nil || compareContainerScore(container, score, bestContainer, bestScore) > 0 {
			bestContainer = container
			bestScore = score
		}
	}

	if bestContainer == nil || filterTarget(bestContainer, r.filters) {
		return nil, 0
	}

	newPeer, err := allocPeer(bestContainer.meta.ID(), r.rt.p.store, allocPeerID)
	if err != nil {
		log.Errorf("scheduler: allocate peer failure, errors:\n %+v", err)
		return nil, 0
	}

	return newPeer, bestScore
}

func (r *replicaChecker) checkBestReplacement(target *ResourceRuntime) Operator {
	oldPeer, oldScore := r.selectWorstPeer(target)
	if oldPeer == nil {
		return nil
	}
	newPeer, newScore := r.selectBestReplacement(target, oldPeer)
	if newPeer == nil {
		return nil
	}

	// Make sure the new peer is better than the old peer.
	if newScore <= oldScore {
		return nil
	}

	id, err := r.rt.p.store.AllocID()
	if err != nil {
		log.Errorf("prophet: allocate peer failure, %+v", err)
		return nil
	}

	newPeer.ID = id
	return newTransferPeerAggregationOp(r.cfg, target, oldPeer, newPeer)
}

// selectBestReplacement returns the best peer to replace the resource peer.
func (r *replicaChecker) selectBestReplacement(target *ResourceRuntime, peer *Peer) (*Peer, float64) {
	// selectBestReplacement returns the best peer to replace the resource peer.
	// Get a new resource without the peer we are going to replace.
	newResource := target.Clone()
	newResource.RemoveContainerPeer(peer.ContainerID)

	return r.selectBestPeer(newResource, false, NewExcludedFilter(nil, target.GetContainerIDs()))
}

// getDistinctScore returns the score that the other is distinct from the containers.
// A higher score means the other container is more different from the existed containers.
func getDistinctScore(cfg *Cfg, containers []*ContainerRuntime, other *ContainerRuntime) float64 {
	score := float64(0)
	locationLabels := cfg.getLocationLabels()

	for i := range locationLabels {
		keys := locationLabels[0 : i+1]
		level := len(locationLabels) - i - 1
		levelScore := math.Pow(replicaBaseScore, float64(level))

		for _, s := range containers {
			if s.meta.ID() == other.meta.ID() {
				continue
			}
			id1 := s.GetLocationID(keys)
			if len(id1) == 0 {
				return 0
			}
			id2 := other.GetLocationID(keys)
			if len(id2) == 0 {
				return 0
			}
			if id1 != id2 {
				score += levelScore
			}
		}
	}

	return score
}

// compareContainerScore compares which container is better for replica.
// Returns 0 if container A is as good as container B.
// Returns 1 if container A is better than container B.
// Returns -1 if container B is better than container A.
func compareContainerScore(containerA *ContainerRuntime, scoreA float64, containerB *ContainerRuntime, scoreB float64) int {
	// The container with higher score is better.
	if scoreA > scoreB {
		return 1
	}
	if scoreA < scoreB {
		return -1
	}
	// The container with lower resource score is better.
	if containerA.ReplicaScore() < containerB.ReplicaScore() {
		return 1
	}
	if containerA.ReplicaScore() > containerB.ReplicaScore() {
		return -1
	}
	return 0
}

func allocPeer(containerID uint64, store Store, allocPeerID bool) (*Peer, error) {
	value := &Peer{ContainerID: containerID}

	if allocPeerID {
		peerID, err := store.AllocID()
		if err != nil {
			return nil, err
		}

		value.ID = peerID
	}

	return value, nil
}
