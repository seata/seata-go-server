package prophet

import (
	"time"
)

var (
	defaultTime time.Time
)

// ContainerRuntime is the container runtime info
type ContainerRuntime struct {
	meta Container

	blocked, busy                                           bool
	lastHeartbeatTS                                         time.Time
	storageCapacity, storageAvailable                       uint64
	leaderCount, replicaCount                               uint64
	sendingSnapCount, receivingSnapCount, applyingSnapCount uint64
}

func newContainerRuntime(meta Container) *ContainerRuntime {
	return &ContainerRuntime{
		meta: meta,
	}
}

// GetLocationID returns location id
func (cr *ContainerRuntime) GetLocationID(keys []string) string {
	id := ""
	for _, k := range keys {
		v := cr.GetLabelValue(k)
		if len(v) == 0 {
			return ""
		}
		id += v
	}
	return id
}

// GetLabelValue returns label value of key
func (cr *ContainerRuntime) GetLabelValue(key string) string {
	for _, label := range cr.meta.Lables() {
		if label.Key == key {
			return label.Value
		}
	}

	return ""
}

// IsUp returns the container is up state
func (cr *ContainerRuntime) IsUp() bool {
	return cr.meta.State() == UP
}

// Clone returns the container clone info
func (cr *ContainerRuntime) Clone() *ContainerRuntime {
	value := &ContainerRuntime{}
	value.meta = cr.meta.Clone()
	value.blocked = cr.blocked
	value.busy = cr.busy
	value.lastHeartbeatTS = cr.lastHeartbeatTS

	value.storageCapacity = cr.storageCapacity
	value.storageAvailable = cr.storageAvailable
	value.leaderCount = cr.leaderCount
	value.replicaCount = cr.replicaCount
	value.sendingSnapCount = cr.sendingSnapCount
	value.receivingSnapCount = cr.receivingSnapCount
	value.applyingSnapCount = cr.applyingSnapCount
	return value
}

// Downtime returns the container down time
func (cr *ContainerRuntime) Downtime() time.Duration {
	return time.Since(cr.lastHeartbeatTS)
}

// StorageUsedBytes returns container used storage with bytes
func (cr *ContainerRuntime) StorageUsedBytes() uint64 {
	return cr.storageCapacity - cr.storageAvailable
}

// StorageUsedRatio returns container used storage with rate
func (cr *ContainerRuntime) StorageUsedRatio() int {
	cap := cr.storageCapacity

	if cap == 0 {
		return 0
	}

	return int(float64(cr.StorageUsedBytes()) * 100 / float64(cap))
}

// IsBlocked returns the container is blocked from balance if true.
func (cr *ContainerRuntime) IsBlocked() bool {
	return cr.HasNoneHeartbeat() || cr.blocked
}

// HasNoneHeartbeat returns if received a heartbeat from the container
func (cr *ContainerRuntime) HasNoneHeartbeat() bool {
	return cr.lastHeartbeatTS.Equal(defaultTime)
}

// LeaderScore returns score with leader
func (cr *ContainerRuntime) LeaderScore() float64 {
	return float64(cr.leaderCount)
}

// ReplicaScore returns score with replica
func (cr *ContainerRuntime) ReplicaScore() float64 {
	if cr.storageCapacity == 0 {
		return 0
	}

	return float64(cr.replicaCount) / float64(cr.storageCapacity)
}

// ResourceCount returns resource count by kind
func (cr *ContainerRuntime) ResourceCount(kind ResourceKind) uint64 {
	switch kind {
	case LeaderKind:
		return cr.leaderCount
	case ReplicaKind:
		return cr.replicaCount
	default:
		return 0
	}
}

// ResourceScore returns resource score by kind
func (cr *ContainerRuntime) ResourceScore(kind ResourceKind) float64 {
	switch kind {
	case LeaderKind:
		return cr.LeaderScore()
	case ReplicaKind:
		return cr.ReplicaScore()
	default:
		return 0
	}
}

// compareLocation compares 2 container' labels and returns at which level their
// locations are different. It returns -1 if they are at the same location.
func (cr *ContainerRuntime) compareLocation(other *ContainerRuntime, labels []string) int {
	for i, key := range labels {
		v1, v2 := cr.GetLabelValue(key), other.GetLabelValue(key)
		// If label is not set, the container is considered at the same location
		// with any other container.
		if v1 != "" && v2 != "" && v1 != v2 {
			return i
		}
	}
	return -1
}
