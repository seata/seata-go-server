package prophet

import (
	"math"
	"time"
)

var (
	replicaBaseScore = float64(100)
)

type emprtyHandler struct {
}

func (h *emprtyHandler) BecomeLeader()   {}
func (h *emprtyHandler) BecomeFollower() {}

// Cfg prophet cfg
type Cfg struct {
	// MaxScheduleRetries maximum retry times for schedule
	MaxScheduleRetries int
	// MaxScheduleInterval maximum schedule interval per scheduler
	MaxScheduleInterval time.Duration
	// MinScheduleInterval minimum schedule interval per scheduler
	MinScheduleInterval time.Duration
	// TimeoutWaitOperatorComplete timeout for waitting teh operator complete
	TimeoutWaitOperatorComplete time.Duration
	// MaxFreezeScheduleInterval freeze the container for a while if shouldSchedule is returns false
	MaxFreezeScheduleInterval time.Duration
	// MaxAllowContainerDownDuration maximum down time of removed from replicas
	MaxAllowContainerDownDuration time.Duration
	// MaxRebalanceLeader maximum count of transfer leader operator
	MaxRebalanceLeader uint64
	// MaxRebalanceReplica maximum count of remove|add replica operator
	MaxRebalanceReplica uint64
	// MaxScheduleReplica maximum count of schedule replica operator
	MaxScheduleReplica uint64
	// MaxLimitSnapshotsCount maximum count of node about snapshot
	MaxLimitSnapshotsCount uint64
	// CountResourceReplicas replica number per resource
	CountResourceReplicas int
	// MinAvailableStorageUsedRate minimum storage used rate of container, if the rate is over this value, skip the container
	MinAvailableStorageUsedRate int
	// LocationLabels the label used for location
	LocationLabels []string
	// MaxRPCCons rpc conns
	MaxRPCCons int
	// MaxRPCConnIdle rpc conn max idle time
	MaxRPCConnIdle time.Duration
	// MaxRPCTimeout rpc max timeout
	MaxRPCTimeout time.Duration

	Namespace   string
	StorageNode bool
	LeaseTTL    int64
	Schedulers  []Scheduler
	Handler     RoleChangeHandler
}

func (c *Cfg) adujst() {
	if c.LeaseTTL == 0 {
		c.LeaseTTL = 5
	}

	if c.Namespace == "" {
		c.Namespace = "/prophet"
	}

	if c.MaxScheduleRetries == 0 {
		c.MaxScheduleRetries = 3
	}

	if c.MaxScheduleInterval == 0 {
		c.MaxScheduleInterval = time.Minute
	}

	if c.MinScheduleInterval == 0 {
		c.MinScheduleInterval = time.Millisecond * 10
	}

	if c.TimeoutWaitOperatorComplete == 0 {
		c.TimeoutWaitOperatorComplete = 5 * time.Minute
	}

	if c.MaxFreezeScheduleInterval == 0 {
		c.MaxFreezeScheduleInterval = 30 * time.Second
	}

	if c.MaxAllowContainerDownDuration == 0 {
		c.MaxAllowContainerDownDuration = time.Hour
	}

	if c.MaxRebalanceReplica == 0 {
		c.MaxRebalanceReplica = 12
	}

	if c.CountResourceReplicas == 0 {
		c.CountResourceReplicas = 3
	}

	if c.MaxScheduleReplica == 0 {
		c.MaxScheduleReplica = 16
	}

	if c.MinAvailableStorageUsedRate == 0 {
		c.MinAvailableStorageUsedRate = 80
	}

	if c.MaxRebalanceLeader == 0 {
		c.MaxRebalanceLeader = 16
	}

	if c.MaxLimitSnapshotsCount == 0 {
		c.MaxLimitSnapshotsCount = 3
	}

	if c.MaxRPCCons == 0 {
		c.MaxRPCCons = 10
	}

	if c.MaxRPCConnIdle == 0 {
		c.MaxRPCConnIdle = time.Hour
	}

	if c.MaxRPCTimeout == 0 {
		c.MaxRPCTimeout = time.Second * 10
	}

	if len(c.Schedulers) == 0 {
		c.Schedulers = append(c.Schedulers, newBalanceReplicaScheduler(c))
		c.Schedulers = append(c.Schedulers, newBalanceResourceLeaderScheduler(c))
	}

	if c.Handler == nil {
		c.Handler = &emprtyHandler{}
	}
}

func (c *Cfg) getLocationLabels() []string {
	var value []string
	for _, v := range c.LocationLabels {
		value = append(value, v)
	}

	return value
}

// getDistinctScore returns the score that the other is distinct from the containers.
// A higher score means the other container is more different from the existed containers.
func (c *Cfg) getDistinctScore(containers []*ContainerRuntime, other *ContainerRuntime) float64 {
	score := float64(0)
	locationLabels := c.LocationLabels

	for _, s := range containers {
		if s.meta.ID() == other.meta.ID() {
			continue
		}
		if index := s.compareLocation(other, locationLabels); index != -1 {
			score += math.Pow(replicaBaseScore, float64(len(locationLabels)-index-1))
		}
	}
	return score
}
