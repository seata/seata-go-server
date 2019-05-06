package prophet

import (
	"sync"
	"time"
)

// Scheduler is an interface to schedule resources.
type Scheduler interface {
	Name() string
	ResourceKind() ResourceKind
	ResourceLimit() uint64
	Prepare(rt *Runtime) error
	Cleanup(rt *Runtime)
	Schedule(rt *Runtime) Operator
}

// Operator is an interface to scheduler resource
type Operator interface {
	ResourceID() uint64
	ResourceKind() ResourceKind
	Do(target *ResourceRuntime) (*resourceHeartbeatRsp, bool)
}

type scheduleLimiter struct {
	sync.RWMutex
	counts map[ResourceKind]uint64
}

func newScheduleLimiter() *scheduleLimiter {
	return &scheduleLimiter{
		counts: make(map[ResourceKind]uint64),
	}
}

func (l *scheduleLimiter) addOperator(op Operator) {
	l.Lock()
	l.counts[op.ResourceKind()]++
	l.Unlock()
}

func (l *scheduleLimiter) removeOperator(op Operator) {
	l.Lock()
	l.counts[op.ResourceKind()]--
	l.Unlock()
}

func (l *scheduleLimiter) operatorCount(kind ResourceKind) uint64 {
	l.RLock()
	value := l.counts[kind]
	l.RUnlock()
	return value
}

type scheduleController struct {
	sync.Mutex
	Scheduler

	cfg      *Cfg
	limiter  *scheduleLimiter
	interval time.Duration
}

func newScheduleController(c *Coordinator, s Scheduler) *scheduleController {
	return &scheduleController{
		Scheduler: s,
		cfg:       c.cfg,
		limiter:   c.limiter,
		interval:  c.cfg.MinScheduleInterval,
	}
}

func (s *scheduleController) Schedule(rt *Runtime) Operator {
	// If we have schedule, reset interval to the minimal interval.
	if op := s.Scheduler.Schedule(rt); op != nil {
		s.interval = s.cfg.MinScheduleInterval
		return op
	}

	// If we have no schedule, increase the interval exponentially.
	s.interval = minDuration(s.interval*2, s.cfg.MaxScheduleInterval)
	return nil
}

func (s *scheduleController) Interval() time.Duration {
	return s.interval
}

func (s *scheduleController) AllowSchedule() bool {
	return s.limiter.operatorCount(s.ResourceKind()) < s.ResourceLimit()
}
