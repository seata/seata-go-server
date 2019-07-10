package prophet

import (
	"fmt"
)

// ElectorOption elector option
type ElectorOption func(*electorOptions)

type electorOptions struct {
	leaderPath, lockPath string
	leaseSec             int64
	lockIfBecomeLeader   bool
}

func (opts *electorOptions) adjust() {
	if opts.leaderPath == "" || opts.lockPath == "" {
		opts.leaderPath = "/electors/leader"
		opts.lockPath = "/electors/lock"
	}

	if opts.leaseSec <= 0 {
		opts.leaseSec = 5
	}
}

// WithPrefix set data prefix in embed etcd server
func WithPrefix(value string) ElectorOption {
	return func(opts *electorOptions) {
		opts.leaderPath = fmt.Sprintf("%s/leader", value)
		opts.lockPath = fmt.Sprintf("%s/lock", value)
	}
}

// WithLeaderLeaseSeconds set leader lease in seconds
func WithLeaderLeaseSeconds(value int64) ElectorOption {
	return func(opts *electorOptions) {
		opts.leaseSec = value
	}
}

// WithLockIfBecomeLeader set lock enable flag if become leader,
// If true, will add a distributed lock, and will unlock on become follower,
// ensure that the other nodes can be changed to leaders after the previous
// leader has processed the role changes.
func WithLockIfBecomeLeader(value bool) ElectorOption {
	return func(opts *electorOptions) {
		opts.lockIfBecomeLeader = value
	}
}
