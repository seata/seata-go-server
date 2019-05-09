package election

import (
	"fmt"

	"github.com/coreos/etcd/clientv3"
)

// Option option for election
type Option func(*options)

type options struct {
	lockPath   string
	leaderPath string
	leaseSec   int64
	client     *clientv3.Client
}

func (opts *options) adjust() error {
	if opts.client == nil {
		return fmt.Errorf("missing etcd client")
	}

	if opts.leaderPath == "" {
		opts.leaderPath = "/tmp/taas/election"
	}

	if opts.lockPath == "" {
		opts.lockPath = "/tmp/taas/lock/election"
	}

	if opts.leaseSec == 0 {
		opts.leaseSec = 5
	}

	return nil
}

// WithLeaderPath set leader path
func WithLeaderPath(value string) Option {
	return func(opts *options) {
		opts.leaderPath = value
	}
}

// WithLockPath set lock path
func WithLockPath(value string) Option {
	return func(opts *options) {
		opts.lockPath = value
	}
}

// WithLeaderLeaseSec set leader lease time in seconds
func WithLeaderLeaseSec(value int64) Option {
	return func(opts *options) {
		opts.leaseSec = value
	}
}

// WithEtcd set etcd client
func WithEtcd(value *clientv3.Client) Option {
	return func(opts *options) {
		opts.client = value
	}
}
