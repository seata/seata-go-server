package prophet

import (
	"time"

	"github.com/coreos/etcd/clientv3"
)

type options struct {
	client *clientv3.Client
	cfg    *Cfg
}

func (opts *options) adjust() {
	if opts.client == nil {
		log.Fatalf("etcd v3 client is not setting, using WithExternalEtcd or WithEmbeddedEtcd to initialize.")
	}

	opts.cfg.adujst()
}

// Option is prophet create option
type Option func(*options)

// WithExternalEtcd using  external etcd cluster
func WithExternalEtcd(client *clientv3.Client) Option {
	return func(opts *options) {
		opts.client = client
		opts.cfg.StorageNode = false
	}
}

// WithEmbeddedEtcd using embedded etcd cluster
func WithEmbeddedEtcd(clientAddrs []string, cfg *EmbeddedEtcdCfg) Option {
	return func(opts *options) {
		opts.client = initWithEmbedEtcd(clientAddrs, cfg)
		opts.cfg.StorageNode = true
	}
}

// WithLeaseTTL prophet leader lease ttl
func WithLeaseTTL(leaseTTL int64) Option {
	return func(opts *options) {
		opts.cfg.LeaseTTL = leaseTTL
	}
}

// WithMaxScheduleRetries using MaxScheduleRetries maximum retry times for schedule
func WithMaxScheduleRetries(value int) Option {
	return func(opts *options) {
		opts.cfg.MaxScheduleRetries = value
	}
}

// WithMaxScheduleInterval using MaxScheduleInterval maximum schedule interval per scheduler
func WithMaxScheduleInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxScheduleInterval = value
	}
}

// WithMinScheduleInterval using MinScheduleInterval minimum schedule interval per scheduler
func WithMinScheduleInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MinScheduleInterval = value
	}
}

// WithTimeoutWaitOperatorComplete timeout for waitting teh operator complete
func WithTimeoutWaitOperatorComplete(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.TimeoutWaitOperatorComplete = value
	}
}

// WithMaxFreezeScheduleInterval freeze the container for a while if shouldSchedule is returns false
func WithMaxFreezeScheduleInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxFreezeScheduleInterval = value
	}
}

// WithMaxAllowContainerDownDuration maximum down time of removed from replicas
func WithMaxAllowContainerDownDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxAllowContainerDownDuration = value
	}
}

// WithMaxRebalanceLeader maximum count of transfer leader operator
func WithMaxRebalanceLeader(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxRebalanceLeader = value
	}
}

// WithMaxRebalanceReplica maximum count of remove|add replica operator
func WithMaxRebalanceReplica(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxRebalanceReplica = value
	}
}

// WithMaxScheduleReplica maximum count of schedule replica operator
func WithMaxScheduleReplica(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxScheduleReplica = value
	}
}

// WithCountResourceReplicas replica number per resource
func WithCountResourceReplicas(value int) Option {
	return func(opts *options) {
		opts.cfg.CountResourceReplicas = value
	}
}

// WithMinAvailableStorageUsedRate minimum storage used rate of container, if the rate is over this value, skip the container
func WithMinAvailableStorageUsedRate(value int) Option {
	return func(opts *options) {
		opts.cfg.MinAvailableStorageUsedRate = value
	}
}

// WithNamespace set namespace
func WithNamespace(value string) Option {
	return func(opts *options) {
		opts.cfg.Namespace = value
	}
}

// WithMaxLimitSnapshotsCount maximum count of node about snapshot
func WithMaxLimitSnapshotsCount(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxLimitSnapshotsCount = value
	}
}

// WithLocationLabels the label used for location
func WithLocationLabels(value []string) Option {
	return func(opts *options) {
		opts.cfg.LocationLabels = value
	}
}

// WithScheduler add a scheduler
func WithScheduler(value Scheduler) Option {
	return func(opts *options) {
		opts.cfg.Schedulers = append(opts.cfg.Schedulers, value)
	}
}

// WithRoleChangeHandler using a role changed handler
func WithRoleChangeHandler(Handler RoleChangeHandler) Option {
	return func(opts *options) {
		opts.cfg.Handler = Handler
	}
}

// WithMaxRPCCons set MaxRPCCons
func WithMaxRPCCons(value int) Option {
	return func(opts *options) {
		opts.cfg.MaxRPCCons = value
	}
}

// WithMaxRPCConnIdle set MaxRPCConnIdle
func WithMaxRPCConnIdle(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxRPCConnIdle = value
	}
}

// WithMaxRPCTimeout set MaxRPCTimeout
func WithMaxRPCTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxRPCTimeout = value
	}
}
