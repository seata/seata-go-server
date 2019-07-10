package prophet

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
)

var (
	addr                             = flag.String("prophet-addr", "127.0.0.1:9529", "Prophet: rpc address")
	data                             = flag.String("prophet-data", "/tmp/prophet", "Prophet: local data path")
	redirectEmbedEtcdLog             = flag.String("prophet-redirect-etcd-log", "", "Prophet: redirect etcd log")
	join                             = flag.String("prophet-addr-join", "", "Prophet: the target prohet cluster addr")
	clientURLs                       = flag.String("prophet-urls-client", "http://0.0.0.0:2379", "Prophet: embed etcd client urls")
	peerURLs                         = flag.String("prophet-urls-peer", "http://0.0.0.0:2381", "Prophet: embed etcd peer urls")
	advertiseClientURLs              = flag.String("prophet-urls-advertise-client", "", "Prophet: embed etcd client advertise urls")
	advertisePeerURLs                = flag.String("prophet-urls-advertise-peer", "", "Prophet: embed etcd peer advertise urls")
	storageNode                      = flag.Bool("prophet-storage", true, "Prophet: is storage node, if true enable embed etcd server")
	leaderLeaseTTLSec                = flag.Int64("prophet-leader-lease", 5, "Prophet: seconds of leader lease ttl")
	scheduleRetries                  = flag.Int("prophet-schedule-max-retry", 3, "Prophet: max schedule retries times when schedule failed")
	scheduleMaxIntervalSec           = flag.Int("prophet-schedule-max-interval", 60, "Prophet: maximum seconds between twice schedules")
	scheduleMinIntervalMS            = flag.Int("prophet-schedule-min-interval", 10, "Prophet: minimum millisecond between twice schedules")
	timeoutWaitOperatorCompleteMin   = flag.Int("prophet-timeout-wait-operator", 5, "Prophet: timeout for waitting teh operator complete")
	maxFreezeScheduleIntervalSec     = flag.Int("prophet-schedule-max-freeze-interval", 30, "Prophet: maximum seconds freeze the container for a while if no need to schedule")
	maxAllowContainerDownDurationMin = flag.Int("prophet-max-allow-container-down", 60, "Prophet: maximum container down mins, the container will removed from replicas")
	maxRebalanceLeader               = flag.Uint64("prophet-max-rebalance-leader", 16, "Prophet: maximum count of transfer leader operator")
	maxRebalanceReplica              = flag.Uint64("prophet-max-rebalance-replica", 12, "Prophet: maximum count of remove|add replica operator")
	maxScheduleReplica               = flag.Uint64("prophet-schedule-max-replica", 12, "Prophet: maximum count of schedule replica operator")
	maxLimitSnapshotsCount           = flag.Uint64("prophet-max-snapshot", 3, "Prophet: maximum count of node about snapshot with schedule")
	countResourceReplicas            = flag.Int("prophet-resource-replica", 3, "Prophet: replica number per resource")
	minAvailableStorageUsedRate      = flag.Int("prophet-min-storage", 80, "Prophet: minimum storage used rate of container, if the rate is over this value, skip the container")
	maxRPCConns                      = flag.Int("prophet-rpc-conns", 10, "Prophet: maximum connections for rpc")
	rpcConnIdleSec                   = flag.Int("prophet-rpc-idle", 60*60, "Prophet(Sec): maximum idle time for rpc connection")
	rpcTimeoutSec                    = flag.Int("prophet-rpc-timeout", 10, "Prophet(Sec): maximum timeout to wait rpc response")
)

// ParseProphetOptions parse the prophet options from command line parameter
func ParseProphetOptions(name string) []Option {
	if !flag.Parsed() {
		flag.Parse()
	}

	var opts []Option
	opts = append(opts, WithRPCAddr(*addr))
	opts = append(opts, WithLeaseTTL(*leaderLeaseTTLSec))
	opts = append(opts, WithMaxScheduleRetries(*scheduleRetries))
	opts = append(opts, WithMaxScheduleInterval(time.Second*time.Duration(*scheduleMaxIntervalSec)))
	opts = append(opts, WithMinScheduleInterval(time.Millisecond*time.Duration(*scheduleMinIntervalMS)))
	opts = append(opts, WithTimeoutWaitOperatorComplete(time.Minute*time.Duration(*timeoutWaitOperatorCompleteMin)))
	opts = append(opts, WithMaxFreezeScheduleInterval(time.Second*time.Duration(*maxFreezeScheduleIntervalSec)))
	opts = append(opts, WithMaxAllowContainerDownDuration(time.Minute*time.Duration(*maxAllowContainerDownDurationMin)))
	opts = append(opts, WithMaxRebalanceLeader(*maxRebalanceLeader))
	opts = append(opts, WithMaxRebalanceReplica(*maxRebalanceReplica))
	opts = append(opts, WithMaxScheduleReplica(*maxScheduleReplica))
	opts = append(opts, WithMaxLimitSnapshotsCount(*maxLimitSnapshotsCount))
	opts = append(opts, WithCountResourceReplicas(*countResourceReplicas))
	opts = append(opts, WithMinAvailableStorageUsedRate(*minAvailableStorageUsedRate))
	opts = append(opts, WithMaxRPCCons(*maxRPCConns))
	opts = append(opts, WithMaxRPCConnIdle(time.Second*time.Duration(*rpcConnIdleSec)))
	opts = append(opts, WithMaxRPCTimeout(time.Second*time.Duration(*rpcTimeoutSec)))

	if *storageNode {
		embedEtcdCfg := &EmbeddedEtcdCfg{}
		embedEtcdCfg.EmbedEtcdLog = *redirectEmbedEtcdLog
		embedEtcdCfg.Join = *join
		embedEtcdCfg.DataPath = fmt.Sprintf("%s/prophet", *data)
		embedEtcdCfg.Name = name
		embedEtcdCfg.URLsAdvertiseClient = *advertiseClientURLs
		embedEtcdCfg.URLsAdvertisePeer = *advertisePeerURLs
		embedEtcdCfg.URLsClient = *clientURLs
		embedEtcdCfg.URLsPeer = *peerURLs
		opts = append(opts, WithEmbeddedEtcd(embedEtcdCfg))
	} else {
		endpoints := strings.Split(*join, ",")
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: DefaultTimeout,
		})
		if err != nil {
			fmt.Printf("init etcd client failed: %+v\n", err)
			os.Exit(-1)
		}

		opts = append(opts, WithExternalEtcd(client))
	}

	return opts
}

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

// WithRPCAddr set rpc address
func WithRPCAddr(value string) Option {
	return func(opts *options) {
		opts.cfg.RPCAddr = value
	}
}

// WithExternalEtcd using  external etcd cluster
func WithExternalEtcd(client *clientv3.Client) Option {
	return func(opts *options) {
		opts.client = client
		opts.cfg.StorageNode = false
	}
}

// WithEmbeddedEtcd using embedded etcd cluster
func WithEmbeddedEtcd(cfg *EmbeddedEtcdCfg) Option {
	return func(opts *options) {
		opts.client = initWithEmbedEtcd(cfg)
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
