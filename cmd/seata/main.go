package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/prophet"
	"seata.io/server/pkg/core"
	"seata.io/server/pkg/election"
	"seata.io/server/pkg/id"
	"seata.io/server/pkg/metrics"
	"seata.io/server/pkg/sharding"
	"seata.io/server/pkg/storage"
	"seata.io/server/pkg/util"
)

var (
	waitSeconds                        = flag.Int("wait", 0, "wait seconds")
	nodeID                             = flag.Uint("id", 0, "Node ID")
	addr                               = flag.String("addr", "127.0.0.1:8080", "Addr: seata server")
	addrStorage                        = flag.String("addr-store", "cell://127.0.0.1:6379", "Addr: meta storage addresss with protocol")
	addrPeer                           = flag.String("addr-peer", "127.0.0.1:8081", "Addr: sharding fragment addr")
	addrPPROF                          = flag.String("addr-pprof", "", "Addr: pprof addr")
	dataPath                           = flag.String("data", "/tmp/seata", "Seata local data path")
	zone                               = flag.String("zone", "zone-1", "Zone label")
	rack                               = flag.String("rack", "rack-1", "Rack label")
	cpu                                = flag.Int("cpu", 0, "Limit: schedule threads count")
	rmLeaseSec                         = flag.Int("rm-lease", 30, "Limit: rm lease seconds")
	transactionTimeoutSec              = flag.Int("timeout-transaction", 30, "Limit: transaction timeout seconds")
	transportWorkerCount               = flag.Int("transport-worker", 1, "transport worker count")
	prWorkerCount                      = flag.Int("fragment-worker", 128, "fragment worker count")
	ackTimeout                         = flag.Int("timeout-ack", 30, "Limit: RM ack timeout seconds")
	commitIfAllBranchSucceedInPhaseOne = flag.Bool("commit-on-timeout", false, "Enable: Commit the global transaction if all branch transaction was succeed on timeout")
	electionLockPath                   = flag.String("election-lock-path", "/tmp/seata/lock/election", "election lock path")
	electionLeaderPath                 = flag.String("election-leader-path", "/tmp/seata/election", "election leader path")
	electionLease                      = flag.Int64("election-lease", 5, "election leader lease seconds")
	storeHBIntervalSec                 = flag.Int("heartbeat-store", 30, "HB(sec): store heartbeat")
	fragHBIntervalSec                  = flag.Int("heartbeat-frag", 5, "HB(sec): fragment heartbeat")
	maxPeerDownSec                     = flag.Int("peer-max-downtime", 30, "Max(sec): max peer down time in seconds")
	initFragmentCounts                 = flag.Int("init", 1, "Count: init fragment count")
	concurrency                        = flag.Int("concurrency", 256, "Count: fragment max concurrent")
	overloadPercentage                 = flag.Uint64("overload-percentage", 10, "Percentage of overload times in the statistical period")
	overloadPeriod                     = flag.Uint64("overload-period", 60, "Statistical overload period in seconds")

	// metrics
	prometheusJob             = flag.String("metrics-job", "seata", "Prometheus job name")
	prometheusPushgateway     = flag.String("metrics-push-addr", "", "Prometheus pushgateway address")
	prometheusPushIntervalSec = flag.Int("metrics-push-interval", 0, "Prometheus metrics push interval in seconds")

	version = flag.Bool("version", false, "Show version info")
)

var (
	prophetName = ""
)

func main() {
	flag.Parse()
	if *version && util.PrintVersion() {
		os.Exit(0)
	}

	if *waitSeconds > 0 {
		time.Sleep(time.Second * time.Duration(*waitSeconds))
	}

	prophetName = fmt.Sprintf("p%d", *nodeID)

	log.InitLog()
	prophet.SetLogger(&adapterLog{})

	if *cpu == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(*cpu)
	}

	if *addrPPROF != "" {
		go func() {
			log.Errorf("start pprof failed, errors:\n%+v",
				http.ListenAndServe(*addrPPROF, nil))
		}()
	}

	metrics.Push(&metrics.MetricConfig{
		PushJob:      *prometheusJob,
		PushAddress:  *prometheusPushgateway,
		PushInterval: time.Second * time.Duration(*prometheusPushIntervalSec),
	})

	s, err := sharding.NewSeata(sharding.Cfg{
		Addr:                *addr,
		ShardingAddr:        *addrPeer,
		DataPath:            *dataPath,
		ProphetName:         prophetName,
		RMLease:             time.Second * time.Duration(*rmLeaseSec),
		CoreOptions:         parseCoreOptions(),
		FragHBInterval:      time.Second * time.Duration(*fragHBIntervalSec),
		StoreHBInterval:     time.Second * time.Duration(*storeHBIntervalSec),
		MaxPeerDownDuration: time.Second * time.Duration(*maxPeerDownSec),
		Labels: []prophet.Pair{
			prophet.Pair{
				Key:   "zone",
				Value: *zone,
			},
			prophet.Pair{
				Key:   "rack",
				Value: *rack,
			},
		},
		InitFragments:      *initFragmentCounts,
		Concurrency:        *concurrency,
		OverloadPercentage: *overloadPercentage,
		OverloadPeriod:     *overloadPeriod,
		TransWorkerCount:   *transportWorkerCount,
		PRWorkerCount:      *prWorkerCount,
	})
	if err != nil {
		log.Fatalf("create seata server failed, %+v", err)
	}

	go s.Start()

	waitStop(s)
}

func waitStop(s *sharding.Seata) {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	s.Stop()
	log.Infof("exit: signal=<%d>.", sig)
	switch sig {
	case syscall.SIGTERM:
		log.Infof("exit: bye :-).")
		os.Exit(0)
	default:
		log.Infof("exit: bye :-(.")
		os.Exit(1)
	}
}

func parseCoreOptions() []core.Option {
	store, err := storage.CreateStorage(*addrStorage)
	if err != nil {
		log.Fatalf("init storage failed with %+v", err)
	}

	var opts []core.Option
	opts = append(opts, core.WithIDGenerator(id.NewSnowflakeGenerator(uint16(*nodeID))))
	opts = append(opts, core.WithTransactionTimeout(time.Second*time.Duration(*transactionTimeoutSec)))
	opts = append(opts, core.WithACKTimeout(time.Second*time.Duration(*ackTimeout)))
	opts = append(opts, core.WithCommitIfAllBranchSucceedInPhaseOne(*commitIfAllBranchSucceedInPhaseOne))
	opts = append(opts, core.WithStorage(store))
	opts = append(opts, core.WithElectorOptions(election.WithLeaderLeaseSec(*electionLease),
		election.WithLeaderPath(*electionLeaderPath),
		election.WithLockPath(*electionLockPath)))

	return opts
}

type adapterLog struct{}

func (l *adapterLog) Info(v ...interface{}) {
	log.Info(v...)
}

func (l *adapterLog) Infof(format string, v ...interface{}) {
	log.Infof(format, v...)
}

func (l *adapterLog) Debug(v ...interface{}) {
	log.Debug(v...)
}

func (l *adapterLog) Debugf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

func (l *adapterLog) Warn(v ...interface{}) {
	log.Warn(v...)
}

func (l *adapterLog) Warnf(format string, v ...interface{}) {
	log.Warnf(format, v...)
}

func (l *adapterLog) Error(v ...interface{}) {}

func (l *adapterLog) Errorf(format string, v ...interface{}) {
	log.Errorf(format, v...)
}

func (l *adapterLog) Fatal(v ...interface{}) {
	log.Fatal(v...)
}

func (l *adapterLog) Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}
