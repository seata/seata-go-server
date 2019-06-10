package main

import (
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/fagongzi/log"
	"seata.io/server/pkg/core"
	"seata.io/server/pkg/dashboard"
	"seata.io/server/pkg/storage"
	"seata.io/server/pkg/util"
)

var (
	addr        = flag.String("addr", "127.0.0.1:8080", "Addr: seata api http server")
	addrStorage = flag.String("addr-store", "cell://127.0.0.1:6379", "Addr: meta storage addresss with protocol")
	addrProphet = flag.String("addr-prophet", "127.0.0.1:2379", "Addr: prophet address")
	cpu         = flag.Int("cpu", 0, "Limit: schedule threads count")
	ui          = flag.String("ui", "/app/seata/ui", "The seata dashboard ui dist dir.")
	uiPrefix    = flag.String("ui-prefix", "/ui", "The dashboard ui prefix path.")
	version     = flag.Bool("version", false, "Show version info")
)

func main() {
	flag.Parse()

	if *version && util.PrintVersion() {
		os.Exit(0)
	}

	log.InitLog()

	if *cpu == 0 {
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		runtime.GOMAXPROCS(*cpu)
	}

	store, err := storage.CreateStorage(*addrStorage)
	if err != nil {
		log.Fatalf("init storage failed with %+v", err)
	}

	api := core.NewQueryAPI(store)
	manual := core.NewManualAPI(store)
	s := dashboard.NewDashboard(dashboard.Cfg{
		Addr:         *addr,
		UI:           *ui,
		UIPrefix:     *uiPrefix,
		ProphetAddrs: strings.Split(*addrProphet, ","),
	}, api, manual)

	go s.Start()

	waitStop(s)
}

func waitStop(s *dashboard.Dashboard) {
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
