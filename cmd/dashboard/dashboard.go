package main

import (
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/cedis"
	"github.com/infinivision/taas/pkg/core"
	"github.com/infinivision/taas/pkg/dashboard"
	"github.com/infinivision/taas/pkg/util"
)

var (
	addr                = flag.String("addr", "127.0.0.1:8080", "Addr: api http server")
	addrCell            = flag.String("addr-cell", "127.0.0.1:6379", "Addr: cell proxy address")
	addrProphet         = flag.String("addr-prophet", "127.0.0.1:6379", "Addr: cell proxy address")
	cpu                 = flag.Int("cpu", 0, "Limit: schedule threads count")
	cellReties          = flag.Int("cell-reties", 3, "retry time of operator cell")
	cellMaxActive       = flag.Int("cell-max-active", 100, "Limit: cell max active connections")
	cellMaxIdle         = flag.Int("cell-max-idle", 10, "Limit: cell max idle connections")
	cellIdleTimeoutSec  = flag.Int("cell-timeout-idle", 30, "Limit: cell connection idle timeout seconds")
	cellDailTimeoutSec  = flag.Int("cell-timeout-dail", 10, "Limit: cell connection dail timeout seconds")
	cellReadTimeoutSec  = flag.Int("cell-timeout-read", 30, "Limit: cell connection read timeout seconds")
	cellWriteTimeoutSec = flag.Int("cell-timeout-write", 10, "Limit: cell connection write timeout seconds")
	ui                  = flag.String("ui", "/app/taas/ui", "The Taas dashboard ui dist dir.")
	uiPrefix            = flag.String("ui-prefix", "/ui", "The gateway ui prefix path.")
	version             = flag.Bool("version", false, "Show version info")
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

	var opts []cedis.Option
	opts = append(opts, cedis.WithCellProxies(strings.Split(*addrCell, ",")...))
	opts = append(opts, cedis.WithMaxActive(*cellMaxActive))
	opts = append(opts, cedis.WithMaxIdle(*cellMaxIdle))
	opts = append(opts, cedis.WithDailTimeout(time.Second*time.Duration(*cellDailTimeoutSec)))
	opts = append(opts, cedis.WithIdleTimeout(time.Second*time.Duration(*cellIdleTimeoutSec)))
	opts = append(opts, cedis.WithReadTimeout(time.Second*time.Duration(*cellReadTimeoutSec)))
	opts = append(opts, cedis.WithWriteTimeout(time.Second*time.Duration(*cellWriteTimeoutSec)))

	api := core.NewCellQueryAPI(opts...)
	manual := core.NewcellManualAPI(opts...)
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
