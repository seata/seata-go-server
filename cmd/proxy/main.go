package main

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/fagongzi/log"
	"github.com/infinivision/prophet"
	"github.com/infinivision/taas/pkg/proxy"
	"github.com/infinivision/taas/pkg/util"
)

var (
	addr        = flag.String("addr", "127.0.0.1:7070", "Addr: seata server")
	addrProphet = flag.String("addr-prophet", "127.0.0.1:6379", "Addr: cell proxy address")
	addrPPROF   = flag.String("addr-pprof", "", "Addr: pprof addr")
	cpu         = flag.Int("cpu", 0, "Limit: schedule threads count")
	version     = flag.Bool("version", false, "Show version info")
)

func main() {
	flag.Parse()
	if *version && util.PrintVersion() {
		os.Exit(0)
	}

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

	p := proxy.NewProxy(*addr, strings.Split(*addrProphet, ",")...)
	go p.Start()

	waitStop(p)
}

func waitStop(p *proxy.Proxy) {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	p.Stop()
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
