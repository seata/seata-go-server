package sharding

import (
	"io"
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/meta"
)

// Seata seata compatibled server
type Seata struct {
	sync.RWMutex

	cfg     Cfg
	msgID   uint64
	store   *Store
	svr     *goetty.Server
	proxies map[string]*session
}

// NewSeata creates a seata compatibled server
func NewSeata(cfg Cfg) (*Seata, error) {
	f := &Seata{
		cfg: cfg,
		svr: goetty.NewServer(cfg.Addr,
			goetty.WithServerDecoder(meta.ProxyDecoder),
			goetty.WithServerEncoder(meta.ProxyEncoder),
			goetty.WithServerIDGenerator(goetty.NewUUIDV4IdGenerator())),
		proxies: make(map[string]*session),
	}

	f.cfg.TransSendCB = f.doNotify
	f.store = NewStore(f.cfg)
	return f, nil
}

// Start start
func (f *Seata) Start() error {
	f.store.Start()

	return f.svr.Start(func(conn goetty.IOSession) error {
		s := f.addProxySession(conn)
		log.Infof("%s connected",
			s.id)

		defer func() {
			log.Infof("%s disconnect", s.id)
			f.removeProxySession(s.id)
		}()

		for {
			data, e := conn.Read()
			if e != nil {
				if e != io.EOF {
					log.Errorf("read failed with %+v", e)
				}
				return e
			}

			log.Debugf("received %+v", data)

			routeMsg := data.(*meta.RouteableMessage)
			f.store.HandleRoutable(s, routeMsg)
		}
	})
}

// Stop stop seata
func (f *Seata) Stop() {
	f.svr.Stop()
}
