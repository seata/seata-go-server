package sharding

import (
	"fmt"
	"io"
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"seata.io/server/pkg/meta"
)

// Seata seata compatibled server
type Seata struct {
	sync.RWMutex

	cfg     Cfg
	msgID   uint64
	store   Store
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
			f.handle(s, routeMsg)
		}
	})
}

// Stop stop seata
func (f *Seata) Stop() {
	f.svr.Stop()
}

func (f *Seata) handle(ss *session, msg *meta.RouteableMessage) {
	if msg.MsgType == meta.TypeHeartbeat {
		f.store.RenewRMLease(ss.id, msg.RMSID)
		return
	} else if msg.MsgType == meta.TypeRegRM {
		f.handleRegisterRM(ss, msg)
		return
	} else if msg.MsgType == meta.TypeRegClt {
		f.handleRegisterTM(ss, msg)
		return
	}

	pr := f.store.GetFragment(msg.FID, true)
	if pr == nil {
		ss.cb(msg, nil, meta.ErrNotLeader)
		return
	}

	switch msg.MsgType {
	case meta.TypeGlobalBegin:
		pr.handleGlobalBeginRequest(ss, msg)
		break
	case meta.TypeBranchRegister:
		pr.handleBranchRegisterRequest(ss, msg)
		break
	case meta.TypeGlobalCommit:
		pr.handleGlobalCommitRequest(ss, msg)
		break
	case meta.TypeGlobalRollback:
		pr.handleGlobalRollbackRequest(ss, msg)
		break
	case meta.TypeBranchStatusReport:
		pr.handleBranchReportRequest(ss, msg)
		break
	case meta.TypeGlobalStatus:
		pr.handleGlobalStatusRequest(ss, msg)
		break
	case meta.TypeBranchCommitResult:
		pr.handleBranchCommitResponse(ss, msg)
		break
	case meta.TypeBranchRollbackResult:
		pr.handleBranchRollbackResponse(ss, msg)
		break
	case meta.TypeGlobalLockQuery:
		pr.handleGlobalLockQueryRequest(ss, msg)
		break
	default:
		ss.cb(msg, nil, fmt.Errorf("not support msg type %d", msg.MsgType))
	}
}

func (f *Seata) handleRegisterRM(ss *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.RegisterRMRequest)

	rsp := meta.AcquireRegisterRMResponse()
	rsp.Identified = false
	rsp.ResultCode = meta.Failed
	rsp.Version = req.Version

	if req.ResourceIDs == "" {
		log.Fatalf("%s register RM failed, missing resource, using unknown resource to instead", ss.id)
	}

	f.store.AddRM(req.ToResourceSet(ss.id, msg.RMSID))

	rsp.Identified = true
	rsp.ResultCode = meta.Succeed
	ss.cb(msg, rsp, nil)
}

func (f *Seata) handleRegisterTM(ss *session, msg *meta.RouteableMessage) {
	req := msg.ReadOriginMsg().(*meta.RegisterTMRequest)

	rsp := meta.AcquireRegisterTMResponse()
	rsp.Identified = true
	rsp.Version = req.Version
	ss.cb(msg, rsp, nil)
}
