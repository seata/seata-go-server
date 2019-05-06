package proxy

import (
	"fmt"
	"io"
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/meta"
)

// Proxy sharding seata proxy
// The proxy will forward the request to the appropriate backend
type Proxy struct {
	sync.RWMutex

	Addr     string
	svr      *goetty.Server
	router   *router
	sessions map[string]*session
}

// NewProxy returns seata server proxy
func NewProxy(addr string, prophetAddrs ...string) *Proxy {
	p := &Proxy{
		Addr: addr,
		svr: goetty.NewServer(addr,
			goetty.WithServerDecoder(meta.SeataDecoder),
			goetty.WithServerEncoder(meta.SeataEncoder),
			goetty.WithServerIDGenerator(goetty.NewUUIDV4IdGenerator())),
		sessions: make(map[string]*session),
	}

	p.router = newRouter(p.handleRsp, prophetAddrs...)
	return p
}

// Start start the proxy
func (p *Proxy) Start() error {
	p.router.start()
	return p.svr.Start(p.doConnection)
}

// Stop stop the proxy
func (p *Proxy) Stop() error {
	p.svr.Stop()
	return nil
}

func (p *Proxy) doConnection(conn goetty.IOSession) error {
	s := p.addSession(conn)
	log.Infof("%s connected",
		s.id)

	defer func() {
		log.Infof("%s disconnect", s.id)
		p.removeSession(s.id)
	}()

	for {
		data, err := conn.Read()
		if err != nil {
			if err != io.EOF {
				log.Errorf("%s read error: %+v", s.id, err)
			}
			return err
		}

		var routeables []*meta.RouteableMessage
		var rsp meta.Message
		msg := data.(*meta.RPCMessage)

		switch msg.Header.MsgType {
		case meta.TypeSeataMerge:
			rsp, routeables, err = p.handleMergedWarpMessage(s, msg.Header.MsgID, msg.Body.(*meta.MergedWarpMessage))
			break
		default:
			rsp, routeables, err = p.handleByMsgType(s, msg.Header.MsgID, msg.Body)
		}

		if err != nil {
			log.Errorf("%+v", err)
			return err
		}

		if rsp != nil {
			s.addMsg(meta.AcquireRPCMessageForResponse(msg, rsp))
		}

		if len(routeables) > 0 {
			p.router.dispatch(s, routeables)
		}
	}
}

func (p *Proxy) handleByMsgType(s *session, msgID uint64, req meta.Message) (meta.Message, []*meta.RouteableMessage, error) {
	var routeables []*meta.RouteableMessage
	switch req.Type() {

	case meta.TypeRegClt:
		rsp := meta.AcquireRegisterTMResponse()
		rsp.Identified = true
		rsp.Version = req.(*meta.RegisterTMRequest).Version
		return rsp, nil, nil
	case meta.TypeRegRM:
		s.cacheRegisterRMRequest(req.(*meta.RegisterRMRequest))
		rsp := meta.AcquireRegisterRMResponse()
		rsp.Version = req.(*meta.RegisterRMRequest).Version
		rsp.Identified = true
		rsp.ResultCode = meta.Succeed
		return rsp, nil, nil
	case meta.TypeHeartbeat:
		return meta.SeataHB, s.hbs, nil
	case meta.TypeGlobalBegin:
		fid := p.router.allocFragment()
		m := s.toRouteable(fid, msgID, req)
		m.ProxyAddr = p.Addr
		routeables = append(routeables, m)
		break
	case meta.TypeBranchRegister:
		value := req.(*meta.BranchRegisterRequest)
		routeables = append(routeables, s.toRouteable(value.XID.FragmentID, msgID, req))
		break
	case meta.TypeGlobalCommit:
		value := req.(*meta.GlobalCommitRequest)
		routeables = append(routeables, s.toRouteable(value.XID.FragmentID, msgID, req))
		break
	case meta.TypeGlobalRollback:
		value := req.(*meta.GlobalRollbackRequest)
		routeables = append(routeables, s.toRouteable(value.XID.FragmentID, msgID, req))
		break
	case meta.TypeBranchStatusReport:
		value := req.(*meta.BranchReportRequest)
		routeables = append(routeables, s.toRouteable(value.XID.FragmentID, msgID, req))
		break
	case meta.TypeGlobalStatus:
		value := req.(*meta.GlobalStatusRequest)
		routeables = append(routeables, s.toRouteable(value.XID.FragmentID, msgID, req))
		break
	case meta.TypeBranchCommitResult:
		value := req.(*meta.BranchCommitResponse)
		routeables = append(routeables, s.toRouteable(value.XID.FragmentID, msgID, req))
		break
	case meta.TypeBranchRollbackResult:
		value := req.(*meta.BranchRollbackResponse)
		routeables = append(routeables, s.toRouteable(value.XID.FragmentID, msgID, req))
		break
	case meta.TypeGlobalLockQuery:
		value := req.(*meta.GlobalLockQueryRequest)
		routeables = append(routeables, s.toRouteable(value.XID.FragmentID, msgID, req))
		break
	default:
		return nil, nil, fmt.Errorf("not support msg type %d", req.Type())
	}

	return nil, routeables, nil
}

func (p *Proxy) handleMergedWarpMessage(s *session, msgID uint64, req *meta.MergedWarpMessage) (meta.Message, []*meta.RouteableMessage, error) {
	var routeables []*meta.RouteableMessage
	s.startBatch(len(req.Msgs), msgID)
	for idx, item := range req.Msgs {
		rsp, routeable, err := p.handleByMsgType(s, msgID, item)
		if err != nil {
			return nil, nil, err
		}

		if rsp != nil {
			s.addBatchRsp(msgID, idx, rsp)
		} else if len(routeable) > 0 {
			s.addBatchWaitSeq(msgID, idx, routeable[0].Seq)
			routeable[0].Batch = true
			routeables = append(routeables, routeable...)
		}
	}

	if len(routeables) == 0 {
		s.endBatch(msgID)
		return nil, nil, nil
	}

	return nil, routeables, nil
}

func (p *Proxy) handleRsp(store uint64, msg *meta.RouteableMessage) {
	switch msg.MsgType {
	case meta.TypeBranchCommit:
		p.handleNotify(msg)
		break
	case meta.TypeBranchRollback:
		p.handleNotify(msg)
		break
	default:
		p.handleBackendRsp(store, msg)
	}
}

func (p *Proxy) handleNotify(msg *meta.RouteableMessage) {
	nt := meta.AcquireRPCMessage()
	nt.Header.Flag |= meta.FlagSeataCodec
	nt.Header.MsgID = msg.MsgID
	nt.Header.MsgType = msg.MsgType
	nt.Body = msg.ReadOriginMsg()
	p.handleBackendNotify(msg.RMSID, nt)
}

func (p *Proxy) handleBackendRsp(store uint64, msg *meta.RouteableMessage) {
	p.RLock()
	if s, ok := p.sessions[msg.RMSID]; ok {
		if msg.MsgType == meta.TypeRetryNotLeader {
			retryMsg := msg.ReadOriginMsg().(*meta.RetryNotLeaderMessage)
			p.router.retryDispatch(s, retryMsg)
			p.RUnlock()
			return
		}

		if msg.Internal {
			if msg.MsgType == meta.TypeRegRMResult {
				s.setRegisted(store)
			}
			p.RUnlock()
			return
		}

		if msg.Batch {
			s.addBatchWaitRsp(msg.MsgID, msg.Seq, msg.ReadOriginMsg())
		} else {
			s.addMsg(meta.AcquireRPCMessageForResponseByMsgID(msg.MsgID, msg.ReadOriginMsg()))
		}
	} else {
		log.Warnf("handle from backend %d with no session", msg.MsgID)
	}
	p.RUnlock()
}
