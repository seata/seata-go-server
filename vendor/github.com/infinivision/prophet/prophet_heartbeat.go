package prophet

import (
	"context"
	"time"

	"github.com/fagongzi/goetty"
)

// HeartbeatHandler handle for heartbeat rsp
type HeartbeatHandler interface {
	ChangeLeader(resourceID uint64, newLeader *Peer)
	ChangePeer(resourceID uint64, peer *Peer, changeType ChangePeerType)
}

func (p *Prophet) startResourceHeartbeatLoop() {
	p.doResourceHeartbeatLoop()

	p.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(p.adapter.ResourceHBInterval())
		defer ticker.Stop()

		var conn goetty.IOSession
		for {
			select {
			case <-ctx.Done():
				if nil != conn {
					conn.Close()
				}
				return
			case <-ticker.C:
				ids := p.adapter.FetchLeaderResources()
				for _, id := range ids {
					p.resourceHBC <- id
				}
			}
		}
	})
}

func (p *Prophet) doResourceHeartbeatLoop() {
	p.runner.RunCancelableTask(func(ctx context.Context) {
		var conn goetty.IOSession
		for {
			select {
			case <-ctx.Done():
				if nil != conn {
					conn.Close()
				}
				return
			case id := <-p.resourceHBC:
				if conn == nil {
					conn = p.getLeaderClient()
				}

				hb := p.adapter.FetchResourceHB(id)
				if hb == nil {
					break
				}

				err := conn.WriteAndFlush(hb)
				if err != nil {
					conn.Close()
					conn = nil
					log.Errorf("prophet: send resource heartbeat failed, errors: %+v", err)
					break
				}

				// read rsp
				msg, err := conn.ReadTimeout(p.cfg.MaxRPCTimeout)
				if err != nil {
					conn.Close()
					conn = nil
					log.Errorf("prophet: read heartbeat rsp failed, errors: %+v", err)
					break
				}

				if rsp, ok := msg.(*errorRsp); ok {
					conn.Close()
					conn = nil
					log.Infof("prophet: read heartbeat rsp with error %s", rsp.Err)
					break
				} else if rsp, ok := msg.(*resourceHeartbeatRsp); ok {
					if rsp.NewLeader != nil {
						p.adapter.HBHandler().ChangeLeader(rsp.ResourceID, rsp.NewLeader)
					} else if rsp.Peer != nil {
						p.adapter.HBHandler().ChangePeer(rsp.ResourceID, rsp.Peer, rsp.ChangeType)
					}
				}
			}
		}
	})
}

func (p *Prophet) startContainerHeartbeatLoop() {
	p.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(p.adapter.ContainerHBInterval())
		defer ticker.Stop()

		var conn goetty.IOSession
		for {
			select {
			case <-ctx.Done():
				if nil != conn {
					conn.Close()
				}
				return
			case <-ticker.C:
				if conn == nil {
					conn = p.getLeaderClient()
				}

				req := p.adapter.FetchContainerHB()
				if req == nil {
					continue
				}

				err := conn.WriteAndFlush(req)
				if err != nil {
					conn.Close()
					conn = nil
					log.Errorf("prophet: send container heartbeat failed, errors: %+v", err)
					continue
				}

				// read rsp
				msg, err := conn.ReadTimeout(p.cfg.MaxRPCTimeout)
				if err != nil {
					conn.Close()
					conn = nil
					log.Errorf("prophet: read container rsp failed, errors: %+v", err)
					continue
				}

				if rsp, ok := msg.(*errorRsp); ok {
					conn.Close()
					conn = nil
					log.Infof("prophet: read container rsp with error %s", rsp.Err)
				}
			}
		}
	})
}
