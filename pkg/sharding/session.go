package sharding

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/taas/pkg/meta"
)

const (
	batch int64 = 32
)

var (
	closeFlag = struct{}{}
)

type session struct {
	sync.RWMutex

	id    string
	msgQ  *task.Queue
	conn  goetty.IOSession
	store *Store
}

func newSession(conn goetty.IOSession, store *Store) *session {
	s := &session{
		id:    fmt.Sprintf("%s-%s", conn.RemoteIP(), conn.ID().(string)),
		conn:  conn,
		msgQ:  task.New(128),
		store: store,
	}

	go s.writeLoop()
	return s
}

func (s *session) cb(routeMsg *meta.RouteableMessage, rsp meta.Message, err error) {
	if err != nil {
		if err != meta.ErrNotLeader {
			log.Errorf("[frag-%d]: cb with %+v",
				routeMsg.FID,
				err)
			return
		}

		leader, storeID, err := s.store.CurrentLeader(routeMsg.FID)
		if err != nil {
			log.Errorf("[frag-%d]: fetch current leader failed %+v",
				routeMsg.FID,
				err)
			return
		}

		msg := meta.AcquireRetryNotLeaderMessage()
		msg.FID = routeMsg.FID
		msg.NewLeader = leader
		msg.NewLeaderStore = storeID
		msg.RetryData = routeMsg.Encode()
		rsp = msg
	}

	log.Debugf("response %+v(%T)", rsp, rsp)
	if nil != rsp {
		err = s.addMsg(meta.AcquireRouteableMessageForResponse(routeMsg, rsp))
		if err != nil {
			log.Errorf("proxy will exit with %+v", err)
			return
		}
	}
}

func (s *session) addMsg(value interface{}) error {
	return s.msgQ.Put(value)
}

func (s *session) doClose() {
	s.msgQ.Dispose()
}

func (s *session) writeLoop() {
	items := make([]interface{}, batch, batch)
	for {
		n, err := s.msgQ.Get(batch, items)
		if err != nil {
			return
		}

		for i := int64(0); i < n; i++ {
			item := items[i]
			if item == closeFlag {
				s.doClose()
				return
			}

			s.conn.Write(item)
		}

		if n > 0 {
			s.conn.Flush()
		}
	}
}

func (f *Seata) addProxySession(conn goetty.IOSession) *session {
	f.Lock()

	s := newSession(conn, f.store)
	f.proxies[s.id] = s

	f.Unlock()
	return s
}

func (f *Seata) removeProxySession(id string) {
	f.Lock()

	if s, ok := f.proxies[id]; ok {
		s.addMsg(closeFlag)
		delete(f.proxies, id)
	}

	f.Unlock()
}

func (f *Seata) doNotify(rm meta.ResourceManager, notify meta.Notify) error {
	f.RLock()

	if s, ok := f.proxies[rm.ProxySID]; ok {
		var msg meta.Message

		switch notify.Action {
		case meta.RollbackAction:
			msg = f.doNotifyRollback(notify)
			break
		case meta.CommitAction:
			msg = f.doNotifyCommit(notify)
			break
		default:
			log.Fatalf("notify with none action")
		}

		err := s.addMsg(meta.AcquireRouteableMessageForNotify(rm.RMSID, f.nextID(), msg))
		if err != nil {
			f.RUnlock()
			return err
		}

		f.RUnlock()
		return nil
	}

	f.RUnlock()
	return fmt.Errorf("seata: proxy session %s not found", rm.ProxySID)
}

func (f *Seata) doNotifyCommit(notify meta.Notify) meta.Message {
	req := meta.AcquireBranchCommitRequest()
	req.XID = notify.XID
	req.BranchID = notify.BID
	req.ResourceID = notify.Resource
	req.BranchType = notify.BranchType
	return req
}

func (f *Seata) doNotifyRollback(notify meta.Notify) meta.Message {
	req := meta.AcquireBranchRollbackRequest()
	req.XID = notify.XID
	req.BranchID = notify.BID
	req.ResourceID = notify.Resource
	req.BranchType = notify.BranchType
	return req
}

func (f *Seata) nextID() uint64 {
	return atomic.AddUint64(&f.msgID, 1)
}
