package proxy

import (
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

	seq         uint64
	id          string
	msgQ        *task.Queue
	conn        goetty.IOSession
	registerReq *meta.RegisterRMRequest
	registerMap map[uint64]struct{}
	hbs         []*meta.RouteableMessage

	batchID  uint64
	batchSeq []uint64
	batchRsp *meta.MergeResultMessage
}

func newSession(conn goetty.IOSession) *session {
	s := &session{
		id:          conn.ID().(string),
		conn:        conn,
		msgQ:        task.New(128),
		registerMap: make(map[uint64]struct{}),
	}

	go s.writeLoop()
	return s
}

func (s *session) setRegisted(storeID uint64) {
	s.Lock()
	s.registerMap[storeID] = struct{}{}
	hb := s.toRouteableWithInternal(0, meta.SeataHB)
	hb.ToStore = storeID
	s.hbs = append(s.hbs, hb)
	s.Unlock()
}

func (s *session) isRegistered(store uint64) bool {
	s.RLock()
	_, ok := s.registerMap[store]
	s.RUnlock()
	return ok
}

func (s *session) registerRMRequest() *meta.RegisterRMRequest {
	return s.registerReq
}

func (s *session) cacheRegisterRMRequest(req *meta.RegisterRMRequest) {
	if req.ResourceIDs == "" {
		return
	}

	s.registerReq = req
}

func (s *session) startBatch(n int, msgID uint64) {
	s.Lock()
	s.batchID = msgID
	s.batchSeq = make([]uint64, n, n)
	s.batchRsp = meta.AcquireMergeResultMessage()
	s.batchRsp.Msgs = make([]meta.Message, n, n)
	s.Unlock()
}

func (s *session) addBatchWaitSeq(batchID uint64, idx int, seq uint64) {
	s.Lock()
	if s.batchID != batchID {
		s.Unlock()
		return
	}
	s.batchSeq[idx] = seq
	s.Unlock()
}

func (s *session) addBatchWaitRsp(batchID uint64, seq uint64, rsp meta.Message) {
	s.Lock()
	if s.batchID != batchID {
		log.Errorf("******************response error batch %d, expect %d", batchID, s.batchID)
		s.Unlock()
		return
	}

	for idx, wseq := range s.batchSeq {
		if wseq == seq {
			s.batchRsp.Msgs[idx] = rsp
			break
		}
	}

	for _, msg := range s.batchRsp.Msgs {
		if msg == nil {
			s.Unlock()
			return
		}
	}

	s.doEndBatch()
	s.Unlock()
}

func (s *session) addBatchRsp(batchID uint64, idx int, rsp meta.Message) {
	s.Lock()
	if s.batchID != batchID {
		s.Unlock()
		return
	}
	s.batchRsp.Msgs[idx] = rsp
	s.Unlock()
}

func (s *session) endBatch(batchID uint64) {
	s.Lock()
	if s.batchID != batchID {
		s.Unlock()
		return
	}

	s.doEndBatch()
	s.Unlock()
}

func (s *session) doEndBatch() {
	s.addMsg(meta.AcquireRPCMessageForResponseByMsgID(s.batchID, s.batchRsp))
	s.batchRsp = nil
	s.batchSeq = nil
	s.batchID = 0
}

func (s *session) toRouteableWithInternal(fid uint64, req meta.Message) *meta.RouteableMessage {
	var data []byte
	buf := req.Encode()
	if nil != buf {
		_, data, _ = buf.ReadAll()
		buf.Release()
	}
	return &meta.RouteableMessage{
		RMSID:    s.id,
		FID:      fid,
		Seq:      s.nextSeq(),
		Internal: true,
		MsgType:  req.Type(),
		Data:     data,
	}
}

func (s *session) toRouteable(fid, msgID uint64, req meta.Message) *meta.RouteableMessage {
	buf := req.Encode()
	_, data, _ := buf.ReadAll()
	buf.Release()
	return &meta.RouteableMessage{
		RMSID:   s.id,
		FID:     fid,
		Seq:     s.nextSeq(),
		MsgID:   msgID,
		MsgType: req.Type(),
		Data:    data,
	}
}

func (s *session) nextSeq() uint64 {
	return atomic.AddUint64(&s.seq, 1)
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

			log.Debugf("%s: to client: %+v", s.id, item)
			s.conn.Write(item)
		}

		if n > 0 {
			s.conn.Flush()
		}
	}
}

func (p *Proxy) addSession(conn goetty.IOSession) *session {
	p.Lock()

	s := newSession(conn)
	p.sessions[s.id] = s

	p.Unlock()
	return s
}

func (p *Proxy) handleBackendNotify(sid string, msg *meta.RPCMessage) {
	p.RLock()
	if s, ok := p.sessions[sid]; ok {
		s.addMsg(msg)
	}
	p.RUnlock()
}

func (p *Proxy) removeSession(id string) {
	p.Lock()

	if s, ok := p.sessions[id]; ok {
		s.addMsg(closeFlag)
		delete(p.sessions, id)
	}

	p.Unlock()
}
