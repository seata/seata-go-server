package proxy

import (
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/taas/pkg/meta"
)

type transport struct {
	id     uint64
	addr   string
	state  uint64
	conn   goetty.IOSession
	queue  *task.Queue
	router *router
}

func newTransport(id uint64, addr string, router *router) *transport {
	return &transport{
		id:     id,
		addr:   addr,
		queue:  task.New(1024),
		router: router,
	}
}

func (t *transport) start() {
	t.resetConn()
	go t.writeLoop()
}

func (t *transport) stop() {
	atomic.StoreUint64(&t.state, 1)
	t.queue.Dispose()
}

func (t *transport) sent(reqs ...interface{}) error {
	return t.queue.Put(reqs...)
}

func (t *transport) stopped() bool {
	return atomic.LoadUint64(&t.state) == 1
}

func (t *transport) doSend(items []interface{}, n int64) error {
	for i := int64(0); i < n; i++ {
		t.conn.Write(items[i])
	}

	return t.conn.Flush()
}

func (t *transport) readLoop(conn goetty.IOSession) {
	log.Infof("[transport-%d]: start read loop", t.id)

	for {
		data, err := conn.Read()
		if err != nil {
			log.Errorf("[transport-%d]: stop read loop with error %+v",
				t.id,
				err)
			return
		}

		t.router.cb(t.id, data.(*meta.RouteableMessage))
	}
}

func (t *transport) writeLoop() {
	log.Infof("[transport-%d]: start write loop", t.id)

	defer func() {
		if err := recover(); err != nil {
			log.Errorf("[transport-%d]: crashed %+v",
				t.id,
				err)
		}
	}()

	items := make([]interface{}, batch, batch)

OUTER:
	for {
		n, err := t.queue.Get(batch, items)
		if err != nil || t.stopped() {
			t.conn.Close()
			log.Errorf("[transport-%d]: exit write loop", t.id)
			return
		}

		for {
			err := t.doSend(items, n)
			if err == nil || t.stopped() {
				continue OUTER
			}

			log.Errorf("[transport-%d]: %d request sent failed, %+v, retry",
				t.id,
				n,
				err)
			t.resetConn()
		}
	}
}

func (t *transport) resetConn() {
	if t.conn != nil {
		t.conn.Close()
	}

	t.conn = goetty.NewConnector(t.addr,
		goetty.WithClientEncoder(meta.ProxyEncoder),
		goetty.WithClientDecoder(meta.ProxyDecoder))
	for {
		_, err := t.conn.Connect()
		if err == nil {
			break
		}

		log.Errorf("[transport-%d]: connect failed, errors:\n%+v",
			t.id,
			err)
		time.Sleep(time.Second * 2)
	}

	go t.readLoop(t.conn)
	log.Infof("connect to %s succeed",
		t.addr)
}
