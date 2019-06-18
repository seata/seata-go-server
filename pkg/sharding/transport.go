package sharding

import (
	"errors"
	"io"
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/task"
	"seata.io/server/pkg/meta"
)

var (
	errConnect = errors.New("not connected")
)

// Transport sharding transport
type Transport interface {
	Start()
	Stop()

	Send(uint64, interface{})
}

type sendMsg struct {
	to   uint64
	data interface{}
}

type shardingTransport struct {
	sync.RWMutex

	store             Store
	server            *goetty.Server
	storeAddrDetector func(storeID uint64) (string, error)
	addrs             *sync.Map
	addrsRevert       *sync.Map
	conns             map[uint64]goetty.IOSessionPool
	msgs              *task.Queue
}

func newShardingTransport(store Store) Transport {
	t := &shardingTransport{
		server: goetty.NewServer(store.Meta().Addr,
			goetty.WithServerDecoder(meta.ShardingDecoder),
			goetty.WithServerEncoder(meta.ShardingEncoder)),
		conns:       make(map[uint64]goetty.IOSessionPool),
		addrs:       &sync.Map{},
		addrsRevert: &sync.Map{},
		store:       store,
		msgs:        task.New(1024),
	}

	t.storeAddrDetector = t.getStoreAddr

	return t
}

func (t *shardingTransport) Start() {
	go t.readyToSend()
	log.Infof("sharding transport start listen at %s", t.store.Meta().Addr)
	go func() {
		err := t.server.Start(t.doConnection)
		if err != nil {
			log.Fatalf("peer transport start failed with %+v",
				err)
		}
	}()
}

func (t *shardingTransport) Stop() {
	t.msgs.Dispose()
	t.server.Stop()
	log.Infof("sharding transport stopped")
}

func (t *shardingTransport) doConnection(session goetty.IOSession) error {
	remoteIP := session.RemoteIP()

	log.Infof("%s connected", remoteIP)
	for {
		msg, err := session.Read()
		if err != nil {
			if err == io.EOF {
				log.Infof("closed by %s", remoteIP)
			} else {
				log.Warnf("read error from conn-%s, errors:\n%+v",
					remoteIP,
					err)
			}

			return err
		}

		ack := t.store.HandleShardingMsg(msg)
		if ack != nil {
			session.WriteAndFlush(ack)
		}
	}
}

func (t *shardingTransport) Send(to uint64, msg interface{}) {
	if to == t.store.Meta().ID {
		t.store.HandleShardingMsg(msg)
		return
	}

	t.msgs.Put(&sendMsg{
		to:   to,
		data: msg,
	})
}

func (t *shardingTransport) readyToSend() {
	items := make([]interface{}, batch, batch)

	for {
		n, err := t.msgs.Get(batch, items)
		if err != nil {
			log.Infof("transfer sent worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*sendMsg)
			t.doSend(msg)
		}
	}
}

func (t *shardingTransport) doSend(msg *sendMsg) error {
	conn, err := t.getConn(msg.to)
	if err != nil {
		return err
	}

	err = t.doWrite(msg.data, conn)
	t.putConn(msg.to, conn)
	return err
}

func (t *shardingTransport) doWrite(msg interface{}, conn goetty.IOSession) error {
	err := conn.WriteAndFlush(msg)
	if err != nil {
		conn.Close()
		return err
	}

	return nil
}

func (t *shardingTransport) getConn(storeID uint64) (goetty.IOSession, error) {
	conn, err := t.getConnLocked(storeID)
	if err != nil {
		return nil, err
	}

	if t.checkConnect(storeID, conn) {
		return conn, nil
	}

	t.putConn(storeID, conn)
	return nil, errConnect
}

func (t *shardingTransport) putConn(id uint64, conn goetty.IOSession) {
	t.RLock()
	pool := t.conns[id]
	t.RUnlock()

	if pool != nil {
		pool.Put(conn)
	} else {
		conn.Close()
	}
}

func (t *shardingTransport) getConnLocked(id uint64) (goetty.IOSession, error) {
	var err error

	t.RLock()
	pool := t.conns[id]
	t.RUnlock()

	if pool == nil {
		t.Lock()
		pool = t.conns[id]
		if pool == nil {
			pool, err = goetty.NewIOSessionPool(1, 1, func() (goetty.IOSession, error) {
				return t.createConn(id)
			})

			if err != nil {
				return nil, err
			}

			t.conns[id] = pool
		}
		t.Unlock()
	}

	return pool.Get()
}

func (t *shardingTransport) checkConnect(id uint64, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.IsConnected() {
		return true
	}

	ok, err := conn.Connect()
	if err != nil {
		log.Errorf("connect to store %d failure, errors:\n %+v",
			id,
			err)
		return false
	}

	// read loop
	go func() {
		for {
			data, err := conn.Read()
			if err != nil {
				return
			}

			ack := t.store.HandleShardingMsg(data)
			if ack != nil {
				log.Fatalf("%+v %T unexpect ack %T", data, data, ack)
			}
		}
	}()

	log.Infof("connected to store %d", id)
	return ok
}

func (t *shardingTransport) createConn(id uint64) (goetty.IOSession, error) {
	addr, err := t.storeAddrDetector(id)
	if err != nil {
		return nil, err
	}

	return goetty.NewConnector(addr,
		goetty.WithClientDecoder(meta.ShardingDecoder),
		goetty.WithClientEncoder(meta.ShardingEncoder)), nil
}

func (t *shardingTransport) getStoreAddr(storeID uint64) (string, error) {
	var err error
	addr, ok := t.addrs.Load(storeID)
	if !ok {
		addr, err = t.store.GetStoreAddr(storeID)
		if err != nil {
			return "", err
		}

		t.addrs.Store(storeID, addr)
		t.addrsRevert.Store(addr, storeID)
	}

	return addr.(string), nil
}
