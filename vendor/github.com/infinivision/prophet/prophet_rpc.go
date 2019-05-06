package prophet

import (
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
)

var (
	clientConnPool sync.Pool
)

// RPC prophet rpc
type RPC interface {
	TiggerContainerHeartbeat()
	TiggerResourceHeartbeat(id uint64)
	AllocID() (uint64, error)
	AskSplit(res Resource) (uint64, []uint64, error)
}

type simpleRPC struct {
	sync.RWMutex

	prophet    *Prophet
	maxConns   int
	maxIdle    time.Duration
	maxTimeout time.Duration

	version    uint64
	connsCount int
	conns      []*clientConn
}

func newSimpleRPC(prophet *Prophet) *simpleRPC {
	return &simpleRPC{
		prophet:    prophet,
		maxConns:   prophet.cfg.MaxRPCCons,
		maxIdle:    prophet.cfg.MaxRPCConnIdle,
		maxTimeout: prophet.cfg.MaxRPCTimeout,
	}
}

// AllocID returns uint64 id
func (rpc *simpleRPC) AllocID() (uint64, error) {
	conn, err := rpc.acquireConn()
	if err != nil {
		return 0, err
	}

	for {
		err = conn.c.WriteAndFlush(&allocIDReq{})
		if err != nil {
			rpc.closeConn(conn)
			return 0, err
		}

		value, err := conn.c.ReadTimeout(rpc.maxTimeout)
		if err != nil {
			rpc.closeConn(conn)
			return 0, err
		}

		if rsp, ok := value.(*allocIDRsp); ok {
			rpc.releaseConn(conn)
			return rsp.ID, rsp.Err
		}
	}
}

// AskSplit ask split, returns the new resource id and it's peer ids
func (rpc *simpleRPC) AskSplit(res Resource) (uint64, []uint64, error) {
	conn, err := rpc.acquireConn()
	if err != nil {
		return 0, nil, err
	}

	for {
		err = conn.c.WriteAndFlush(&askSplitReq{
			Resource: res,
		})
		if err != nil {
			rpc.closeConn(conn)
			return 0, nil, err
		}

		value, err := conn.c.ReadTimeout(rpc.maxTimeout)
		if err != nil {
			rpc.closeConn(conn)
			return 0, nil, err
		}

		if rsp, ok := value.(*askSplitRsp); ok {
			rpc.releaseConn(conn)
			return rsp.NewID, rsp.NewPeerIDs, rsp.Err
		}
	}
}

func (rpc *simpleRPC) TiggerResourceHeartbeat(id uint64) {
	rpc.prophet.resourceHBC <- id
}

func (rpc *simpleRPC) TiggerContainerHeartbeat() {
	conn, err := rpc.acquireConn()
	if err != nil {
		return
	}

	for {
		err = conn.c.WriteAndFlush(rpc.prophet.adapter.FetchContainerHB())
		if err != nil {
			rpc.closeConn(conn)
			return
		}

		value, err := conn.c.ReadTimeout(rpc.maxTimeout)
		if err != nil {
			rpc.closeConn(conn)
			return
		}

		if _, ok := value.(*containerHeartbeatRsp); ok {
			rpc.releaseConn(conn)
			return
		}
	}
}

func (rpc *simpleRPC) resetConns() {
	rpc.Lock()
	rpc.version++
	rpc.connsCount = 0
	rpc.conns = make([]*clientConn, 0, 0)
	rpc.Unlock()
}

func (rpc *simpleRPC) acquireConn() (*clientConn, error) {
	var cc *clientConn
	createConn := false
	startCleaner := false

	var version uint64
	var n int
	rpc.Lock()
	version = rpc.version
	n = len(rpc.conns)
	if n == 0 {
		maxConns := rpc.maxConns
		if rpc.connsCount < maxConns {
			rpc.connsCount++
			createConn = true
		}
		if createConn && rpc.connsCount == 1 {
			startCleaner = true
		}
	} else {
		n--
		cc = rpc.conns[n]
		rpc.conns = rpc.conns[:n]
	}
	rpc.Unlock()

	if cc != nil {
		return cc, nil
	}
	if !createConn {
		return nil, fmt.Errorf("no free conns")
	}

	conn := rpc.prophet.getLeaderClient()
	cc = acquireClientConn(conn)
	cc.version = version
	if startCleaner {
		go rpc.connsCleaner()
	}
	return cc, nil
}

func (rpc *simpleRPC) decConnsCount() {
	rpc.connsCount--
}

func (rpc *simpleRPC) releaseConn(cc *clientConn) {
	cc.lastUseTime = time.Now()
	rpc.Lock()
	if cc.version != rpc.version {
		cc.c.Close()
		releaseClientConn(cc)
	} else {
		rpc.conns = append(rpc.conns, cc)
	}
	rpc.Unlock()
}

func (rpc *simpleRPC) closeConn(cc *clientConn) {
	rpc.Lock()
	if cc.version == rpc.version {
		rpc.decConnsCount()
	}
	rpc.Unlock()
	cc.c.Close()
	releaseClientConn(cc)
}

func (rpc *simpleRPC) connsCleaner() {
	var (
		scratch             []*clientConn
		mustStop            bool
		maxIdleConnDuration = rpc.maxIdle
	)

	for {
		currentTime := time.Now()

		rpc.Lock()
		conns := rpc.conns
		n := len(conns)
		i := 0
		for i < n && currentTime.Sub(conns[i].lastUseTime) > maxIdleConnDuration {
			i++
		}
		mustStop = (rpc.connsCount == i)
		scratch = append(scratch[:0], conns[:i]...)
		if i > 0 {
			m := copy(conns, conns[i:])
			for i = m; i < n; i++ {
				conns[i] = nil
			}
			rpc.conns = conns[:m]
		}
		rpc.Unlock()

		for i, cc := range scratch {
			rpc.closeConn(cc)
			scratch[i] = nil
		}
		if mustStop {
			break
		}
		time.Sleep(maxIdleConnDuration)
	}
}

type clientConn struct {
	version     uint64
	lastUseTime time.Time
	c           goetty.IOSession
}

func acquireClientConn(conn goetty.IOSession) *clientConn {
	v := clientConnPool.Get()
	if v == nil {
		v = &clientConn{}
	}
	cc := v.(*clientConn)
	cc.c = conn
	return cc
}

func releaseClientConn(cc *clientConn) {
	cc.c = nil
	cc.version = 0
	clientConnPool.Put(cc)
}
