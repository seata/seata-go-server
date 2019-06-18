package sharding

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/infinivision/prophet"
	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/core"
	"seata.io/server/pkg/meta"
)

type testTC struct {
	core.EmptyTransactionCoordinator
	leader  bool
	stopped bool
	active  int

	leaderPeer uint64
}

func newTestTC() *testTC {
	return &testTC{}
}

func (tc *testTC) ChangeLeaderTo(peerID uint64) {
	tc.leaderPeer = peerID
}

func (tc *testTC) IsLeader() bool {
	return tc.leader
}

func (tc *testTC) ActiveGCount() int {
	return tc.active
}

func (tc *testTC) Stop() {
	tc.stopped = true
}

type testStore struct {
	emptyStore
	seq      uint64
	meta     meta.StoreMeta
	sharding Transport
	cfg      Cfg
	prs      map[uint64]*PeerReplicate
	prCount  int
	lastMsg  interface{}
	rspMsg   interface{}
	addrs    map[uint64]string
	addrErr  error
}

func newTestStore() *testStore {
	return &testStore{
		prs:   make(map[uint64]*PeerReplicate),
		addrs: make(map[uint64]string),
	}
}

func (s *testStore) next() uint64 {
	value := s.seq
	s.seq++
	return value
}

func (s *testStore) CreateFragment() meta.Fragment {
	return meta.Fragment{
		ID: s.next(),
		Peers: []prophet.Peer{
			prophet.Peer{ID: s.next(), ContainerID: s.meta.ID},
		},
	}
}

func (s *testStore) AddReplicate(pr *PeerReplicate) {
	s.prs[pr.frag.ID] = pr
	s.prCount++
}

func (s *testStore) AddPeer(id uint64, peer prophet.Peer) {
	pr := s.GetFragment(id, true)
	if pr != nil {
		pr.addPeer(peer)
	}
}

func (s *testStore) RemovePeer(id uint64, peer prophet.Peer) {
	pr := s.GetFragment(id, true)
	if pr != nil {
		pr.removePeer(peer)
	}
}

func (s *testStore) Cfg() Cfg {
	return s.cfg
}

func (s *testStore) Meta() meta.StoreMeta {
	return s.meta
}

func (s *testStore) ShardingTransport() Transport {
	return s.sharding
}

func (s *testStore) GetFragment(id uint64, leader bool) *PeerReplicate {
	pr, ok := s.prs[id]
	if !ok {
		return nil
	}

	if leader && !pr.isLeader() {
		return nil
	}

	return pr
}

func (s *testStore) ForeachReplicate(f func(*PeerReplicate) bool) {
	for _, pr := range s.prs {
		if !f(pr) {
			break
		}
	}
}

func (s *testStore) HandleShardingMsg(msg interface{}) interface{} {
	s.lastMsg = msg
	return s.rspMsg
}

func (s *testStore) GetStoreAddr(storeID uint64) (string, error) {
	if s.addrErr != nil {
		return "", s.addrErr
	}

	if value, ok := s.addrs[storeID]; ok {
		return value, nil
	}

	return "", errors.New("not found")

}

type testShardingTransport struct {
	count int
	m     map[uint64]interface{}
}

func (t *testShardingTransport) Start() {}
func (t *testShardingTransport) Stop()  {}
func (t *testShardingTransport) Send(to uint64, data interface{}) {
	t.m[to] = data
	t.count++
}

func TestFindPeer(t *testing.T) {
	var peers []prophet.Peer
	peers = append(peers, prophet.Peer{
		ID:          1,
		ContainerID: 10001,
	})
	peers = append(peers, prophet.Peer{
		ID:          2,
		ContainerID: 10002,
	})
	peers = append(peers, prophet.Peer{
		ID:          3,
		ContainerID: 10003,
	})

	target, ok := findPeer(peers, 10000)
	assert.True(t, !ok, "find not exists peer failed")
	assert.Equal(t, uint64(0), target.ID, "find not exists failed")

	target, ok = findPeer(peers, 10001)
	assert.True(t, ok, "find exists peer failed")
	assert.Equal(t, uint64(1), target.ID, "find exists peer not match")

	target, ok = findPeer(peers, 10002)
	assert.True(t, ok, "find exists peer failed")
	assert.Equal(t, uint64(2), target.ID, "find exists peer not match")

	target, ok = findPeer(peers, 10003)
	assert.True(t, ok, "find exists peer failed")
	assert.Equal(t, uint64(3), target.ID, "find exists peer not match")
}

func TestIsLeader(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	tc.leader = true
	assert.True(t, pr.isLeader(), "check leader failed")

	tc.leader = false
	assert.True(t, !pr.isLeader(), "check leader failed")
}

func TestAddPeer(t *testing.T) {
	pr := new(PeerReplicate)
	pr.heartbeatsMap = &sync.Map{}
	pr.frag = meta.Fragment{ID: 1}

	pr.addPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	assert.Equal(t, 1, len(pr.frag.Peers), "add peer failed")
	assert.Equal(t, uint64(1), pr.frag.Version, "check version failed")

	pr.addPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	assert.Equal(t, 1, len(pr.frag.Peers), "add peer failed")
	assert.Equal(t, uint64(1), pr.frag.Version, "check version failed")

	pr.addPeer(prophet.Peer{ID: 2, ContainerID: 10002})
	assert.Equal(t, 2, len(pr.frag.Peers), "add peer failed")
	assert.Equal(t, uint64(2), pr.frag.Version, "check version failed")
}

func TestRemovePeer(t *testing.T) {
	pr := new(PeerReplicate)
	pr.heartbeatsMap = &sync.Map{}
	pr.frag = meta.Fragment{ID: 1}

	pr.addPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	pr.addPeer(prophet.Peer{ID: 2, ContainerID: 10002})
	pr.addPeer(prophet.Peer{ID: 3, ContainerID: 10003})
	version := pr.frag.Version

	pr.removePeer(prophet.Peer{ID: 4, ContainerID: 10004})
	assert.Equal(t, 3, len(pr.frag.Peers), "remove peer failed")
	assert.Equal(t, version, pr.frag.Version, "check version failed")

	pr.removePeer(prophet.Peer{ID: 1, ContainerID: 10001})
	assert.Equal(t, 2, len(pr.frag.Peers), "remove peer failed")
	assert.Equal(t, version+1, pr.frag.Version, "check version failed")
}

func TestRemovePendingPeer(t *testing.T) {
	pr := new(PeerReplicate)
	pr.heartbeatsMap = &sync.Map{}
	pr.frag = meta.Fragment{ID: 1}

	pr.addPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	assert.Equal(t, 1, len(pr.pendingPeers), "remove pending peer failed")

	pr.removePendingPeer(prophet.Peer{ID: 2, ContainerID: 10002})
	assert.Equal(t, 1, len(pr.pendingPeers), "remove pending peer failed")

	pr.removePendingPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	assert.Equal(t, 0, len(pr.pendingPeers), "remove pending peer failed")
}

func TestDestroy(t *testing.T) {
	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc

	pr.destroy()
	assert.True(t, tc.stopped, "destory failed")
}

func TestCollectPendingPeers(t *testing.T) {
	pr := new(PeerReplicate)
	pr.heartbeatsMap = &sync.Map{}
	pr.frag = meta.Fragment{ID: 1}

	pr.addPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	pendings := pr.collectPendingPeers()
	assert.Equal(t, 1, len(pendings), "check pending peer failed")

	pr.addPeer(prophet.Peer{ID: 2, ContainerID: 10002})
	pendings = pr.collectPendingPeers()
	assert.Equal(t, 2, len(pendings), "check pending peer failed")

	pr.addPeer(prophet.Peer{ID: 3, ContainerID: 10003})
	pendings = pr.collectPendingPeers()
	assert.Equal(t, 3, len(pendings), "check pending peer failed")

	pr.removePendingPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	pendings = pr.collectPendingPeers()
	assert.Equal(t, 2, len(pendings), "check pending peer failed")

	pr.removePendingPeer(prophet.Peer{ID: 2, ContainerID: 10002})
	pendings = pr.collectPendingPeers()
	assert.Equal(t, 1, len(pendings), "check pending peer failed")

	pr.removePendingPeer(prophet.Peer{ID: 3, ContainerID: 10003})
	pendings = pr.collectPendingPeers()
	assert.Equal(t, 0, len(pendings), "check pending peer failed")
}

func TestCollectDownPeers(t *testing.T) {
	pr := new(PeerReplicate)
	pr.heartbeatsMap = &sync.Map{}
	pr.frag = meta.Fragment{ID: 1}

	pr.addPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	pr.addPeer(prophet.Peer{ID: 2, ContainerID: 10002})
	pr.addPeer(prophet.Peer{ID: 3, ContainerID: 10003})

	time.Sleep(time.Millisecond)
	downs := pr.collectDownPeers(time.Second)
	assert.Equal(t, 0, len(downs), "check down peer failed")

	downs = pr.collectDownPeers(time.Millisecond)
	assert.Equal(t, 3, len(downs), "check down peer failed")
}

func TestDoHB(t *testing.T) {
	trans := &testShardingTransport{m: make(map[uint64]interface{})}
	s := newTestStore()
	s.sharding = trans
	s.meta.ID = 10001

	pr := new(PeerReplicate)
	pr.store = s

	pr.heartbeatsMap = &sync.Map{}
	pr.frag = meta.Fragment{ID: 1}

	pr.addPeer(prophet.Peer{ID: 1, ContainerID: 10001})
	pr.addPeer(prophet.Peer{ID: 2, ContainerID: 10002})
	pr.addPeer(prophet.Peer{ID: 3, ContainerID: 10003})

	pr.doHB()
	assert.Equal(t, 2, trans.count, "check pr heartbeat failed")
	for to := range trans.m {
		if to == s.meta.ID {
			assert.Fail(t, "check pr heartbeat failed")
		}
	}
}

func TestDoCheckConcurrency(t *testing.T) {
	s := newTestStore()
	s.meta.ID = 10001
	tc := newTestTC()
	s.cfg.TC = tc
	pr := new(PeerReplicate)
	pr.store = s
	pr.tc = tc
	pr.heartbeatsMap = &sync.Map{}
	pr.frag = meta.Fragment{ID: 1, Peers: []prophet.Peer{
		prophet.Peer{ID: 1, ContainerID: 10001},
		prophet.Peer{ID: 2, ContainerID: 10002},
		prophet.Peer{ID: 3, ContainerID: 10003},
	}}

	pr.frag.DisableGrow = true
	pr.doCheckConcurrency()
	assert.Equal(t, uint64(0), pr.overloadCheckTimes, "check pr concurrency failed")
	assert.Equal(t, uint64(0), pr.overloadTimes, "check pr concurrency failed")

	pr.frag.DisableGrow = false
	s.cfg.Concurrency = 100
	tc.active = 99
	pr.doCheckConcurrency()
	assert.Equal(t, uint64(1), pr.overloadCheckTimes, "check pr concurrency failed")
	assert.Equal(t, uint64(0), pr.overloadTimes, "check pr concurrency failed")

	s.cfg.Concurrency = 100
	tc.active = 100
	pr.overloadCheckTimes = 0
	pr.overloadTimes = 0
	pr.doCheckConcurrency()
	assert.Equal(t, uint64(1), pr.overloadCheckTimes, "check pr concurrency failed")
	assert.Equal(t, uint64(0), pr.overloadTimes, "check pr concurrency failed")

	s.cfg.Concurrency = 100
	s.cfg.OverloadPeriod = 10
	s.cfg.OverloadPercentage = 8
	tc.active = 101
	pr.overloadCheckTimes = 0
	pr.overloadTimes = 0
	for i := uint64(0); i < s.cfg.OverloadPeriod; i++ {
		pr.doCheckConcurrency()
		if i < s.cfg.OverloadPercentage {
			assert.Equal(t, i+1, pr.overloadCheckTimes, "check pr concurrency failed")
			assert.Equal(t, i+1, pr.overloadTimes, "check pr concurrency failed")
		}
	}

	assert.True(t, pr.frag.DisableGrow, "check pr concurrency failed")
	assert.Equal(t, 1, s.prCount, "check pr concurrency failed")
}
