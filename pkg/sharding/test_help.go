package sharding

import (
	"errors"

	"github.com/infinivision/prophet"
	"seata.io/server/pkg/core"
	"seata.io/server/pkg/meta"
)

type testTC struct {
	core.EmptyTransactionCoordinator
	leader       bool
	stopped      bool
	active       int
	events       int
	handleEvents bool

	leaderPeer uint64
}

func newTestTC() *testTC {
	return &testTC{}
}

func (tc *testTC) HandleEvent() bool {
	if tc.handleEvents {
		tc.events++
	}

	return tc.handleEvents
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

func newTestShardingTransport() *testShardingTransport {
	return &testShardingTransport{m: make(map[uint64]interface{})}
}

func (t *testShardingTransport) Start() {}
func (t *testShardingTransport) Stop()  {}
func (t *testShardingTransport) Send(to uint64, data interface{}) {
	t.m[to] = data
	t.count++
}

type testStorage struct {
	emptyStorage

	frags []meta.Fragment
}

func newTestStorage() *testStorage {
	return &testStorage{}
}

func (s *testStorage) putFragment(frag meta.Fragment) error {
	s.frags = append(s.frags, frag)
	return nil
}
