package sharding

import (
	"errors"

	"github.com/coreos/etcd/clientv3"
	"github.com/fagongzi/util/json"
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
	leaderPeer   uint64
	ack          meta.NotifyACK
	globalStatus meta.GlobalStatus
	lockable     bool
	fid          uint64
	rspErr       error
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

func (tc *testTC) RegistryGlobalTransaction(value meta.CreateGlobalTransaction, cb func(uint64, error)) {
	cb(tc.fid, tc.rspErr)
}

func (tc *testTC) RegistryBranchTransaction(value meta.CreateBranchTransaction, cb func(uint64, error)) {
	cb(tc.fid, tc.rspErr)
}

func (tc *testTC) CommitGlobalTransaction(gid uint64, who string, cb func(meta.GlobalStatus, error)) {
	cb(tc.globalStatus, tc.rspErr)
}

func (tc *testTC) RollbackGlobalTransaction(gid uint64, who string, cb func(meta.GlobalStatus, error)) {
	cb(tc.globalStatus, tc.rspErr)
}

func (tc *testTC) ReportBranchTransactionStatus(value meta.ReportBranchStatus, cb func(error)) {
	cb(tc.rspErr)
}

func (tc *testTC) BranchTransactionNotifyACK(ack meta.NotifyACK) {
	tc.ack = ack
}

func (tc *testTC) Lockable(resource string, gid uint64, lockKeys []meta.LockKey, cb func(bool, error)) {
	cb(tc.lockable, tc.rspErr)
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
	kvs   map[string][]byte
}

func newTestStorage() *testStorage {
	return &testStorage{
		kvs: make(map[string][]byte),
	}
}

func (s *testStorage) countFragments() (int, error) {
	return len(s.frags), nil
}

func (s *testStorage) loadFragments(handleFunc func(value []byte) (uint64, error)) error {
	for _, frag := range s.frags {
		handleFunc(json.MustMarshal(&frag))
	}
	return nil
}

func (s *testStorage) removeFragment(id uint64) error {
	var newFrags []meta.Fragment
	for _, frag := range s.frags {
		if frag.ID != id {
			newFrags = append(newFrags, frag)
		}
	}

	s.frags = newFrags
	return nil
}

func (s *testStorage) putFragment(frag meta.Fragment) error {
	s.frags = append(s.frags, frag)
	return nil
}

func (s *testStorage) get(key []byte) ([]byte, error) {
	return s.kvs[string(key)], nil
}

func (s *testStorage) set(key, value []byte) error {
	s.kvs[string(key)] = value
	return nil
}

type testProphet struct {
	seq      uint64
	bootSucc bool
}

func newTestProphet() *testProphet {
	return &testProphet{}
}

func (p *testProphet) Start() {}
func (p *testProphet) GetRPC() prophet.RPC {
	return p
}
func (p *testProphet) TiggerContainerHeartbeat()         {}
func (p *testProphet) TiggerResourceHeartbeat(id uint64) {}
func (p *testProphet) AllocID() (uint64, error) {
	p.seq++
	return p.seq, nil
}
func (p *testProphet) AskSplit(res prophet.Resource) (uint64, []uint64, error) {
	return 0, nil, nil
}
func (p *testProphet) GetStore() prophet.Store {
	return p
}
func (p *testProphet) CampaignLeader(ttl int64, enableLeaderFun, disableLeaderFun func()) error {
	return nil
}
func (p *testProphet) ResignLeader() error                                          { return nil }
func (p *testProphet) GetCurrentLeader() (*prophet.Node, error)                     { return nil, nil }
func (p *testProphet) WatchLeader()                                                 {}
func (p *testProphet) PutResource(meta prophet.Resource) error                      { return nil }
func (p *testProphet) PutContainer(meta prophet.Container) error                    { return nil }
func (p *testProphet) GetContainer(id uint64) (prophet.Container, error)            { return nil, nil }
func (p *testProphet) LoadResources(limit int64, do func(prophet.Resource)) error   { return nil }
func (p *testProphet) LoadContainers(limit int64, do func(prophet.Container)) error { return nil }
func (p *testProphet) PutBootstrapped(container prophet.Container, res prophet.Resource) (bool, error) {
	return p.bootSucc, nil
}
func (p *testProphet) GetEtcdClient() *clientv3.Client {
	return nil
}
