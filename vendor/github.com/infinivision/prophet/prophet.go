package prophet

import (
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/fagongzi/goetty"
)

// RoleChangeHandler prophet role change handler
type RoleChangeHandler interface {
	BecomeLeader()
	BecomeFollower()
}

// Adapter prophet adapter
type Adapter interface {
	// NewResource return a new resource
	NewResource() Resource
	// NewContainer return a new container
	NewContainer() Container
	// FetchLeaderResources fetch loacle leader resource
	FetchLeaderResources() []uint64
	// FetchResourceHB fetch resource HB
	FetchResourceHB(id uint64) *ResourceHeartbeatReq
	// FetchContainerHB fetch container HB
	FetchContainerHB() *ContainerHeartbeatReq
	// ResourceHBInterval fetch resource HB interface
	ResourceHBInterval() time.Duration
	// ContainerHBInterval fetch container HB interface
	ContainerHBInterval() time.Duration
	// HBHandler HB hander
	HBHandler() HeartbeatHandler
}

// Prophet is the distributed scheduler and coordinator
type Prophet interface {
	// Start start the prophet instance, this will start the lead election, heartbeat loop and listen requests
	Start()
	// GetStore returns the Store
	GetStore() Store
	// GetRPC returns the RPC client
	GetRPC() RPC
	// GetEtcdClient returns the internal etcd instance
	GetEtcdClient() *clientv3.Client
}

type defaultProphet struct {
	sync.Mutex
	adapter     Adapter
	opts        *options
	cfg         *Cfg
	store       Store
	rt          *Runtime
	coordinator *Coordinator
	node        *Node
	leader      *Node
	leaderFlag  int64
	signature   string
	tcpL        *goetty.Server
	runner      *Runner
	completeC   chan struct{}
	rpc         *simpleRPC
	bizCodec    *codec

	wn          *watcherNotifier
	resourceHBC chan uint64
}

// NewProphet returns a prophet instance
func NewProphet(name string, adapter Adapter, opts ...Option) Prophet {
	value := &options{cfg: &Cfg{}}
	for _, opt := range opts {
		opt(value)
	}
	value.adjust()

	p := new(defaultProphet)
	p.opts = value
	p.cfg = value.cfg
	p.adapter = adapter
	p.bizCodec = &codec{adapter: adapter}
	p.leaderFlag = 0
	p.node = &Node{
		Name: name,
		Addr: p.cfg.RPCAddr,
	}
	p.signature = p.node.marshal()
	p.store = newEtcdStore(value.client, adapter, p.signature)
	p.runner = NewRunner()
	p.coordinator = newCoordinator(value.cfg, p.runner, p.rt)
	p.tcpL = goetty.NewServer(p.cfg.RPCAddr,
		goetty.WithServerDecoder(goetty.NewIntLengthFieldBasedDecoder(p.bizCodec)),
		goetty.WithServerEncoder(goetty.NewIntLengthFieldBasedEncoder(p.bizCodec)))
	p.completeC = make(chan struct{})
	p.rpc = newSimpleRPC(p)
	p.resourceHBC = make(chan uint64, 512)

	return p
}

// Start start the prophet
func (p *defaultProphet) Start() {
	p.startListen()
	p.startLeaderLoop()
	p.startResourceHeartbeatLoop()
	p.startContainerHeartbeatLoop()
}

// GetStore returns the store
func (p *defaultProphet) GetStore() Store {
	return p.store
}

// GetRPC returns the rpc interface
func (p *defaultProphet) GetRPC() RPC {
	return p.rpc
}

// GetEtcdClient return etcd client for reuse
func (p *defaultProphet) GetEtcdClient() *clientv3.Client {
	return p.opts.client
}
