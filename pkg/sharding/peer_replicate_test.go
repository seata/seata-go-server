package sharding

import (
	"sync"
	"testing"
	"time"

	"github.com/infinivision/prophet"
	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/meta"
)

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
	trans := newTestShardingTransport()
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
	s.cfg.tc = tc
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
