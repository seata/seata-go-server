package sharding

import (
	"sync"
	"testing"
	"time"

	"github.com/infinivision/prophet"
	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/meta"
)

func TestNewResource(t *testing.T) {
	pa := new(ProphetAdapter)
	value := pa.NewResource()
	_, ok := value.(*ResourceAdapter)
	assert.True(t, ok, "new resource failed")
}

func TestNewContainer(t *testing.T) {
	pa := new(ProphetAdapter)
	value := pa.NewContainer()
	_, ok := value.(*ContainerAdapter)
	assert.True(t, ok, "new container failed")
}

func TestFetchResourceHB(t *testing.T) {
	tc := newTestTC()
	s := newTestStore()
	pa := &ProphetAdapter{
		store: s,
	}
	assert.Nil(t, pa.FetchResourceHB(1), "fetch resource hb failed")

	pr := new(PeerReplicate)
	pr.heartbeatsMap = &sync.Map{}
	pr.store = s
	pr.tc = tc
	pr.frag = meta.Fragment{ID: 1, Peers: []prophet.Peer{
		prophet.Peer{ID: 1, ContainerID: 10001},
		prophet.Peer{ID: 2, ContainerID: 10002},
		prophet.Peer{ID: 3, ContainerID: 10003},
	}}
	s.AddReplicate(pr)

	tc.leader = false
	assert.Nil(t, pa.FetchResourceHB(1), "fetch resource hb failed")

	tc.leader = true
	assert.NotNil(t, pa.FetchResourceHB(1), "fetch resource hb failed")
}

func TestFetchLeaderResources(t *testing.T) {
	s := newTestStore()
	pa := &ProphetAdapter{
		store: s,
	}

	assert.Equal(t, 0, len(pa.FetchLeaderResources()), "fetch leader resources failed")

	tc := newTestTC()
	pr := new(PeerReplicate)
	pr.tc = tc
	pr.frag = meta.Fragment{ID: 1, Peers: []prophet.Peer{
		prophet.Peer{ID: 1, ContainerID: 10001},
		prophet.Peer{ID: 2, ContainerID: 10002},
		prophet.Peer{ID: 3, ContainerID: 10003},
	}}
	s.AddReplicate(pr)

	tc.leader = false
	assert.Equal(t, 0, len(pa.FetchLeaderResources()), "fetch leader resources failed")

	tc.leader = true
	assert.Equal(t, 1, len(pa.FetchLeaderResources()), "fetch leader resources failed")
}

func TestFetchContainerHB(t *testing.T) {
	s := newTestStore()
	pa := &ProphetAdapter{
		store: s,
	}

	assert.NotNil(t, pa.FetchContainerHB(), "fetch container hb failed")
}

func TestResourceHBInterval(t *testing.T) {
	s := newTestStore()
	pa := &ProphetAdapter{
		store: s,
	}
	s.cfg.FragHBInterval = time.Second

	assert.Equal(t, s.cfg.FragHBInterval, pa.ResourceHBInterval(), "check resource HB interval failed")
}

func TestContainerHBInterval(t *testing.T) {
	s := newTestStore()
	pa := &ProphetAdapter{
		store: s,
	}
	s.cfg.StoreHBInterval = time.Second

	assert.Equal(t, s.cfg.StoreHBInterval, pa.ContainerHBInterval(), "check container HB interval failed")
}

func TestHBHandler(t *testing.T) {
	s := newTestStore()
	pa := &ProphetAdapter{
		store: s,
	}
	assert.Equal(t, pa, pa.HBHandler(), "check HB handler failed")
}

func TestChangeLeader(t *testing.T) {
	tc := newTestTC()
	s := newTestStore()
	pa := &ProphetAdapter{
		store: s,
	}

	pa.ChangeLeader(1, &prophet.Peer{ID: 2, ContainerID: 10001})
	assert.Equal(t, uint64(0), tc.leaderPeer, "changer leader to failed")

	pr := new(PeerReplicate)
	pr.tc = tc
	pr.frag = meta.Fragment{ID: 1, Peers: []prophet.Peer{
		prophet.Peer{ID: 1, ContainerID: 10001},
		prophet.Peer{ID: 2, ContainerID: 10002},
		prophet.Peer{ID: 3, ContainerID: 10003},
	}}
	s.AddReplicate(pr)

	tc.leader = false
	pa.ChangeLeader(1, &prophet.Peer{ID: 2, ContainerID: 10001})
	assert.Equal(t, uint64(0), tc.leaderPeer, "changer leader to failed")

	tc.leader = true
	pa.ChangeLeader(2, &prophet.Peer{ID: 2, ContainerID: 10001})
	assert.Equal(t, uint64(0), tc.leaderPeer, "changer leader to failed")

	pa.ChangeLeader(1, &prophet.Peer{ID: 2, ContainerID: 10001})
	assert.Equal(t, uint64(2), tc.leaderPeer, "changer leader to failed")
}

func TestChangePeer(t *testing.T) {
	tc := newTestTC()
	s := newTestStore()
	pa := &ProphetAdapter{
		store: s,
	}

	pr := new(PeerReplicate)
	pr.tc = tc
	pr.heartbeatsMap = &sync.Map{}
	pr.frag = meta.Fragment{ID: 1}
	s.AddReplicate(pr)

	pa.ChangePeer(1, &prophet.Peer{ID: 1, ContainerID: 10001}, prophet.AddPeer)
	assert.Equal(t, 0, len(pr.frag.Peers), "changer leader to failed")

	tc.leader = true
	pa.ChangePeer(1, &prophet.Peer{ID: 1, ContainerID: 10001}, prophet.AddPeer)
	assert.Equal(t, 1, len(pr.frag.Peers), "changer leader to failed")

	pa.ChangePeer(2, &prophet.Peer{ID: 1, ContainerID: 10001}, prophet.RemovePeer)
	assert.Equal(t, 1, len(pr.frag.Peers), "changer leader to failed")

	pa.ChangePeer(1, &prophet.Peer{ID: 2, ContainerID: 10002}, prophet.RemovePeer)
	assert.Equal(t, 1, len(pr.frag.Peers), "changer leader to failed")

	tc.leader = false
	pa.ChangePeer(1, &prophet.Peer{ID: 1, ContainerID: 10001}, prophet.RemovePeer)
	assert.Equal(t, 1, len(pr.frag.Peers), "changer leader to failed")

	tc.leader = true
	pa.ChangePeer(1, &prophet.Peer{ID: 1, ContainerID: 10001}, prophet.RemovePeer)
	assert.Equal(t, 0, len(pr.frag.Peers), "changer leader to failed")
}
