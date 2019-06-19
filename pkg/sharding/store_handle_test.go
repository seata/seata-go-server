package sharding

import (
	"testing"

	"github.com/infinivision/prophet"
	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/meta"
)

func TestHandleShardingMsgWithHB(t *testing.T) {
	st := newTestStorage()

	cfg := Cfg{}
	cfg.storeID = 10001
	cfg.tc = newTestTC()
	cfg.storage = st
	s := NewStore(cfg)

	msg := &meta.HBMsg{}
	msg.Frag.Version = 2
	msg.Frag.ID = 1
	msg.Frag.Peers = append(msg.Frag.Peers, prophet.Peer{
		ID:          1,
		ContainerID: 10001,
	})
	rsp := s.HandleShardingMsg(msg)
	assert.Nil(t, rsp, "handle hb msg failed")
	assert.NotNil(t, s.GetFragment(1, false), "check handle hb msg failed")

	// check stale
	msg.Frag.Version = 1
	rsp = s.HandleShardingMsg(msg)
	assert.NotNil(t, rsp, "handle hb msg failed")
	_, ok := rsp.(*meta.RemoveMsg)
	assert.True(t, ok, "check handle stale hb msg failed")

	// check update
	msg.Frag.Version = 3
	rsp = s.HandleShardingMsg(msg)
	assert.Equal(t, uint64(3), st.frags[10001].Version, "check handle update hb msg failed")
	_, ok = rsp.(*meta.HBACKMsg)
	assert.True(t, ok, "check handle update hb msg failed")
}

func TestHandleShardingMsgWithHBACKMsg(t *testing.T) {
	st := newTestStorage()
	trans := newTestShardingTransport()
	tc := newTestTC()

	cfg := Cfg{}
	cfg.shardingTrans = trans
	cfg.storeID = 10001
	cfg.tc = tc
	cfg.storage = st
	s := NewStore(cfg)

	m := &meta.HBMsg{}
	m.Frag.Version = 2
	m.Frag.ID = 1
	m.Frag.Peers = append(m.Frag.Peers, prophet.Peer{
		ID:          1,
		ContainerID: 10001,
	})
	s.HandleShardingMsg(m)

	msg := &meta.HBACKMsg{}
	msg.ID = 2
	msg.Version = 1
	msg.Peer.ContainerID = 10002
	msg.Peer.ID = 1

	rsp := s.HandleShardingMsg(msg)
	assert.Nil(t, rsp, "handle hb ack msg failed")
	assert.Nil(t, trans.m[10002], "check handle hb ack msg failed")

	// check stale case 1
	tc.leader = true
	msg.ID = 1
	msg.Version = 1
	rsp = s.HandleShardingMsg(msg)
	assert.Nil(t, rsp, "handle hb stale ack msg failed")
	assert.NotNil(t, trans.m[10002], "check stale handle hb ack msg failed")
	delete(trans.m, uint64(10002))

	// check normal
	tc.leader = true
	msg.ID = 1
	msg.Version = 2
	rsp = s.HandleShardingMsg(msg)
	assert.Nil(t, rsp, "handle hb ack msg failed")

	// check stae case 2
	tc.leader = true
	msg.ID = 1
	msg.Version = 3
	rsp = s.HandleShardingMsg(msg)
	assert.Nil(t, rsp, "check stale handle hb ack msg failed")
	assert.Nil(t, trans.m[10002], "check stale handle hb ack msg failed")
	assert.Nil(t, s.GetFragment(1, false), "check stale handle hb ack msg failed")
}

func TestHandleShardingMsgWithRemovePR(t *testing.T) {
	cfg := Cfg{}
	cfg.storeID = 10001
	cfg.tc = newTestTC()
	cfg.storage = newTestStorage()
	s := NewStore(cfg)

	m := &meta.HBMsg{}
	m.Frag.Version = 2
	m.Frag.ID = 1
	m.Frag.Peers = append(m.Frag.Peers, prophet.Peer{
		ID:          1,
		ContainerID: 10001,
	})
	s.HandleShardingMsg(m)

	msg := &meta.RemoveMsg{}
	msg.ID = 2
	rsp := s.HandleShardingMsg(msg)
	assert.Nil(t, rsp, "handle remove msg failed")
	assert.NotNil(t, s.GetFragment(1, false), "handle remove msg failed")

	msg.ID = 1
	rsp = s.HandleShardingMsg(msg)
	assert.Nil(t, rsp, "handle remove msg failed")
	assert.Nil(t, s.GetFragment(1, false), "handle remove msg failed")
}
