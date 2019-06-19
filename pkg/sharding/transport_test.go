package sharding

import (
	"testing"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/stretchr/testify/assert"
	"seata.io/server/pkg/meta"
)

func TestStart(t *testing.T) {
	s := newTestStore()
	s.meta.Addr = "127.0.0.1:12345"
	trans := newShardingTransport(s)
	trans.Start()

	conn := goetty.NewConnector(s.meta.Addr,
		goetty.WithClientDecoder(meta.ShardingDecoder),
		goetty.WithClientEncoder(meta.ShardingEncoder))
	_, err := conn.Connect()
	assert.Nilf(t, err, "check sharding transport start failed with: %+v", err)
	trans.Stop()
}

func TestSend(t *testing.T) {
	s1 := newTestStore()
	s1.meta.ID = 10001
	s1.meta.Addr = "127.0.0.1:12345"
	s1.addrs[10001] = "127.0.0.1:12345"
	s1.addrs[10002] = "127.0.0.1:12346"
	trans1 := newShardingTransport(s1)
	trans1.Start()

	s2 := newTestStore()
	s2.meta.ID = 10001
	s2.meta.Addr = "127.0.0.1:12346"
	s2.addrs[10001] = "127.0.0.1:12345"
	s2.addrs[10002] = "127.0.0.1:12346"
	s2.rspMsg = &meta.HBMsg{}
	trans2 := newShardingTransport(s2)
	trans2.Start()

	trans1.Send(10002, &meta.RemoveMsg{ID: 1})

	time.Sleep(time.Millisecond * 10)
	assert.NotNil(t, s1.lastMsg, "check sharding transport failed")
	assert.NotNil(t, s2.lastMsg, "check sharding transport failed")
}
