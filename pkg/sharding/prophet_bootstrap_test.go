package sharding

import (
	"testing"

	"github.com/fagongzi/goetty"
	"github.com/stretchr/testify/assert"
)

func TestDoBootstrapCluster(t *testing.T) {
	st := newTestStorage()
	cfg := Cfg{}
	cfg.InitFragments = 128
	cfg.tc = newTestTC()
	cfg.storage = st
	s := NewStore(cfg).(*store)

	st.set(storeKey, goetty.Uint64ToBytes(10002))
	s.doBootstrapCluster()
	assert.Equal(t, uint64(10002), s.meta.ID, "check do bootstrap failed")

	pd := newTestProphet()
	st = newTestStorage()
	s.storage = st
	s.pd = pd
	pd.bootSucc = false
	s.doBootstrapCluster()
	assert.Equal(t, 0, len(st.frags), "check do bootstrap failed")
	assert.Equal(t, uint64(1), s.meta.ID, "check do bootstrap failed")

	pd = newTestProphet()
	st = newTestStorage()
	s.storage = st
	s.pd = pd
	pd.bootSucc = true
	s.doBootstrapCluster()
	assert.Equal(t, 128, len(st.frags), "check do bootstrap failed")
	assert.Equal(t, uint64(1), s.meta.ID, "check do bootstrap failed")
}
