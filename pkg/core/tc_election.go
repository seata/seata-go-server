package core

import (
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"seata.io/server/pkg/meta"
)

func (tc *defaultTC) becomeLeader() {
	c := acquireCMD()
	c.cmdType = cmdBecomeLeader

	err := tc.cmds.Put(c)
	if err != nil {
		log.Fatalf("[frag-%d]: add become leader event failed with %+v",
			tc.id,
			err)
	}
}

func (tc *defaultTC) becomeFollower() {
	completeC := make(chan struct{})

	c := acquireCMD()
	c.cmdType = cmdBecomeFollower
	c.completeCB = func() {
		completeC <- struct{}{}
	}

	err := tc.cmds.Put(c)
	if err != nil {
		log.Fatalf("[frag-%d]: add become follower event failed with %+v",
			tc.id,
			err)
	}

	select {
	case <-completeC:
		return
	case <-time.After(time.Minute):
		log.Fatalf("[frag-%d]: become follower event handle timeout",
			tc.id)
	}
}

func (tc *defaultTC) reset() {
	tc.gids = make(map[uint64]*meta.GlobalTransaction)
	tc.timeouts = make(map[uint64]goetty.Timeout)
	tc.notifyTimeouts = make(map[string]goetty.Timeout)

	tc.initEvent()

	log.Infof("[frag-%d]: reset",
		tc.id)
}

func (tc *defaultTC) cancelTimeouts() {
	for _, timeout := range tc.timeouts {
		timeout.Stop()
	}

	for _, timeout := range tc.notifyTimeouts {
		timeout.Stop()
	}

	log.Infof("[frag-%d]: all transaction timeout cancelled",
		tc.id)
}

func (tc *defaultTC) loadTransactions() error {
	q := meta.EmptyQuery
	q.Limit = batch
	err := tc.opts.storage.Load(tc.id, q, func(g *meta.GlobalTransaction) error {
		g.StartAtTime = time.Unix(0, g.StartAt*int64(time.Millisecond))
		for _, b := range g.Branches {
			b.StartAtTime = time.Unix(0, b.StartAt*int64(time.Millisecond))
			b.ReportAtTime = time.Unix(0, b.ReportAt*int64(time.Millisecond))
			b.NotifyAtTime = time.Unix(0, b.NotifyAt*int64(time.Millisecond))
		}

		return tc.doRegistryGlobalTransaction(g)
	})
	if err != nil {
		return err
	}

	return nil
}
