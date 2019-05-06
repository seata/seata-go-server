package core

import (
	"time"

	"github.com/infinivision/taas/pkg/meta"
	"github.com/infinivision/taas/pkg/util"
)

func (tc *cellTransactionCoordinator) calcGTimeout(g *meta.GlobalTransaction) error {
	timeout, err := util.DefaultTW.Schedule(time.Duration(g.Timeout), tc.onGTimeout, g.ID)
	if err != nil {
		return err
	}

	tc.timeouts[g.ID] = timeout
	return nil
}

func (tc *cellTransactionCoordinator) onGTimeout(arg interface{}) {
	gid := arg.(uint64)
	c := acquireCMD()
	c.cmdType = cmdGTimeout
	c.gid = gid

	err := tc.cmds.Put(c)
	if err != nil {
		util.DefaultTW.Schedule(time.Second, tc.onGTimeout, gid)
	}
}

func (tc *cellTransactionCoordinator) removeGTimeout(gid uint64) {
	if timeout, ok := tc.timeouts[gid]; ok {
		timeout.Stop()
		delete(tc.timeouts, gid)
	}
}

func (tc *cellTransactionCoordinator) onBNotifyTimeout(arg interface{}) {
	c := acquireCMD()
	c.cmdType = cmdBNotifyTimeout
	c.nt = arg.(meta.Notify)

	tc.cmds.Put(c)
}

func (tc *cellTransactionCoordinator) calcBNotifyTimeout(nt meta.Notify) {
	c := acquireCMD()
	c.cmdType = cmdCalcBNotifyTimeout
	c.nt = nt

	tc.cmds.Put(c)
}

func (tc *cellTransactionCoordinator) removeBNotifyTimeout(ack meta.NotifyACK) {
	if timeout, ok := tc.notifyTimeouts[ack.ID()]; ok {
		timeout.Stop()
		delete(tc.notifyTimeouts, ack.ID())
	}
}
