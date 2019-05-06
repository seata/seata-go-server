package core

import (
	"context"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/json"
	"github.com/infinivision/taas/pkg/meta"
)

func (tc *cellTransactionCoordinator) becomeLeader() {
	c := acquireCMD()
	c.cmdType = cmdBecomeLeader

	err := tc.cmds.Put(c)
	if err != nil {
		log.Fatalf("[frag-%d]: add become leader event failed with %+v",
			tc.id,
			err)
	}
}

func (tc *cellTransactionCoordinator) becomeFollower() {
	c := acquireCMD()
	c.cmdType = cmdBecomeFollower

	err := tc.cmds.Put(c)
	if err != nil {
		log.Fatalf("[frag-%d]: add become follower event failed with %+v",
			tc.id,
			err)
	}
}

func (tc *cellTransactionCoordinator) reset() {
	tc.gids = make(map[uint64]*meta.GlobalTransaction)
	tc.timeouts = make(map[uint64]goetty.Timeout)
	tc.notifyTimeouts = make(map[string]goetty.Timeout)
	tc.tasks = make([]uint64, 0, 0)
	tc.wg = &sync.WaitGroup{}

	tc.initEvent()

	log.Infof("[frag-%d]: reset",
		tc.id)
}

func (tc *cellTransactionCoordinator) loadTasks() {
	tc.addTask(tc.runTasks)
}

func (tc *cellTransactionCoordinator) addTask(taskFunc func(ctx context.Context)) {
	id, err := tc.runner.RunCancelableTask(taskFunc)
	if err != nil {
		log.Fatalf("run tasks failed, %+v",
			err)
	}
	tc.tasks = append(tc.tasks, id)
	tc.wg.Add(1)
}

func (tc *cellTransactionCoordinator) cancelTimeouts() {
	for _, timeout := range tc.timeouts {
		timeout.Stop()
	}

	for _, timeout := range tc.notifyTimeouts {
		timeout.Stop()
	}

	log.Infof("[frag-%d]: all transaction timeout cancelled",
		tc.id)
}

func (tc *cellTransactionCoordinator) cancelTasks() {
	for _, id := range tc.tasks {
		tc.runner.StopCancelableTask(id)
	}

	// wait all task complete
	if len(tc.tasks) > 0 {
		tc.wg.Wait()
	}
}

func (tc *cellTransactionCoordinator) loadTransactions() error {
	err := tc.batchLoad(tc.gidKey, func(value []byte) error {
		g := &meta.GlobalTransaction{}
		json.MustUnmarshal(g, value)

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
