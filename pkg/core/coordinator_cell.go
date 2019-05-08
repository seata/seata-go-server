package core

import (
	"context"
	"fmt"
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/json"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/taas/pkg/cedis"
	"github.com/infinivision/taas/pkg/election"
	"github.com/infinivision/taas/pkg/meta"
	"github.com/infinivision/taas/pkg/transport"
)

const (
	batch = 1000
)

type cellTransactionCoordinator struct {
	opts options

	id, peerID, leaderPeerID uint64

	idLabel string
	cell    *cedis.Cedis
	trans   transport.Transport

	leader  bool
	elector election.Elector
	cancel  context.CancelFunc

	gids           map[uint64]*meta.GlobalTransaction
	timeouts       map[uint64]goetty.Timeout
	notifyTimeouts map[string]goetty.Timeout

	eventListeners  []eventListener
	metricsListener *metricsListener

	wg     *sync.WaitGroup
	runner *task.Runner
	tasks  []uint64

	gidKey    string
	manualKey string

	// about event loop
	cmds *task.RingBuffer
}

// NewCellTransactionCoordinator create a transaction coordinator used Elasticell with meta storage
func NewCellTransactionCoordinator(id, peerID uint64, trans transport.Transport, opts ...Option) (TransactionCoordinator, error) {
	tc := &cellTransactionCoordinator{
		runner: task.NewRunner(),
	}

	for _, opt := range opts {
		opt(&tc.opts)
	}
	tc.opts.adjust()

	if tc.opts.elector != nil {
		tc.elector = tc.opts.elector
	} else {
		elector, err := election.NewElector(tc.opts.electorOptions...)
		if err != nil {
			log.Fatalf("[frag-%d]: init elector failed with %+v",
				tc.id,
				err)
		}
		tc.elector = elector
	}

	tc.id = id
	tc.idLabel = fmt.Sprintf("%d", id)
	tc.peerID = peerID
	tc.gidKey = gCellKey(id)
	tc.manualKey = manualCellKey(id)
	tc.cmds = task.NewRingBuffer(uint64(tc.opts.concurrency) * 64)
	tc.trans = trans
	tc.cell = tc.opts.cell
	ctx, cancel := context.WithCancel(context.Background())
	tc.cancel = cancel
	go tc.elector.ElectionLoop(ctx, id, peerID, func(data uint64) bool {
		return peerID == data
	}, tc.becomeLeader, tc.becomeFollower)

	tc.metricsListener = &metricsListener{tc: tc}
	go tc.startEventLoop(ctx)
	return tc, nil
}

func (tc *cellTransactionCoordinator) Stop() {
	tc.cmds.Dispose()
	tc.cancel()
	tc.elector.Stop(tc.id)
	log.Infof("[frag-%d]: stopped", tc.id)
}

func (tc *cellTransactionCoordinator) IsLeader() bool {
	return tc.leader
}

func (tc *cellTransactionCoordinator) ChangeLeaderTo(id uint64) {
	if id == tc.peerID {
		return
	}

	c := acquireCMD()
	c.cmdType = cmdTransferLeader
	c.leader = id
	tc.cmds.Put(c)
}

func (tc *cellTransactionCoordinator) CurrentLeader() (uint64, error) {
	if tc.leader {
		return tc.peerID, nil
	}

	if tc.leaderPeerID != 0 {
		return tc.leaderPeerID, nil
	}

	return tc.elector.CurrentLeader(tc.id)
}

func (tc *cellTransactionCoordinator) ActiveGCount() int {
	return tc.doGetGCount()
}

// RegistryGlobalTransaction registry a global transaction
func (tc *cellTransactionCoordinator) RegistryGlobalTransaction(value meta.CreateGlobalTransaction, cb func(uint64, error)) {
	if !tc.leader {
		cb(0, meta.ErrNotLeader)
		return
	}

	c := acquireCMD()
	c.cmdType = cmdRegistryG
	c.createG = value
	c.idCB = cb

	err := tc.cmds.Put(c)
	if err != nil {
		cb(0, err)
		releaseCMD(c)
	}
}

// RegistryBranchTransaction registry a branch transaction
func (tc *cellTransactionCoordinator) RegistryBranchTransaction(value meta.CreateBranchTransaction, cb func(uint64, error)) {
	if !tc.leader {
		cb(0, meta.ErrNotLeader)
		return
	}

	c := acquireCMD()
	c.cmdType = cmdRegistryB
	c.createB = value
	c.idCB = cb

	err := tc.cmds.Put(c)
	if err != nil {
		cb(0, err)
		releaseCMD(c)
	}
}

func (tc *cellTransactionCoordinator) ReportBranchTransactionStatus(value meta.ReportBranchStatus, cb func(error)) {
	if !tc.leader {
		cb(meta.ErrNotLeader)
		return
	}

	c := acquireCMD()
	c.cmdType = cmdReportB
	c.reportB = value
	c.errorCB = cb

	err := tc.cmds.Put(c)
	if err != nil {
		cb(err)
		releaseCMD(c)
	}
}

func (tc *cellTransactionCoordinator) GlobalTransactionStatus(gid uint64, cb func(meta.GlobalStatus, error)) {
	if !tc.leader {
		cb(meta.GlobalStatusUnKnown, meta.ErrNotLeader)
		return
	}

	c := acquireCMD()
	c.cmdType = cmdStatusG
	c.gid = gid
	c.statusCB = cb

	err := tc.cmds.Put(c)
	if err != nil {
		cb(meta.GlobalStatusUnKnown, err)
		releaseCMD(c)
	}
}

func (tc *cellTransactionCoordinator) CommitGlobalTransaction(gid uint64, who string, cb func(meta.GlobalStatus, error)) {
	if !tc.leader {
		cb(meta.GlobalStatusUnKnown, meta.ErrNotLeader)
		return
	}

	c := acquireCMD()
	c.cmdType = cmdCommitG
	c.gid = gid
	c.who = who
	c.statusCB = cb

	err := tc.cmds.Put(c)
	if err != nil {
		cb(meta.GlobalStatusUnKnown, err)
		releaseCMD(c)
	}
}

func (tc *cellTransactionCoordinator) RollbackGlobalTransaction(gid uint64, who string, cb func(meta.GlobalStatus, error)) {
	if !tc.leader {
		cb(meta.GlobalStatusUnKnown, meta.ErrNotLeader)
		return
	}

	c := acquireCMD()
	c.cmdType = cmdRollbackG
	c.gid = gid
	c.who = who
	c.statusCB = cb

	err := tc.cmds.Put(c)
	if err != nil {
		cb(meta.GlobalStatusUnKnown, err)
		releaseCMD(c)
	}
}

func (tc *cellTransactionCoordinator) BranchTransactionNotifyACK(ack meta.NotifyACK) {
	if !tc.leader {
		return
	}

	c := acquireCMD()
	c.cmdType = cmdACKB
	c.ack = ack

	tc.cmds.Put(c)
}

func (tc *cellTransactionCoordinator) Lockable(resource string, gid uint64, lockKeys []meta.LockKey, cb func(bool, error)) {
	if !tc.leader {
		cb(false, meta.ErrNotLeader)
		return
	}

	c := acquireCMD()
	c.cmdType = cmdLockable
	c.resource = resource
	c.gid = gid
	c.lockKeys = lockKeys
	c.boolCB = cb

	err := tc.cmds.Put(c)
	if err != nil {
		cb(false, err)
		releaseCMD(c)
	}
}

func (tc *cellTransactionCoordinator) checkPhaseOneCommitted(g *meta.GlobalTransaction) bool {
	completed := true
	for _, b := range g.Branches {
		if !b.PhaseOneCommitted() {
			completed = false
			break
		}

	}

	return completed
}

func (tc *cellTransactionCoordinator) mustDo(doFunc func() error) error {
	times := 0

	for {
		err := doFunc()
		if err == nil {
			break
		}

		if times >= tc.opts.retries {
			return err
		}
		times++
	}

	return nil
}

func (tc *cellTransactionCoordinator) mustSave(g *meta.GlobalTransaction) {
	conn := tc.cell.Get()
	err := tc.mustDo(func() error {
		_, err := conn.Do("HSET", tc.gidKey, g.ID, json.MustMarshal(g))
		return err
	})

	if err != nil {
		log.Fatalf("%s: failed with %+v",
			meta.TagGlobalTransaction(g.ID, "save"),
			err)
	}
	conn.Close()
}

func (tc *cellTransactionCoordinator) removeGlobalTransaction(g *meta.GlobalTransaction) {
	conn := tc.cell.Get()
	err := tc.mustDo(func() error {
		_, err := conn.Do("HDEL", tc.gidKey, g.ID)
		return err
	})

	if err != nil {
		log.Fatalf("%s: failed with %+v",
			meta.TagGlobalTransaction(g.ID, "remove"),
			err)
	}
	conn.Close()

	tc.doRemoveG(g.ID)
	log.Infof("%s: with status %s",
		meta.TagGlobalTransaction(g.ID, "remove"),
		g.Status.Name())

	tc.publishEvent(event{eventType: completeG, data: g})
}

func (tc *cellTransactionCoordinator) checkGlobalCompleted(g *meta.GlobalTransaction) bool {
	completed := true
	for _, b := range g.Branches {
		if !b.Complete() {
			completed = false
			break
		}

		if !g.Complete() {
			switch b.Status {
			case meta.BranchStatusPhaseTwoCommitted:
				g.Status = meta.GlobalStatusCommitted
			case meta.BranchStatusPhaseTwoRollbacked:
				if g.TimeoutStatus() {
					g.Status = meta.GlobalStatusRollbackedSinceTimeout
				} else {
					g.Status = meta.GlobalStatusRollbacked
				}
			case meta.BranchStatusPhaseTwoCommitFailedUnretriable:
				g.Status = meta.GlobalStatusCommitFailed
			case meta.BranchStatusPhaseTwoRollbackFailedUnretriable:
				if g.TimeoutStatus() {
					g.Status = meta.GlobalStatusRollbackFailedSinceTimeout
				} else {
					g.Status = meta.GlobalStatusRollbackFailed
				}
			}
		}
	}

	return completed
}
