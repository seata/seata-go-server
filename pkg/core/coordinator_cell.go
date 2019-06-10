package core

import (
	"context"
	"fmt"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/task"
	"seata.io/server/pkg/election"
	"seata.io/server/pkg/meta"
	"seata.io/server/pkg/transport"
)

const (
	batch = 1000
)

type cellTransactionCoordinator struct {
	opts options

	id, peerID, leaderPeerID uint64

	idLabel string
	trans   transport.Transport

	leader  bool
	elector election.Elector
	cancel  context.CancelFunc

	gids           map[uint64]*meta.GlobalTransaction
	timeouts       map[uint64]goetty.Timeout
	notifyTimeouts map[string]goetty.Timeout

	eventListeners  []eventListener
	metricsListener *metricsListener

	gidKey    string
	manualKey string

	// about event loop
	cmds *task.RingBuffer
}

// NewCellTransactionCoordinator create a transaction coordinator used Elasticell with meta storage
func NewCellTransactionCoordinator(id, peerID uint64, trans transport.Transport, opts ...Option) (TransactionCoordinator, error) {
	tc := &cellTransactionCoordinator{}

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
	tc.manualKey = manualCellKey(id)
	tc.cmds = task.NewRingBuffer(uint64(tc.opts.concurrency) * 4)
	tc.trans = trans
	ctx, cancel := context.WithCancel(context.Background())
	tc.cancel = cancel
	go tc.elector.ElectionLoop(ctx, id, peerID, func(data uint64) bool {
		return peerID == data
	}, tc.becomeLeader, tc.becomeFollower)

	tc.metricsListener = &metricsListener{tc: tc}
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

func (tc *cellTransactionCoordinator) HandleManual() {
	log.Debugf("[frag-%d]: manual start",
		tc.id)

	cnt, err := tc.opts.storage.Manual(tc.id, func(manual *meta.Manual) error {
		log.Infof("%s: schedule to %s",
			meta.TagGlobalTransaction(manual.GID, "manual"),
			manual.Action.Name())

		c := acquireCMD()
		c.gid = manual.GID

		switch manual.Action {
		case meta.CommitAction:
			c.cmdType = cmdCommitG
		case meta.RollbackAction:
			c.cmdType = cmdRollbackG
		default:
			log.Fatalf("bug: not support manual action %s",
				manual.Action.Name())
		}

		completeC := make(chan error, 1)
		c.statusCB = func(status meta.GlobalStatus, err error) {
			if err != nil {
				completeC <- err
				return
			}

			log.Infof("%s: schedule to %s with %s",
				meta.TagGlobalTransaction(manual.GID, "manual"),
				manual.Action.Name(),
				status.Name())
			completeC <- nil
		}
		tc.cmds.Put(c)

		err := <-completeC
		if err != nil {
			log.Warnf("%s: schedule to %s failed with %+v",
				meta.TagGlobalTransaction(manual.GID, "manual"),
				manual.Action.Name(),
				err)
		}

		return err
	})
	if err != nil {
		log.Errorf("[frag-%d]: handle manual failed with %+v",
			tc.id,
			err)
		return
	}

	if cnt == 0 {
		log.Debugf("[frag-%d]: no manual schedule", tc.id)
		return
	}
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
	if err := tc.opts.storage.Put(tc.id, g); err != nil {
		log.Fatalf("%s: failed with %+v",
			meta.TagGlobalTransaction(g.ID, "save"),
			err)
	}
}

func (tc *cellTransactionCoordinator) removeGlobalTransaction(g *meta.GlobalTransaction) {
	if err := tc.opts.storage.Remove(tc.id, g); err != nil {
		log.Fatalf("%s: failed with %+v",
			meta.TagGlobalTransaction(g.ID, "remove"),
			err)
	}

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
