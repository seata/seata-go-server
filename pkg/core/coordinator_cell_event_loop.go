package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/meta"
	"github.com/infinivision/taas/pkg/util"
)

const (
	cmdUnknown = iota
	cmdBecomeLeader
	cmdBecomeFollower
	cmdRegistryG
	cmdRegistryB
	cmdReportB
	cmdStatusG
	cmdCommitG
	cmdRollbackG
	cmdACKB
	cmdLockable
	cmdGTimeout
	cmdGComplete
	cmdCalcBNotifyTimeout
	cmdBNotifyTimeout
	cmdTransferLeader
)

var (
	cmdPool sync.Pool

	emptyRM      meta.ResourceManager
	emptyRMS     meta.ResourceManagerSet
	emptyCreateG meta.CreateGlobalTransaction
	emptyCreateB meta.CreateBranchTransaction
	emptyReportB meta.ReportBranchStatus
	emptyACK     meta.NotifyACK
	emptyNotify  meta.Notify
)

func acquireCMD() *cmd {
	value := cmdPool.Get()
	if value == nil {
		return &cmd{}
	}

	return value.(*cmd)
}

func releaseCMD(value *cmd) {
	value.reset()
	cmdPool.Put(value)
}

type cmd struct {
	cmdType int

	gid           uint64
	pid, sid, who string
	rm            meta.ResourceManager
	rms           meta.ResourceManagerSet
	createG       meta.CreateGlobalTransaction
	createB       meta.CreateBranchTransaction
	reportB       meta.ReportBranchStatus
	ack           meta.NotifyACK
	lockKeys      []meta.LockKey
	resource      string
	nt            meta.Notify
	leader        uint64

	errorCB  func(error)
	idCB     func(uint64, error)
	statusCB func(meta.GlobalStatus, error)
	boolCB   func(bool, error)
}

func (c *cmd) reset() {
	c.cmdType = cmdUnknown

	c.gid = 0
	c.pid = ""
	c.sid = ""
	c.rm = emptyRM
	c.rms = emptyRMS
	c.createG = emptyCreateG
	c.createB = emptyCreateB
	c.reportB = emptyReportB
	c.ack = emptyACK
	c.lockKeys = nil
	c.resource = ""
	c.leader = 0

	c.errorCB = nil
	c.idCB = nil
	c.statusCB = nil
	c.boolCB = nil
}

func (c *cmd) respError(err error) {
	if c.errorCB != nil {
		c.errorCB(err)
	}
}

func (c *cmd) respID(gid uint64, err error) {
	if c.idCB != nil {
		c.idCB(gid, err)
	}
}

func (c *cmd) respStatus(value meta.GlobalStatus, err error) {
	if c.statusCB != nil {
		c.statusCB(value, err)
	}
}

func (c *cmd) respBool(value bool, err error) {
	if c.boolCB != nil {
		c.boolCB(value, err)
	}
}

func (tc *cellTransactionCoordinator) startEventLoop(ctx context.Context) {
	log.Infof("[frag-%d]: start the event loop", tc.id)

	for {
		if tc.cmds.Len() == 0 && !tc.cmds.IsDisposed() {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		data, err := tc.cmds.Get()
		if err != nil {
			log.Infof("[frag-%d]: exit event loop", tc.id)
			return
		}

		c := data.(*cmd)
		switch c.cmdType {
		case cmdUnknown:
			break
		case cmdTransferLeader:
			tc.handleTransferLeader(c)
			break
		case cmdBecomeLeader:
			tc.handleBecomeLeader()
			break
		case cmdBecomeFollower:
			tc.handleBecomeFollower()
			break
		case cmdRegistryG:
			tc.handleRegistryGlobalTransaction(c)
			break
		case cmdRegistryB:
			tc.handleRegistryBranchTransaction(c)
			break
		case cmdReportB:
			tc.handleReportB(c)
			break
		case cmdStatusG:
			tc.handleGlobalTransactionStatus(c)
			break
		case cmdCommitG:
			tc.handleCommitGlobalTransaction(c)
			break
		case cmdRollbackG:
			tc.handleRollbackGlobalTransaction(c)
			break
		case cmdACKB:
			tc.handleBNotifyACK(c)
			break
		case cmdLockable:
			tc.handleLockable(c)
			break
		case cmdGTimeout:
			tc.handleGTimeout(c)
			break
		case cmdGComplete:
			tc.handleGComplete(c)
			break
		case cmdCalcBNotifyTimeout:
			tc.handleCalcBNotifyTimeout(c)
			break
		case cmdBNotifyTimeout:
			tc.handleBNotifyTimeout(c)
			break
		}

		releaseCMD(c)
	}
}

func (tc *cellTransactionCoordinator) handleTransferLeader(c *cmd) {
	log.Infof("[frag-%d]: my id is %d, transfer leader to peer %d",
		tc.id,
		tc.peerID,
		c.leader)

	if !tc.leader || c.leader == tc.peerID {
		return
	}

	err := tc.elector.ChangeLeaderTo(tc.id, tc.peerID, c.leader)
	if err != nil {
		log.Errorf("[frag-%d]: transfer leader to peer %d failed with %+v",
			tc.id,
			c.leader,
			err)
		return
	}

	tc.leaderPeerID = c.leader
	tc.handleBecomeFollower()
}

func (tc *cellTransactionCoordinator) handleBecomeLeader() {
	if tc.leader {
		return
	}

	log.Infof("[frag-%d]: ********become leader now********",
		tc.id)

	tc.leaderPeerID = tc.peerID
	tc.leader = true
	tc.reset()

	if err := tc.loadTransactions(); err != nil {
		log.Fatalf("load transactions failed, %+v", err)
	}
	log.Infof("[frag-%d]: %d global transactions loaded",
		tc.id,
		tc.doGetGCount())

	tc.loadTasks()
	log.Infof("[frag-%d]: all task loadded",
		tc.id)

	if tc.opts.becomeLeader != nil {
		tc.opts.becomeLeader()
	}
}

func (tc *cellTransactionCoordinator) handleBecomeFollower() {
	if !tc.leader {
		return
	}

	log.Infof("[frag-%d]: ********become follower now********",
		tc.id)

	tc.leader = false
	tc.cancelTimeouts()
	tc.cancelTasks()

	if tc.opts.becomeFollower != nil {
		tc.opts.becomeFollower()
	}
}

func (tc *cellTransactionCoordinator) handleRegistryGlobalTransaction(c *cmd) {
	if !tc.leader {
		tc.publishEvent(event{eventType: registerGFailed, data: c.createG})
		c.respID(0, meta.ErrNotLeader)
		return
	}

	id, err := tc.opts.gen.Gen()
	if err != nil {
		tc.publishEvent(event{eventType: registerGFailed, data: c.createG})
		c.respID(0, err)
		return
	}

	if c.createG.Timeout == 0 {
		c.createG.Timeout = tc.opts.transactionTimeout
	}

	now := time.Now()
	g := &meta.GlobalTransaction{
		ID:          id,
		Creator:     c.createG.Creator,
		Proxy:       c.createG.Proxy,
		Name:        c.createG.Name,
		Timeout:     int64(c.createG.Timeout),
		StartAtTime: now,
		StartAt:     millisecond(now),
		Status:      meta.GlobalStatusBegin,
		Action:      meta.NoneAction,
	}

	tc.mustSave(g)
	err = tc.doRegistryGlobalTransaction(g)
	if err != nil {
		tc.publishEvent(event{eventType: registerGFailed, data: c.createG})
		c.respID(0, err)
		return
	}

	tc.publishEvent(event{eventType: registerG, data: g})
	c.respID(id, nil)
}

func (tc *cellTransactionCoordinator) handleRegistryBranchTransaction(c *cmd) {
	if !tc.leader {
		tc.publishEvent(event{eventType: registerBFailed, data: c.createB})
		c.respID(0, meta.ErrNotLeader)
		return
	}

	g, ok := tc.doGetG(c.createB.GID)
	if !ok {
		log.Errorf("%s: not found, %s, [frag-%d]",
			meta.TagGlobalTransaction(c.createB.GID, "reg_branch"),
			c.createB.RMSID,
			tc.id)
		tc.publishEvent(event{eventType: registerBFailed, data: c.createB})
		c.respID(0, meta.ErrGlobalTransactionNotExist)
		return
	}

	if g.Status != meta.GlobalStatusBegin {
		log.Warnf("%s: invalid status %s, %s",
			meta.TagGlobalTransaction(c.createB.GID, "reg_branch"),
			g.Status.Name(),
			c.createB.RMSID)
		tc.publishEvent(event{eventType: registerBFailed, data: c.createB})
		c.respID(0, meta.ErrGlobalTransactionStatusInvalid)
		return
	}

	for _, b := range g.Branches {
		if b.RMSID == c.createB.RMSID {
			log.Warnf("%s: already added by %s",
				meta.TagGlobalTransaction(c.createB.GID, "reg_branch"),
				c.createB.RMSID)
			c.respID(b.ID, nil)
			return
		}
	}

	now := time.Now()
	b := &meta.BranchTransaction{
		ID:          uint64(len(g.Branches) + 1),
		RMSID:       c.createB.RMSID,
		Resource:    c.createB.ResourceID,
		GID:         c.createB.GID,
		LockKeys:    c.createB.LockKeys,
		StartAt:     millisecond(now),
		StartAtTime: now,
		Status:      meta.BranchStatusRegistered,
		BranchType:  c.createB.BranchType,
	}

	ok, conflict, err := tc.opts.lock.Lock(c.createB.ResourceID, c.createB.GID, b.LockKeys...)
	if err != nil {
		log.Fatalf("%s: lock failed with %+v, %s",
			meta.TagGlobalTransaction(c.createB.GID, "reg_branch"),
			err,
			c.createB.RMSID)
	}

	if !ok {
		log.Infof("%s: failed with conflict %s, %s",
			meta.TagGlobalTransaction(c.createB.GID, "reg_branch"),
			conflict,
			c.createB.RMSID)
		tc.publishEvent(event{eventType: registerBFailed, data: c.createB})
		c.respID(0, errors.New(conflict))
		return
	}

	g.Branches = append(g.Branches, b)
	tc.mustSave(g)

	log.Infof("%s: added with %d, %d BT, %s",
		meta.TagGlobalTransaction(c.createB.GID, "reg_branch"),
		b.ID,
		len(g.Branches),
		c.createB.RMSID)

	tc.publishEvent(event{eventType: registerB, data: b})
	c.respID(b.ID, nil)
}

func (tc *cellTransactionCoordinator) handleReportB(c *cmd) {
	switch c.reportB.Status {
	case meta.BranchStatusPhaseOneDone:
		break
	case meta.BranchStatusPhaseOneFailed:
		break
	case meta.BranchStatusPhaseOneTimeout:
		break
	default:
		log.Fatalf("%s: invalid status %s",
			meta.TagGlobalTransaction(c.reportB.GID, "report"),
			c.reportB.Status.Name())
	}

	if !tc.leader {
		tc.publishEvent(event{eventType: reportBFailed, data: c.reportB})
		c.respError(meta.ErrNotLeader)
		return
	}

	g, ok := tc.doGetG(c.reportB.GID)
	if !ok {
		log.Warnf("%s: not found, %s",
			meta.TagGlobalTransaction(c.reportB.GID, "report"),
			c.reportB.RMSID)
		tc.publishEvent(event{eventType: reportBFailed, data: c.reportB})
		c.respError(meta.ErrGlobalTransactionNotExist)
		return
	}

	var target *meta.BranchTransaction
	for _, b := range g.Branches {
		if b.ID == c.reportB.BID {
			if target != nil {
				log.Fatalf("%s: repeated of %s, repeated: %+v, %+v",
					meta.TagBranchTransaction(c.reportB.GID, c.reportB.BID, "report"),
					b.RMSID,
					target,
					b)
			}
			target = b
		}
	}

	if target == nil {
		log.Fatalf("%s: missing, %s",
			meta.TagBranchTransaction(c.reportB.GID, c.reportB.BID, "report"),
			c.reportB.RMSID)
	}

	now := time.Now()
	target.Status = c.reportB.Status
	target.ReportAt = millisecond(now)
	target.ReportAtTime = now
	tc.mustSave(g)

	log.Infof("%s: update to %s, %s",
		meta.TagBranchTransaction(c.reportB.GID, c.reportB.BID, "report"),
		c.reportB.Status.Name(),
		c.reportB.RMSID)

	tc.publishEvent(event{eventType: reportB, data: target})
	c.respError(nil)
}

func (tc *cellTransactionCoordinator) handleGlobalTransactionStatus(c *cmd) {
	if !tc.leader {
		c.respStatus(meta.GlobalStatusUnKnown, meta.ErrNotLeader)
		return
	}

	g, ok := tc.doGetG(c.gid)
	if !ok {
		c.respStatus(meta.GlobalStatusUnKnown, fmt.Errorf("global transcation not found"))
		return
	}

	c.respStatus(g.Status, nil)
}

func (tc *cellTransactionCoordinator) handleCommitGlobalTransaction(c *cmd) {
	if !tc.leader {
		tc.publishEvent(event{eventType: commitGFailed, data: c.gid})
		c.respStatus(meta.GlobalStatusUnKnown, meta.ErrNotLeader)
		return
	}

	g, ok := tc.doGetG(c.gid)
	if !ok {
		log.Warnf("%s: not found",
			meta.TagGlobalTransaction(c.gid, "commit"))
		tc.publishEvent(event{eventType: commitGFailed, data: c.gid})
		c.respStatus(meta.GlobalStatusUnKnown, meta.ErrGlobalTransactionNotExist)
		return
	}

	oldStatus := g.Status
	switch g.Status {
	case meta.GlobalStatusBegin:
		g.Status = meta.GlobalStatusCommitting
		break
	case meta.GlobalStatusCommitting:
		g.Status = meta.GlobalStatusRetryCommitting
		break
	case meta.GlobalStatusRetryCommitting:
		g.Status = meta.GlobalStatusRetryCommitting
		break
	default:
		log.Warnf("%s: invalid status %s",
			meta.TagGlobalTransaction(c.gid, "commit"),
			g.Status.Name())
		tc.publishEvent(event{eventType: commitGFailed, data: c.gid})
		c.respStatus(oldStatus, fmt.Errorf("invalid status %s", oldStatus.Name()))
		return
	}

	if g.Action != meta.NoneAction {
		log.Infof("%s: has action %s, ingore",
			meta.TagGlobalTransaction(c.gid, "commit"),
			g.Action.Name())
		tc.publishEvent(event{eventType: commitGFailed, data: c.gid})
		c.respStatus(g.Status, nil)
		return
	}

	g.Action = meta.CommitAction
	tc.removeGTimeout(c.gid)

	tc.mustSave(g)
	tc.publishEvent(event{eventType: commitG, data: g})
	c.respStatus(g.Status, nil)

	log.Infof("%s: from %s to %s, %s",
		meta.TagGlobalTransaction(c.gid, "commit"),
		oldStatus.Name(),
		g.Status.Name(),
		c.who)

	tc.doCommit(g)
}

func (tc *cellTransactionCoordinator) handleRollbackGlobalTransaction(c *cmd) {
	gid := c.gid
	who := c.who

	if !tc.leader {
		tc.publishEvent(event{eventType: rollbackGFailed, data: gid})
		c.respStatus(meta.GlobalStatusUnKnown, meta.ErrNotLeader)
		return
	}

	g, ok := tc.doGetG(gid)
	if !ok {
		log.Warnf("%s: not found",
			meta.TagGlobalTransaction(gid, "rollback"))
		tc.publishEvent(event{eventType: rollbackGFailed, data: gid})
		c.respStatus(meta.GlobalStatusUnKnown, meta.ErrGlobalTransactionNotExist)
		return
	}

	if g.Action == meta.CommitAction {
		log.Fatalf("%s: a committed transaction",
			meta.TagGlobalTransaction(gid, "rollback"))
		c.respStatus(g.Status, nil)
		return
	}

	if g.Action != meta.NoneAction {
		log.Warnf("%s: already has action %s, ingore",
			meta.TagGlobalTransaction(gid, "rollback"),
			g.Action.Name())
		tc.publishEvent(event{eventType: rollbackGFailed, data: gid})
		c.respStatus(g.Status, nil)
		return
	}

	oldStatus := g.Status
	switch g.Status {
	case meta.GlobalStatusBegin:
		g.Status = meta.GlobalStatusRollbacking
		break
	case meta.GlobalStatusCommitting:
		g.Status = meta.GlobalStatusRollbacking
		break
	case meta.GlobalStatusRetryCommitting:
		g.Status = meta.GlobalStatusRollbacking
		break
	case meta.GlobalStatusRollbacking:
		g.Status = meta.GlobalStatusRetryRollbacking
		break
	case meta.GlobalStatusRetryRollbacking:
		g.Status = meta.GlobalStatusRetryRollbacking
		break
	case meta.GlobalStatusRollbackingSinceTimeout:
		g.Status = meta.GlobalStatusRetryRollbackingSinceTimeout
		break
	case meta.GlobalStatusRetryRollbackingSinceTimeout:
		g.Status = meta.GlobalStatusRetryRollbackingSinceTimeout
		break
	default:
		log.Warnf("%s: invalid status %s",
			meta.TagGlobalTransaction(gid, "rollback"),
			g.Status.Name())
		tc.publishEvent(event{eventType: rollbackGFailed, data: gid})
		c.respStatus(oldStatus, fmt.Errorf("invalid status %s", oldStatus.Name()))
		return
	}

	if !g.Reported() {
		log.Warnf("%s: missing report, %+v",
			meta.TagGlobalTransaction(gid, "rollback"),
			g.MissingReportB())
	}

	g.Action = meta.RollbackAction
	tc.removeGTimeout(gid)

	tc.mustSave(g)
	tc.publishEvent(event{eventType: rollbackG, data: g})
	c.respStatus(g.Status, nil)

	log.Infof("%s: from %s to %s, %s",
		meta.TagGlobalTransaction(gid, "rollback"),
		oldStatus.Name(),
		g.Status.Name(),
		who)

	tc.doRollback(g)
}

func (tc *cellTransactionCoordinator) handleBNotifyACK(c *cmd) {
	ack := c.ack

	if !tc.leader {
		tc.publishEvent(event{eventType: ackBFailed, data: ack})
		return
	}

	g, ok := tc.doGetG(ack.GID)
	if !ok {
		log.Warnf("%s: not found, %s",
			meta.TagBranchTransaction(ack.GID, ack.BID, "ack"),
			ack.From)
		tc.publishEvent(event{eventType: ackBFailed, data: ack})
		return
	}

	var target *meta.BranchTransaction
	for _, b := range g.Branches {
		if b.ID == ack.BID {
			target = b
		}
	}

	if target == nil {
		log.Fatalf("%s: not found, %s",
			meta.TagBranchTransaction(ack.GID, ack.BID, "ack"),
			ack.From)
	}

	if target.Complete() {
		log.Warnf("%s: already complete with status %s, %s",
			meta.TagBranchTransaction(ack.GID, ack.BID, "ack"),
			target.Status.Name(),
			ack.From)
		return
	}

	if !ack.Succeed {
		log.Infof("%s: action failed, retry sent to another node, %s",
			meta.TagBranchTransaction(ack.GID, ack.BID, "ack"),
			ack.From)

		tc.doSendNotify(meta.Notify{
			XID:        meta.NewFragmentXID(g.ID, tc.id),
			BID:        ack.BID,
			Action:     g.Action,
			Resource:   target.Resource,
			BranchType: target.BranchType,
		})
		tc.publishEvent(event{eventType: ackBFailed, data: ack})
		return
	}

	log.Infof("%s: status %s, %s",
		meta.TagBranchTransaction(ack.GID, ack.BID, "ack"),
		ack.Status.Name(),
		ack.From)

	// remove from timeouts
	tc.removeBNotifyTimeout(ack)

	// check branch ack status, retry or complete global
	retry := false
	unlock := false
	switch ack.Status {
	case meta.BranchStatusPhaseTwoCommitted:
		unlock = true
		target.Status = ack.Status
	case meta.BranchStatusPhaseTwoCommitFailedRetriable:
		retry = true
		target.Status = ack.Status
	case meta.BranchStatusPhaseTwoCommitFailedUnretriable:
		target.Status = ack.Status
	case meta.BranchStatusPhaseTwoRollbacked:
		unlock = true
		target.Status = ack.Status
	case meta.BranchStatusPhaseTwoRollbackFailedRetriable:
		retry = true
		target.Status = ack.Status
	case meta.BranchStatusPhaseTwoRollbackFailedUnretriable:
		target.Status = ack.Status
	default:
		log.Fatalf("%s: invalid status %s",
			meta.TagBranchTransaction(ack.GID, ack.BID, "ack"),
			ack.Status.Name())
	}

	if unlock {
		err := tc.opts.lock.Unlock(target.Resource, target.LockKeys...)
		if err != nil {
			log.Fatalf("%s: unlock failed with %+v",
				meta.TagBranchTransaction(ack.GID, ack.BID, "ack"),
				err)
		}

		log.Infof("%s: unlock keys",
			meta.TagBranchTransaction(ack.GID, ack.BID, "ack"))
	}

	tc.mustSave(g)

	if retry {
		tc.doSendNotify(meta.Notify{
			XID:        meta.NewFragmentXID(g.ID, tc.id),
			BID:        ack.BID,
			Action:     g.Action,
			Resource:   target.Resource,
			BranchType: target.BranchType,
		})

		tc.publishEvent(event{eventType: ackBFailed, data: ack})
		return
	}

	tc.publishEvent(event{eventType: ackB, data: target})

	if tc.checkGlobalCompleted(g) {
		tc.removeGlobalTransaction(g)
		return
	}

	// trigger next branch in cases as below:
	// 1. commit global and has TCC branch
	// 2. rollback global
	if g.Action == meta.RollbackAction || !g.Auto() {
		tc.doComplete(g.ID)
	}
	return
}

func (tc *cellTransactionCoordinator) handleLockable(c *cmd) {
	if !tc.leader {
		c.respBool(false, meta.ErrNotLeader)
		return
	}

	if _, ok := tc.doGetG(c.gid); !ok {
		log.Fatalf("%s: not found",
			meta.TagGlobalTransaction(c.gid, "query_lock"))
	}

	ok, conflict, err := tc.opts.lock.Lockable(c.resource, c.gid, c.lockKeys...)
	if err != nil {
		log.Fatalf("%s: lockable failed with %+v",
			meta.TagGlobalTransaction(c.gid, "query_lock"),
			err)
	}

	if !ok {
		c.respBool(false, errors.New(conflict))
		return
	}

	c.respBool(true, nil)
}

func (tc *cellTransactionCoordinator) handleGTimeout(c *cmd) {
	gid := c.gid

	if !tc.leader {
		log.Warnf("%s: not leader, ignore",
			meta.TagGlobalTransaction(gid, "g_timeout"))
		return
	}

	if _, ok := tc.timeouts[gid]; !ok {
		log.Warnf("%s: not found, maybe cancelled",
			meta.TagGlobalTransaction(gid, "g_timeout"))
		return
	}

	tc.doGTimeout(gid)
}

func (tc *cellTransactionCoordinator) handleGComplete(c *cmd) {
	if !tc.leader {
		return
	}

	tc.doComplete(c.gid)
}

func (tc *cellTransactionCoordinator) handleCalcBNotifyTimeout(c *cmd) {
	if !tc.leader {
		return
	}

	if _, ok := tc.doGetG(c.nt.XID.GID); ok {
		timeout, err := util.DefaultTW.Schedule(tc.opts.ackTimeout, tc.onBNotifyTimeout, c.nt)
		if err != nil {
			log.Fatalf("add notify to timeout with error: %+v", err)
		}

		tc.notifyTimeouts[c.nt.ID()] = timeout
	}
}

func (tc *cellTransactionCoordinator) handleBNotifyTimeout(c *cmd) {
	if !tc.leader {
		return
	}

	if _, ok := tc.notifyTimeouts[c.nt.ID()]; ok {
		delete(tc.notifyTimeouts, c.nt.ID())
		tc.doSendNotify(c.nt)
	}
}
