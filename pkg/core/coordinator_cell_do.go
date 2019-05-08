package core

import (
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/meta"
)

func (tc *cellTransactionCoordinator) doRegistryGlobalTransaction(g *meta.GlobalTransaction) error {
	tc.doAddG(g)

	if g.Action != meta.NoneAction {
		tc.doComplete(g.ID)
	} else {
		err := tc.calcGTimeout(g)
		if err != nil {
			return err
		}
	}

	log.Infof("%s: timeout %s,added by %s/%s",
		meta.TagGlobalTransaction(g.ID, "reg_g"),
		time.Duration(g.Timeout),
		g.Creator,
		g.Name)
	return nil
}

func (tc *cellTransactionCoordinator) doComplete(gid uint64) {
	g, ok := tc.doGetG(gid)
	if !ok {
		log.Fatalf("%s: not found",
			meta.TagGlobalTransaction(gid, "do_complete"))
	}

	switch g.Action {
	case meta.NoneAction:
		log.Fatalf("%s: none action",
			meta.TagGlobalTransaction(gid, "do_complete"))
	case meta.CommitAction:
		tc.doCommit(g)
		break
	case meta.RollbackAction:
		tc.doRollback(g)
		break
	}
}

func (tc *cellTransactionCoordinator) doGTimeout(gid uint64) {
	g, ok := tc.doGetG(gid)
	if !ok {
		log.Warnf("%s: not found, ignore",
			meta.TagGlobalTransaction(gid, "do_g_timeout"))
		return
	}

	// Commit the global transaction if all branch transaction was succeed in phase one
	if tc.opts.commitIfAllBranchSucceedInPhaseOne && tc.checkPhaseOneCommitted(g) {
		g.Status = meta.GlobalStatusRetryCommitting
		g.Action = meta.CommitAction
		tc.mustSave(g)
		log.Infof("%s: commit by all branches was succeed",
			meta.TagGlobalTransaction(gid, "do_g_timeout"))

		tc.doCommit(g)
		return
	}

	oldStatus := g.Status
	switch g.Status {
	case meta.GlobalStatusBegin:
		g.Status = meta.GlobalStatusRollbackingSinceTimeout
		break
	case meta.GlobalStatusCommitting:
		g.Status = meta.GlobalStatusRollbackingSinceTimeout
		break
	case meta.GlobalStatusRetryCommitting:
		g.Status = meta.GlobalStatusRollbackingSinceTimeout
		break
	case meta.GlobalStatusRollbacking:
		g.Status = meta.GlobalStatusRetryRollbackingSinceTimeout
		break
	case meta.GlobalStatusRetryRollbacking:
		g.Status = meta.GlobalStatusRetryRollbackingSinceTimeout
		break
	case meta.GlobalStatusRollbackingSinceTimeout:
		g.Status = meta.GlobalStatusRetryRollbackingSinceTimeout
		break
	case meta.GlobalStatusRetryRollbackingSinceTimeout:
		g.Status = meta.GlobalStatusRetryRollbackingSinceTimeout
		break
	default:
		log.Fatalf("%s: invalid status %s",
			meta.TagGlobalTransaction(gid, "do_g_timeout"),
			g.Status.Name())
	}

	g.Action = meta.RollbackAction
	tc.removeGTimeout(gid)

	log.Infof("%s: from %s to %s",
		meta.TagGlobalTransaction(gid, "do_g_timeout"),
		oldStatus.Name(),
		g.Status.Name())

	tc.doComplete(gid)
}

func (tc *cellTransactionCoordinator) doCommit(g *meta.GlobalTransaction) {
	if len(g.Branches) == 0 {
		log.Fatalf("%s: has no branch registry",
			meta.TagGlobalTransaction(g.ID, "do_commit"))
	}

	auto := g.Auto()
	for _, b := range g.Branches {
		if !b.Complete() {
			if b.NotifyAt == 0 {
				b.NotifyAtTime = time.Now()
				b.NotifyAt = millisecond(b.NotifyAtTime)
			}

			nt := meta.Notify{
				XID:        meta.NewFragmentXID(b.GID, tc.id),
				BID:        b.ID,
				Resource:   b.Resource,
				Action:     meta.CommitAction,
				BranchType: b.BranchType,
			}

			tc.doSendNotify(nt)
			if !auto {
				break
			}
		}
	}
}

func (tc *cellTransactionCoordinator) doRollback(g *meta.GlobalTransaction) {
	if len(g.Branches) == 0 {
		g.Status = meta.GlobalStatusRollbacked
		log.Warnf("%s: no branches",
			meta.TagGlobalTransaction(g.ID, "do_rollback"))
	}

	for i := len(g.Branches) - 1; i >= 0; i-- {
		b := g.Branches[i]
		if !b.Complete() {
			nt := meta.Notify{
				XID:        meta.NewFragmentXID(b.GID, tc.id),
				BID:        b.ID,
				Resource:   b.Resource,
				Action:     meta.RollbackAction,
				BranchType: b.BranchType,
			}
			tc.doSendNotify(nt)
			break
		}
	}

	// this case is the start TM crash between registry a global transaction and the first branch transaction
	// remove direct
	if len(g.Branches) == 0 {
		tc.removeGlobalTransaction(g)
	}
}

func (tc *cellTransactionCoordinator) doSendNotify(nt meta.Notify) {
	tc.trans.AsyncSend(nt.Resource, nt, tc.calcBNotifyTimeout)
}

func (tc *cellTransactionCoordinator) doGetGCount() int {
	return len(tc.gids)
}

func (tc *cellTransactionCoordinator) doGetG(id uint64) (*meta.GlobalTransaction, bool) {
	g, ok := tc.gids[id]
	return g, ok
}

func (tc *cellTransactionCoordinator) doAddG(g *meta.GlobalTransaction) {
	tc.gids[g.ID] = g
}

func (tc *cellTransactionCoordinator) doRemoveG(gid uint64) {
	delete(tc.gids, gid)
}
