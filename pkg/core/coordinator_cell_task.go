package core

import (
	"context"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/meta"
)

func (tc *cellTransactionCoordinator) runTasks(ctx context.Context) {
	defer tc.wg.Done()

	manualTicker := time.NewTicker(time.Second * 10)
	defer manualTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("[frag-%d]: task exit",
				tc.id)
			return
		case <-manualTicker.C:
			tc.doManual()
			break
		}
	}
}

func (tc *cellTransactionCoordinator) doManual() {
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
