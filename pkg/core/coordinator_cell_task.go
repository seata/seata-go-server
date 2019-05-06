package core

import (
	"context"
	"time"

	"github.com/fagongzi/log"
	"github.com/fagongzi/util/json"
	"github.com/garyburd/redigo/redis"
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
	conn := tc.cell.Get()
	defer conn.Close()

	for {
		value, err := conn.Do("LRANGE", tc.manualKey, 0, 0)
		if err != nil {
			log.Errorf("[frag-%d]: read manual failed with %+v",
				tc.id,
				err)
			return
		}

		data, _ := redis.ByteSlices(value, err)
		if len(data) == 0 {
			log.Debugf("[frag-%d]: no manual schedule", tc.id)
			return
		}

		manual := &meta.Manual{}
		json.MustUnmarshal(manual, data[0])
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

		c.statusCB = func(status meta.GlobalStatus, err error) {
			if err != nil {
				log.Warnf("%s: schedule to %s failed with %+v",
					meta.TagGlobalTransaction(manual.GID, "manual"),
					manual.Action.Name(),
					err)
				return
			}

			log.Infof("%s: schedule to %s with %s",
				meta.TagGlobalTransaction(manual.GID, "manual"),
				manual.Action.Name(),
				status.Name())

			conn := tc.cell.Get()
			defer conn.Close()

			// we don't care err
			conn.Do("LPOP", tc.manualKey)
		}
		tc.cmds.Put(c)
	}
}
