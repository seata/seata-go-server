package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/client"
	"github.com/infinivision/taas/pkg/meta"
)

type handler struct {
	c *client.Client
}

func (h *handler) OnBranchCommit(xid meta.FragmentXID, bid uint64) {
	h.c.BranchCommitACK(xid, bid, meta.BranchStatusPhaseTwoCommitted)
}

func (h *handler) OnBranchRollback(xid meta.FragmentXID, bid uint64) {
	h.c.BranchRollbackACK(xid, bid, meta.BranchStatusPhaseTwoRollbacked)
}

func main() {
	flag.Parse()
	log.InitLog()

	for i := 0; i < 10; i++ {
		go func(idx int) {
			res := fmt.Sprintf("res-%d", idx)
			name := fmt.Sprintf("test-%d", idx)

			h := &handler{}
			c := client.NewClient(client.Cfg{
				Addrs:             []string{"127.0.0.1:8091"},
				HeartbeatDuration: time.Second * 5,
				Timeout:           time.Second * 15,
				ApplicationID:     "test-bench",
				Version:           "0.5.0",
				Resources:         []string{res},
				Handler:           h,
				Seq:               uint64(idx+1) * 10000,
			})
			h.c = c

			for {
				var xids []meta.FragmentXID
				var bids []uint64

				b := c.CreateBatch()
				for j := 0; j < 8; j++ {
					b.CreateGlobal(name, time.Second*30)
				}

				rsps, err := c.CommitBatch(b)
				if err != nil {
					log.Fatalf("[%s] create global %+v", fmt.Sprintf("c-%d", idx), err)
					return
				}

				if len(rsps) == 0 {
					log.Fatalf("create global response 0")
				}

				b = c.CreateBatch()
				for _, msg := range rsps {
					rsp := msg.(*meta.GlobalBeginResponse)
					if !rsp.Succeed() {
						log.Fatalf("[%s] create global %+v", fmt.Sprintf("c-%d", idx), rsp.Msg)
					}
					xids = append(xids, rsp.XID)
					b.RegisterBranch(rsp.XID, res, meta.AT, "", "")
				}

				rsps, err = c.CommitBatch(b)
				if err != nil {
					log.Fatalf("[%s] register branch %+v", fmt.Sprintf("c-%d", idx), err)
					return
				}

				if len(rsps) == 0 {
					log.Fatalf("register branch  response 0")
				}

				b = c.CreateBatch()
				for idx, msg := range rsps {
					rsp := msg.(*meta.BranchRegisterResponse)
					if !rsp.Succeed() {
						log.Fatalf("[%s] register branch %+v", fmt.Sprintf("c-%d", idx), rsp.Msg)
					}

					bids = append(bids, rsp.BranchID)
					b.ReportBranchStatus(xids[idx], rsp.BranchID, meta.BranchStatusPhaseOneDone, meta.AT, res, "")
					b.CommitGlobal(xids[idx], "")
				}
				_, err = c.CommitBatch(b)
				if err != nil {
					log.Fatalf("[%s] report and commit  %+v", fmt.Sprintf("c-%d", idx), err)
					return
				}

				log.Infof("commit global complete")
			}
		}(i)
	}
	select {}
}
