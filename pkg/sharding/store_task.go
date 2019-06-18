package sharding

import (
	"context"
	"time"

	"github.com/fagongzi/log"
	"seata.io/server/pkg/meta"
)

func (s *store) runPRTask(ctx context.Context, id uint64) {
	log.Infof("pr event worker %d start", id)

	hasEvent := true
	for {
		select {
		case <-ctx.Done():
			log.Infof("pr event worker %d exit", id)
			return
		default:
			if !hasEvent {
				time.Sleep(time.Millisecond * 10)
			}

			hasEvent = false
			s.ForeachReplicate(func(pr *PeerReplicate) bool {
				if pr.workerID == id && pr.tc.HandleEvent() {
					hasEvent = true
				}

				return true
			})
		}
	}
}

func (s *store) runCheckConcurrencyTask(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("check concurrency task exit")
			return
		case <-ticker.C:
			s.doCheckConcurrency()
		}
	}
}

func (s *store) runHBTask(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.MaxPeerDownDuration / 5)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("hb task exit")
			return
		case <-ticker.C:
			s.doHB()
		}
	}
}

func (s *store) runGCRMTask(ctx context.Context) {
	gcRMticker := time.NewTicker(s.cfg.RMLease)
	defer gcRMticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("[GC-RM]: task exit")
			return
		case <-gcRMticker.C:
			s.doGCRM()
		}
	}
}

func (s *store) runManualTask(ctx context.Context) {
	manualTicker := time.NewTicker(time.Second * 10)
	defer manualTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infof("manual task exit")
			return
		case <-manualTicker.C:
			s.doManual()
			break
		}
	}
}

func (s *store) doCheckConcurrency() {
	s.ForeachReplicate(func(pr *PeerReplicate) bool {
		if pr.isLeader() {
			pr.doCheckConcurrency()
		}

		return true
	})
}

func (s *store) doHB() {
	s.ForeachReplicate(func(pr *PeerReplicate) bool {
		if pr.isLeader() {
			pr.doHB()
		}

		return true
	})
}

func (s *store) doGCRM() {
	log.Debugf("[GC-RM]: start")

	now := time.Now()
	newResources := make(map[string][]meta.ResourceManager, len(s.resources))

	s.Lock()
	defer s.Unlock()

	var values []string
	for key, rms := range s.resources {
		var newRMS []meta.ResourceManager
		for _, rm := range rms {
			if now.Sub(rm.LastHB) > s.cfg.RMLease {
				values = append(values, rm.RMSID)
			} else {
				newRMS = append(newRMS, rm)
			}
		}
		s.resources[key] = newRMS
	}

	if len(values) == 0 {
		return
	}

	for _, rm := range values {
		log.Infof("[GC-RM]: %s removed by timeout",
			rm)
	}

	s.resources = newResources
	log.Infof("[GC-RM]: complete with %d timeout rm",
		len(values))
	return
}

func (s *store) doManual() {
	s.ForeachReplicate(func(pr *PeerReplicate) bool {
		if pr.isLeader() {
			pr.tc.HandleManual()
		}

		return true
	})
}
