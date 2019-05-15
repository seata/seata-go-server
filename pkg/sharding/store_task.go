package sharding

import (
	"context"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/meta"
)

func (s *Store) runPRTask(ctx context.Context, id uint64) {
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
			s.foreachFragments(func(pr *PeerReplicate) {
				if pr.workerID == id && pr.tc.HandleEvent() {
					hasEvent = true
				}
			})
		}
	}
}

func (s *Store) runCheckConcurrencyTask(ctx context.Context) {
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

func (s *Store) runHBTask(ctx context.Context) {
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

func (s *Store) runGCRMTask(ctx context.Context) {
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

func (s *Store) runManualTask(ctx context.Context) {
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

func (s *Store) doCheckConcurrency() {
	s.foreachFragments(func(pr *PeerReplicate) {
		if pr.isLeader() {
			pr.doCheckConcurrency()
		}
	})
}

func (s *Store) doHB() {
	s.foreachFragments(func(pr *PeerReplicate) {
		if pr.isLeader() {
			pr.doHB()
		}
	})
}

func (s *Store) doGCRM() {
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

func (s *Store) doManual() {
	s.foreachFragments(func(pr *PeerReplicate) {
		if pr.isLeader() {
			pr.tc.HandleManual()
		}
	})
}
