package sharding

import (
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/taas/pkg/meta"
)

func (s *Store) handleReplicate(data interface{}) interface{} {
	if msg, ok := data.(*meta.HBMsg); ok {
		return s.handleHB(msg)
	} else if msg, ok := data.(*meta.HBACKMsg); ok {
		return s.handleHBACK(msg)
	} else if msg, ok := data.(*meta.RemoveMsg); ok {
		return s.handleRemovePR(msg)
	}

	log.Fatalf("not support msg %T: %+v",
		data,
		data)
	return nil
}

func (s *Store) handleHB(msg *meta.HBMsg) interface{} {
	pr := s.getFragment(msg.Frag.ID, false)
	if pr == nil {
		pr, err := createPeerReplicate(s, msg.Frag)
		if err != nil {
			log.Fatalf("create frag %+v failed with %+v",
				msg.Frag,
				err)
			return nil
		}

		s.doAddPR(pr)
		return nil
	}

	pr.Lock()
	defer pr.Unlock()

	// stale peer, remove
	if pr.frag.Version > msg.Frag.Version {
		return &meta.RemoveMsg{
			ID: pr.frag.ID,
		}
	}

	update := pr.frag.Version < msg.Frag.Version
	pr.frag = msg.Frag
	if update {
		pr.store.mustUpdateFragmentOnStore(pr.frag, pr.peer)
	}

	return &meta.HBACKMsg{
		ID:      pr.frag.ID,
		Version: pr.frag.Version,
		Peer:    pr.peer,
	}
}

func (s *Store) handleHBACK(msg *meta.HBACKMsg) interface{} {
	pr := s.getFragment(msg.ID, true)
	if pr == nil {
		return nil
	}

	pr.Lock()
	defer pr.Unlock()

	// stale peer, remove
	if pr.frag.Version > msg.Version {
		s.trans.sendMsg(msg.Peer.ContainerID, &meta.RemoveMsg{
			ID: pr.frag.ID,
		})
		return nil
	}

	pr.removePendingPeer(msg.Peer)
	pr.heartbeatsMap.Store(msg.Peer.ID, time.Now())
	return nil
}

func (s *Store) handleRemovePR(msg *meta.RemoveMsg) interface{} {
	pr := s.getFragment(msg.ID, false)
	if pr == nil {
		return nil
	}

	pr.Lock()
	defer pr.Unlock()

	pr.destroy()
	s.doRemovePR(pr.frag.ID)
	log.Infof("%s destroy complete",
		pr.tag)
	return nil
}
