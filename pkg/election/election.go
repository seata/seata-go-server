package election

import (
	"context"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/fagongzi/log"
)

var (
	loopInterval = 200 * time.Millisecond
)

// Elector leader election
type Elector interface {
	// Stop stop elector
	Stop(frag uint64)

	// CurrentLeader returns current leader
	CurrentLeader(frag uint64) (uint64, error)

	// ElectionLoop run leader election loop
	ElectionLoop(ctx context.Context, frag, currentPeerID uint64, nodeChecker func(uint64) bool, becomeLeader, becomeFollower func())

	// ChangeLeaderTo change leader to
	ChangeLeaderTo(frag uint64, oldLeader, newLeader uint64) error
}

type elector struct {
	opts  options
	store *store
}

// NewElector create a elector by options
func NewElector(opts ...Option) (Elector, error) {
	e := &elector{}
	for _, opt := range opts {
		opt(&e.opts)
	}

	err := e.opts.adjust()
	if err != nil {
		return nil, err
	}

	e.store = &store{
		opts:          e.opts,
		client:        e.opts.client,
		leasors:       make(map[uint64]clientv3.Lease),
		watcheCancels: make(map[uint64]context.CancelFunc),
		watchers:      make(map[uint64]clientv3.Watcher),
	}
	return e, nil
}

func (e *elector) Stop(frag uint64) {
	e.store.closeWatcher(frag)
	e.store.closeLessor(frag)
}

func (e *elector) CurrentLeader(frag uint64) (uint64, error) {
	return e.store.currentLeader(frag)
}

func (e *elector) ChangeLeaderTo(frag uint64, oldLeader, newLeader uint64) error {
	err := e.store.addExpectLeader(frag, oldLeader, newLeader)
	if err != nil {
		return err
	}
	e.store.closeLessor(frag)
	return nil
}

func (e *elector) ElectionLoop(ctx context.Context, frag, currentPeerID uint64, nodeChecker func(uint64) bool, becomeLeader, becomeFollower func()) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("[frag-%d]: exit the leader election loop", frag)
			return
		default:
			log.Infof("[frag-%d]: ready to fetch leader peer", frag)
			leaderPeerID, err := e.store.currentLeader(frag)
			if err != nil {
				log.Errorf("get current leader peer failure, errors:\n %+v",
					err)
				time.Sleep(loopInterval)
				continue
			}
			log.Infof("[frag-%d]: fetch leader peer: %d",
				frag,
				leaderPeerID)

			if leaderPeerID > 0 {
				if nodeChecker(leaderPeerID) {
					// oh, we are already leader, we may meet something wrong
					// in previous campaignLeader. we can resign and campaign again.
					log.Warnf("[frag-%d]: leader is matched, resign and campaign again, leader peer %d",
						frag,
						leaderPeerID)
					if err = e.store.resignLeader(frag, currentPeerID); err != nil {
						log.Warnf("[frag-%d]: resign leader failure, leader peer %d, errors:\n %+v",
							frag,
							leaderPeerID,
							err)
						time.Sleep(loopInterval)
						continue
					}
				} else {
					log.Infof("[frag-%d]: leader peer changed to %d, start start watch",
						frag,
						leaderPeerID)
					becomeFollower()
					e.store.watchLeader(frag)
					log.Infof("[frag-%d]: leader peer %d out",
						frag,
						leaderPeerID)
				}
			}

			log.Infof("[frag-%d]: begin to campaign leader peer %d",
				frag,
				currentPeerID)
			if err = e.store.campaignLeader(frag, currentPeerID, becomeLeader, becomeFollower); err != nil {
				log.Errorf("[frag-%d]: campaign leader failure, errors:\n %+v",
					frag,
					err)
				time.Sleep(time.Second * time.Duration(e.opts.leaseSec))
			}
		}
	}
}
