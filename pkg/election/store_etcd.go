package election

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
)

var (
	defaultRequestTimeout  = time.Second * 10
	defaultSlowRequestTime = time.Second * 10
)

type store struct {
	sync.RWMutex

	opts   options
	client *clientv3.Client

	watchers      map[uint64]clientv3.Watcher
	watcheCancels map[uint64]context.CancelFunc
	leasors       map[uint64]clientv3.Lease
}

func (s *store) currentLeader(frag uint64) (uint64, error) {
	value, err := s.getValue(getFragPath(s.opts.leaderPath, frag))
	if err != nil {
		return 0, err
	}

	if len(value) == 0 {
		return 0, nil
	}

	return format.ParseStrUInt64(string(value))
}

func (s *store) resignLeader(frag, leaderPeerID uint64) error {
	resp, err := s.leaderTxn(frag, leaderPeerID).Then(clientv3.OpDelete(getFragPath(s.opts.leaderPath, frag))).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (s *store) watchLeader(frag uint64) {
	watcher := clientv3.NewWatcher(s.client)
	ctx, cancel := context.WithCancel(s.client.Ctx())
	s.addWatcher(frag, watcher, cancel)
	defer s.closeWatcher(frag)

	for {
		rch := watcher.Watch(ctx, getFragPath(s.opts.leaderPath, frag))
		for wresp := range rch {
			if wresp.Canceled {
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			log.Infof("[frag-%d]: leader watcher exit",
				frag)
			return
		default:
		}
	}
}

func (s *store) campaignLeader(frag, leaderPeerID uint64, becomeLeader, becomeFollower func()) error {
	// check expect leader exists
	err := s.checkExpectLeader(frag, leaderPeerID)
	if err != nil {
		return err
	}

	path := getFragPath(s.opts.leaderPath, frag)

	lessor := clientv3.NewLease(s.client)
	s.addLessor(frag, lessor)
	defer s.closeLessor(frag)

	start := time.Now()
	ctx, cancel := context.WithTimeout(s.client.Ctx(), defaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, s.opts.leaseSec)
	cancel()

	if cost := time.Now().Sub(start); cost > defaultSlowRequestTime {
		log.Warnf("lessor grants too slow, cost=<%s>", cost)
	}
	if err != nil {
		return err
	}

	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.CreateRevision(path), "=", 0)).
		Then(clientv3.OpPut(path,
			fmt.Sprintf("%d", leaderPeerID),
			clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	ch, err := lessor.KeepAlive(s.client.Ctx(), clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		log.Errorf("etcd KeepAlive failed with %+v", err)
		return err
	}

	session, err := concurrency.NewSession(s.client,
		concurrency.WithLease(clientv3.LeaseID(leaseResp.ID)))
	if err != nil {
		log.Errorf("create etcd concurrency lock session failed with %+v", err)
		return err
	}
	defer session.Close()

	// distributed lock to make sure last leader complete async tasks
	lock := concurrency.NewMutex(session, getFragPath(s.opts.lockPath, frag))
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		err = lock.Lock(ctx)
		if err != nil {
			cancel()
			log.Errorf("get election lock failed with %+v", err)
			continue
		}

		cancel()
		break
	}

	becomeLeader()
	defer func() {
		becomeFollower()
		lock.Unlock(context.Background())
		log.Infof("[frag-%d]: lock released",
			frag)
	}()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Infof("[frag-%d]: channel that keep alive for leader lease is closed",
					frag)
				return nil
			}
		case <-s.client.Ctx().Done():
			return errors.New("server closed")
		}
	}
}

func (s *store) closeWatcher(frag uint64) {
	s.Lock()
	defer s.Unlock()

	if watcher, ok := s.watchers[frag]; ok {
		watcher.Close()
		delete(s.watchers, frag)
	}

	if cancel, ok := s.watcheCancels[frag]; ok {
		cancel()
		delete(s.watcheCancels, frag)
	}
}

func (s *store) addWatcher(frag uint64, watcher clientv3.Watcher, cancel context.CancelFunc) {
	s.Lock()
	defer s.Unlock()

	s.watchers[frag] = watcher
	s.watcheCancels[frag] = cancel
}

func (s *store) checkExpectLeader(frag uint64, leader uint64) error {
	value, err := s.getValue(getFragExpectPath(s.opts.leaderPath, frag))
	if err != nil {
		return err
	}

	if len(value) == 0 {
		return nil
	}

	if string(value) != fmt.Sprintf("%d", leader) {
		return fmt.Errorf("expect leader is %s", string(value))
	}

	return nil
}

func (s *store) addExpectLeader(frag uint64, oldLeaderPeerID, newLeaderPeerID uint64) error {
	lessor := clientv3.NewLease(s.client)
	defer lessor.Close()

	ctx, cancel := context.WithTimeout(s.client.Ctx(), defaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, s.opts.leaseSec)
	cancel()

	if err != nil {
		return err
	}

	resp, err := s.leaderTxn(frag, oldLeaderPeerID).Then(clientv3.OpPut(getFragExpectPath(s.opts.leaderPath, frag),
		fmt.Sprintf("%d", newLeaderPeerID),
		clientv3.WithLease(leaseResp.ID))).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return fmt.Errorf("not leader")
	}

	log.Infof("[frag-%d]: new expect leader %d added, with %d secs ttl",
		frag,
		newLeaderPeerID,
		s.opts.leaseSec)
	return nil
}

func (s *store) closeLessor(frag uint64) {
	s.Lock()
	defer s.Unlock()

	if lessor, ok := s.leasors[frag]; ok {
		lessor.Close()
		delete(s.leasors, frag)
	}
}

func (s *store) addLessor(frag uint64, lessor clientv3.Lease) {
	s.Lock()
	defer s.Unlock()

	s.leasors[frag] = lessor
}

func (s *store) getValue(key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := s.get(key, opts...)
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, nil
	} else if resp.Count > 1 {
		return nil, fmt.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, nil
}

func (s *store) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), defaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(s.client).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("read option failure, key=<%s>, errors:\n %+v",
			key,
			err)
		return resp, err
	}

	if cost := time.Since(start); cost > defaultSlowRequestTime {
		log.Warnf("read option is too slow, key=<%s>, cost=<%d>",
			key,
			cost)
	}

	return resp, nil
}

func (s *store) leaderTxn(frag, leaderPeerID uint64, cs ...clientv3.Cmp) clientv3.Txn {
	return newSlowLogTxn(s.client).If(append(cs, s.leaderCmp(frag, leaderPeerID))...)
}

func (s *store) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

func (s *store) leaderCmp(frag, leaderPeerID uint64) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(getFragPath(s.opts.leaderPath, frag)), "=", fmt.Sprintf("%d", leaderPeerID))
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), defaultRequestTimeout)
	return &slowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

func (t *slowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.If(cs...),
		cancel: t.cancel,
	}
}

func (t *slowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.Then(ops...),
		cancel: t.cancel,
	}
}

// Commit implements Txn Commit interface.
func (t *slowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Now().Sub(start)
	if cost > defaultSlowRequestTime {
		log.Warnf("txn runs too slow, resp=<%+v> cost=<%s> errors:\n %+v",
			resp,
			cost,
			err)
	}

	return resp, err
}

func getFragPath(prefix string, frag uint64) string {
	return fmt.Sprintf("%s/%d", prefix, frag)
}

func getFragExpectPath(prefix string, frag uint64) string {
	return fmt.Sprintf("%s/expect-%d", prefix, frag)
}
