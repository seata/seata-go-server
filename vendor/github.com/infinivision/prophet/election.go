package prophet

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

// Elector a leader elector
type Elector interface {
	// Stop stop elector
	Stop(group uint64)

	// CurrentLeader returns current leader
	CurrentLeader(group uint64) (string, error)

	// ElectionLoop run leader election loop
	ElectionLoop(ctx context.Context, group uint64, currentLeader string, becomeLeader, becomeFollower func())

	// ChangeLeaderTo change leader to
	ChangeLeaderTo(group uint64, oldLeader, newLeader string) error
}

type elector struct {
	sync.RWMutex
	options       electorOptions
	client        *clientv3.Client
	lessor        clientv3.Lease
	watchers      map[uint64]clientv3.Watcher
	watcheCancels map[uint64]context.CancelFunc
	leasors       map[uint64]clientv3.LeaseID
}

// NewElector create a elector
func NewElector(client *clientv3.Client, options ...ElectorOption) (Elector, error) {
	e := &elector{
		client:        client,
		lessor:        clientv3.NewLease(client),
		leasors:       make(map[uint64]clientv3.LeaseID),
		watcheCancels: make(map[uint64]context.CancelFunc),
		watchers:      make(map[uint64]clientv3.Watcher),
	}

	for _, opt := range options {
		opt(&e.options)
	}

	e.options.adjust()

	return e, nil
}

func (e *elector) Stop(group uint64) {
	e.closeWatcher(group)
	e.closeLessor(group)
}

func (e *elector) CurrentLeader(group uint64) (string, error) {
	return e.currentLeader(group)
}

func (e *elector) ChangeLeaderTo(group uint64, oldLeader, newLeader string) error {
	err := e.addExpectLeader(group, oldLeader, newLeader)
	if err != nil {
		return err
	}
	e.closeLessor(group)
	return nil
}

func (e *elector) ElectionLoop(ctx context.Context, group uint64, current string, becomeLeader, becomeFollower func()) {
	for {
		select {
		case <-ctx.Done():
			log.Infof("[group-%d]: exit the leader election loop", group)
			return
		default:
			log.Infof("[group-%d]: ready to fetch leader peer", group)
			leader, err := e.currentLeader(group)
			if err != nil {
				log.Errorf("get current leader peer failure, errors:\n %+v",
					err)
				time.Sleep(loopInterval)
				continue
			}
			log.Infof("[group-%d]: fetch leader: %+v",
				group,
				leader)

			if len(leader) > 0 {
				if leader == current {
					// oh, we are already leader, we may meet something wrong
					// in previous campaignLeader. we can resign and campaign again.
					log.Warnf("[group-%d]: leader is matched, resign and campaign again, leader %+v",
						group,
						leader)
					if err = e.resignLeader(group, current); err != nil {
						log.Warnf("[group-%d]: resign leader failure, leader peer %+v, errors:\n %+v",
							group,
							leader,
							err)
						time.Sleep(loopInterval)
						continue
					}
				} else {
					log.Infof("[group-%d]: leader peer changed to %+v, start start watch",
						group,
						leader)
					becomeFollower()
					e.watchLeader(group)
					log.Infof("[group-%d]: leader peer %+v out",
						group,
						leader)
				}
			}

			log.Infof("[group-%d]: begin to campaign leader peer %+v",
				group,
				current)
			if err = e.campaignLeader(group, current, becomeLeader, becomeFollower); err != nil {
				log.Errorf("[group-%d]: campaign leader failure, errors:\n %+v",
					group,
					err)
				time.Sleep(time.Second * time.Duration(e.options.leaseSec))
			}
		}
	}
}

func (e *elector) currentLeader(group uint64) (string, error) {
	value, err := e.getValue(getGroupPath(e.options.leaderPath, group))
	if err != nil {
		return "", err
	}

	if len(value) == 0 {
		return "", nil
	}

	return string(value), nil
}

func (e *elector) resignLeader(group uint64, leader string) error {
	resp, err := e.leaderTxn(group, leader).Then(clientv3.OpDelete(getGroupPath(e.options.leaderPath, group))).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (e *elector) watchLeader(group uint64) {
	watcher := clientv3.NewWatcher(e.client)
	ctx, cancel := context.WithCancel(e.client.Ctx())
	e.addWatcher(group, watcher, cancel)
	defer e.closeWatcher(group)

	for {
		rch := watcher.Watch(ctx, getGroupPath(e.options.leaderPath, group))
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
			log.Infof("[group-%d]: leader watcher exit",
				group)
			return
		default:
		}
	}
}

func (e *elector) campaignLeader(group uint64, leader string, becomeLeader, becomeFollower func()) error {
	// check expect leader exists
	err := e.checkExpectLeader(group, leader)
	if err != nil {
		return err
	}

	path := getGroupPath(e.options.leaderPath, group)

	start := time.Now()
	ctx, cancel := context.WithTimeout(e.client.Ctx(), DefaultRequestTimeout)
	leaseResp, err := e.lessor.Grant(ctx, e.options.leaseSec)
	cancel()

	if cost := time.Now().Sub(start); cost > DefaultSlowRequestTime {
		log.Warnf("lessor grants too slow, cost=<%s>", cost)
	}
	if err != nil {
		return err
	}

	e.addLessor(group, leaseResp.ID)
	defer e.closeLessor(group)

	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := e.txn().
		If(clientv3.Compare(clientv3.CreateRevision(path), "=", 0)).
		Then(clientv3.OpPut(path,
			string(leader),
			clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	ch, err := e.lessor.KeepAlive(e.client.Ctx(), clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		log.Errorf("etcd KeepAlive failed with %+v", err)
		return err
	}

	var lock *concurrency.Mutex
	if e.options.lockIfBecomeLeader {
		session, err := concurrency.NewSession(e.client,
			concurrency.WithLease(clientv3.LeaseID(leaseResp.ID)))
		if err != nil {
			log.Errorf("create etcd concurrency lock session failed with %+v", err)
			return err
		}
		defer session.Close()

		// distributed lock to make sure last leader complete async tasks
		lock = concurrency.NewMutex(session, getGroupPath(e.options.lockPath, group))
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
	}

	becomeLeader()
	defer func() {
		becomeFollower()
		if lock != nil {
			lock.Unlock(context.Background())
		}
		log.Infof("[group-%d]: lock released",
			group)
	}()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Infof("[group-%d]: channel that keep alive for leader lease is closed",
					group)
				return nil
			}
		case <-e.client.Ctx().Done():
			return errors.New("server closed")
		}
	}
}

func (e *elector) closeWatcher(group uint64) {
	e.Lock()
	defer e.Unlock()

	if watcher, ok := e.watchers[group]; ok {
		watcher.Close()
		delete(e.watchers, group)
	}

	if cancel, ok := e.watcheCancels[group]; ok {
		cancel()
		delete(e.watcheCancels, group)
	}
}

func (e *elector) addWatcher(group uint64, watcher clientv3.Watcher, cancel context.CancelFunc) {
	e.Lock()
	defer e.Unlock()

	e.watchers[group] = watcher
	e.watcheCancels[group] = cancel
}

func (e *elector) checkExpectLeader(group uint64, leader string) error {
	value, err := e.getValue(getGroupExpectPath(e.options.leaderPath, group))
	if err != nil {
		return err
	}

	if len(value) == 0 {
		return nil
	}

	if string(value) != leader {
		return fmt.Errorf("expect leader is %+v", value)
	}

	return nil
}

func (e *elector) addExpectLeader(group uint64, oldLeader, newLeader string) error {
	lessor := clientv3.NewLease(e.client)
	defer lessor.Close()

	ctx, cancel := context.WithTimeout(e.client.Ctx(), DefaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, e.options.leaseSec)
	cancel()

	if err != nil {
		return err
	}

	resp, err := e.leaderTxn(group, oldLeader).Then(clientv3.OpPut(getGroupExpectPath(e.options.leaderPath, group),
		string(newLeader),
		clientv3.WithLease(leaseResp.ID))).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return fmt.Errorf("not leader")
	}

	log.Infof("[group-%d]: new expect leader %d added, with %d secs ttl",
		group,
		newLeader,
		e.options.leaseSec)
	return nil
}

func (e *elector) closeLessor(group uint64) {
	e.Lock()
	defer e.Unlock()

	if id, ok := e.leasors[group]; ok {
		e.lessor.Revoke(e.client.Ctx(), id)
		delete(e.leasors, group)
	}
}

func (e *elector) addLessor(group uint64, id clientv3.LeaseID) {
	e.Lock()
	defer e.Unlock()

	e.leasors[group] = id
}

func (e *elector) getValue(key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := e.get(key, opts...)
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

func (e *elector) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(e.client.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(e.client).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("read option failure, key=<%s>, errors:\n %+v",
			key,
			err)
		return resp, err
	}

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warnf("read option is too slow, key=<%s>, cost=<%d>",
			key,
			cost)
	}

	return resp, nil
}

func (e *elector) leaderTxn(group uint64, leader string, cs ...clientv3.Cmp) clientv3.Txn {
	return newSlowLogTxn(e.client).If(append(cs, e.leaderCmp(group, leader))...)
}

func (e *elector) txn() clientv3.Txn {
	return newSlowLogTxn(e.client)
}

func (e *elector) leaderCmp(group uint64, leader string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(getGroupPath(e.options.leaderPath, group)), "=", leader)
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
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
	if cost > DefaultSlowRequestTime {
		log.Warnf("txn runs too slow, resp=<%+v> cost=<%s> errors:\n %+v",
			resp,
			cost,
			err)
	}

	return resp, err
}

func getGroupPath(prefix string, group uint64) string {
	return fmt.Sprintf("%s/%d", prefix, group)
}

func getGroupExpectPath(prefix string, group uint64) string {
	return fmt.Sprintf("%s/expect-%d", prefix, group)
}
