package prophet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/fagongzi/util/format"
)

const (
	// DefaultTimeout default timeout
	DefaultTimeout = time.Second * 3
	// DefaultRequestTimeout default request timeout
	DefaultRequestTimeout = 10 * time.Second
	// DefaultSlowRequestTime default slow request time
	DefaultSlowRequestTime = time.Second * 1

	idBatch = 1000
)

var (
	errMaybeNotLeader = errors.New("may be not leader")
	errTxnFailed      = errors.New("failed to commit transaction")

	errSchedulerExisted  = errors.New("scheduler is existed")
	errSchedulerNotFound = errors.New("scheduler is not found")
)

var (
	endID = uint64(math.MaxUint64)
)

func getCurrentClusterMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	members, err := client.MemberList(ctx)
	cancel()

	return members, err
}

type etcdStore struct {
	sync.Mutex
	adapter       Adapter
	client        *clientv3.Client
	namespace     string
	idPath        string
	leaderPath    string
	resourcePath  string
	containerPath string
	clusterPath   string

	store     Store
	signature string
	base      uint64
	end       uint64
}

func newEtcdStore(client *clientv3.Client, namespace string, adapter Adapter, signature string) Store {
	return &etcdStore{
		adapter:       adapter,
		client:        client,
		namespace:     namespace,
		signature:     signature,
		idPath:        fmt.Sprintf("%s/meta/id", namespace),
		leaderPath:    fmt.Sprintf("%s/meta/leader", namespace),
		resourcePath:  fmt.Sprintf("%s/meta/resources", namespace),
		containerPath: fmt.Sprintf("%s/meta/containers", namespace),
		clusterPath:   fmt.Sprintf("%s/cluster", namespace),
	}
}

func (s *etcdStore) GetCurrentLeader() (*Node, error) {
	resp, err := s.getValue(s.leaderPath)
	if err != nil {
		return nil, err
	}

	if nil == resp {
		return nil, nil
	}

	v := &Node{}
	err = json.Unmarshal(resp, v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (s *etcdStore) ResignLeader() error {
	resp, err := s.leaderTxn(s.signature).Then(clientv3.OpDelete(s.leaderPath)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (s *etcdStore) WatchLeader() {
	watcher := clientv3.NewWatcher(s.client)
	defer watcher.Close()

	ctx := s.client.Ctx()
	for {
		rch := watcher.Watch(ctx, s.leaderPath)
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
			// server closed, return
			return
		default:
		}
	}
}

func (s *etcdStore) CampaignLeader(leaderLeaseTTL int64, enableLeaderFun, disableLeaderFun func()) error {
	lessor := clientv3.NewLease(s.client)
	defer lessor.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultRequestTimeout)
	leaseResp, err := lessor.Grant(ctx, leaderLeaseTTL)
	cancel()

	if cost := time.Now().Sub(start); cost > DefaultSlowRequestTime {
		log.Warnf("prophet: lessor grants too slow, cost=<%s>", cost)
	}

	if err != nil {
		return err
	}

	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.CreateRevision(s.leaderPath), "=", 0)).
		Then(clientv3.OpPut(s.leaderPath, s.signature, clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	// Make the leader keepalived.
	ch, err := lessor.KeepAlive(s.client.Ctx(), clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		return err
	}

	enableLeaderFun()
	defer disableLeaderFun()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Info("prophet: channel that keep alive for leader lease is closed")
				return nil
			}
		case <-s.client.Ctx().Done():
			return errors.New("server closed")
		}
	}
}

// PutContainer returns nil if container is add or update succ
func (s *etcdStore) PutContainer(meta Container) error {
	key := s.getKey(meta.ID(), s.containerPath)
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	return s.save(key, string(data))
}

// GetContainer returns the spec container
func (s *etcdStore) GetContainer(id uint64) (Container, error) {
	key := s.getKey(id, s.containerPath)
	data, err := s.getValue(key)
	if err != nil {
		return nil, err
	}

	c := s.adapter.NewContainer()
	err = c.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// PutResource returns nil if resource is add or update succ
func (s *etcdStore) PutResource(meta Resource) error {
	key := s.getKey(meta.ID(), s.resourcePath)
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	return s.save(key, string(data))
}

func (s *etcdStore) LoadResources(limit int64, do func(Resource)) error {
	startID := uint64(0)
	endKey := s.getKey(endID, s.resourcePath)
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)

	for {
		startKey := s.getKey(startID, s.resourcePath)
		resp, err := s.get(startKey, withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			v := s.adapter.NewResource()
			err := v.Unmarshal(item.Value)
			if err != nil {
				return err
			}

			startID = v.ID() + 1
			do(v)
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

func (s *etcdStore) LoadContainers(limit int64, do func(Container)) error {
	startID := uint64(0)
	endKey := s.getKey(endID, s.containerPath)
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)

	for {
		startKey := s.getKey(startID, s.containerPath)
		resp, err := s.get(startKey, withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			v := s.adapter.NewContainer()
			err := v.Unmarshal(item.Value)
			if err != nil {
				return err
			}

			startID = v.ID() + 1
			do(v)
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

func (s *etcdStore) AllocID() (uint64, error) {
	s.Lock()
	defer s.Unlock()

	if s.base == s.end {
		end, err := s.generate()
		if err != nil {
			return 0, err
		}

		s.end = end
		s.base = s.end - idBatch
	}

	s.base++
	return s.base, nil
}

func (s *etcdStore) generate() (uint64, error) {
	value, err := s.getID()
	if err != nil {
		return 0, err
	}

	max := value + idBatch

	// create id
	if value == 0 {
		max := value + idBatch
		err := s.createID(s.signature, max)
		if err != nil {
			return 0, err
		}

		return max, nil
	}

	err = s.updateID(s.signature, value, max)
	if err != nil {
		return 0, err
	}

	return max, nil
}

// PutBootstrapped put cluster is bootstrapped
func (s *etcdStore) PutBootstrapped(container Container, res Resource) (bool, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultTimeout)
	defer cancel()

	// build operations
	var ops []clientv3.Op

	meta, err := container.Marshal()
	if err != nil {
		return false, err
	}
	ops = append(ops, clientv3.OpPut(s.getKey(container.ID(), s.containerPath), string(meta)))

	meta, err = res.Marshal()
	if err != nil {
		return false, err
	}
	ops = append(ops, clientv3.OpPut(s.getKey(res.ID(), s.resourcePath), string(meta)))
	ops = append(ops, clientv3.OpPut(s.clusterPath, string(meta)))

	// txn
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(s.clusterPath), "=", 0)).
		Then(ops...).
		Commit()

	if err != nil {
		return false, err
	}

	// already bootstrapped
	if !resp.Succeeded {
		return false, nil
	}

	return true, nil
}

func (s *etcdStore) getID() (uint64, error) {
	resp, err := s.getValue(s.idPath)
	if err != nil {
		return 0, err
	}

	if resp == nil {
		return 0, nil
	}

	return format.BytesToUint64(resp)
}

func (s *etcdStore) createID(leaderSignature string, value uint64) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(s.idPath), "=", 0)
	op := clientv3.OpPut(s.idPath, string(format.Uint64ToBytes(value)))
	resp, err := s.leaderTxn(leaderSignature, cmp).Then(op).Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errMaybeNotLeader
	}

	return nil
}

func (s *etcdStore) updateID(leaderSignature string, old, value uint64) error {
	cmp := clientv3.Compare(clientv3.Value(s.idPath), "=", string(format.Uint64ToBytes(old)))
	op := clientv3.OpPut(s.idPath, string(format.Uint64ToBytes(value)))
	resp, err := s.leaderTxn(leaderSignature, cmp).Then(op).Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errMaybeNotLeader
	}

	return nil
}

func (s *etcdStore) getKey(id uint64, base string) string {
	return fmt.Sprintf("%s/%020d", base, id)
}

func (s *etcdStore) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

func (s *etcdStore) leaderTxn(leaderSignature string, cs ...clientv3.Cmp) clientv3.Txn {
	return newSlowLogTxn(s.client).If(append(cs, s.leaderCmp(leaderSignature))...)
}

func (s *etcdStore) leaderCmp(leaderSignature string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(s.leaderPath), "=", leaderSignature)
}

func (s *etcdStore) getValue(key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := s.get(key, opts...)
	if err != nil {
		return nil, err
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, fmt.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, nil
}

func (s *etcdStore) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(s.client).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("prophet: read option failure, key=<%s>, errors:\n %+v",
			key,
			err)
		return resp, err
	}

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warnf("prophet: read option is too slow, key=<%s>, cost=<%d>",
			key,
			cost)
	}

	return resp, nil
}

func (s *etcdStore) save(key, value string) error {
	resp, err := s.txn().Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (s *etcdStore) create(key, value string) error {
	resp, err := s.txn().If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (s *etcdStore) delete(key string, opts ...clientv3.OpOption) error {
	resp, err := s.txn().Then(clientv3.OpDelete(key, opts...)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
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
		log.Warnf("prophet: txn runs too slow, resp=<%+v> cost=<%s> errors:\n %+v",
			resp,
			cost,
			err)
	}

	return resp, err
}
