package sharding

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/dgraph-io/badger"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/json"
	"github.com/infinivision/prophet"
	"seata.io/server/pkg/meta"
)

var (
	basePath      = "/seata"
	fragmentsPath = fmt.Sprintf("%s/framents", basePath)
	storesPath    = fmt.Sprintf("%s/stores", basePath)

	defaultRequestTimeout       = time.Second * 10
	endID                       = uint64(math.MaxUint64)
	limit                 int64 = 64

	errTxnFailed = errors.New("failed to commit transaction")
)

type storage interface {
	get(key []byte) ([]byte, error)
	set(key, value []byte) error

	countFragments(id uint64) (int, error)
	loadFragments(store uint64, handleFunc func(value []byte) (uint64, error)) error
	createFragment(frag meta.Fragment, peer prophet.Peer) error
	updateFragment(storeID uint64, frag meta.Fragment) error
	removeFragmentOnStore(frag meta.Fragment, peer prophet.Peer) error
	removeFragment(id uint64) error
}

type defaultStorage struct {
	db      *badger.DB
	etcdCli *clientv3.Client
}

func newStorage(dir string, etcdCli *clientv3.Client) *defaultStorage {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalf("create storage failed with %+v", err)
	}

	return &defaultStorage{
		db:      db,
		etcdCli: etcdCli,
	}
}

func (s *defaultStorage) loadFragments(store uint64, handleFunc func(value []byte) (uint64, error)) error {
	prefix := getKey(store, storesPath)

	start := uint64(0)
	end := getKey(endID, prefix)
	withRange := clientv3.WithRange(end)
	withLimit := clientv3.WithLimit(limit)

	for {
		resp, err := s.getKV(getKey(start, prefix), withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			id, err := handleFunc(item.Value)
			if err != nil {
				return err
			}

			start = id + 1
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

func (s *defaultStorage) countFragments(id uint64) (int, error) {
	resp, err := s.getKV(getKey(id, storesPath), clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return 0, err
	}
	return int(resp.Count), nil
}

func (s *defaultStorage) createFragment(frag meta.Fragment, peer prophet.Peer) error {
	value := string(json.MustMarshal(&frag))
	resp, err := s.etcdCli.Txn(s.etcdCli.Ctx()).
		Then(clientv3.OpPut(getKey(frag.ID, fragmentsPath), value),
			clientv3.OpPut(getKey(frag.ID, getKey(peer.ContainerID, storesPath)), value)).
		Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (s *defaultStorage) updateFragment(storeID uint64, frag meta.Fragment) error {
	value := string(json.MustMarshal(&frag))
	resp, err := s.etcdCli.Txn(s.etcdCli.Ctx()).
		Then(clientv3.OpPut(getKey(frag.ID, getKey(storeID, storesPath)), value)).
		Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (s *defaultStorage) removeFragmentOnStore(frag meta.Fragment, peer prophet.Peer) error {
	value := string(json.MustMarshal(&frag))
	resp, err := s.etcdCli.Txn(s.etcdCli.Ctx()).
		Then(clientv3.OpPut(getKey(frag.ID, fragmentsPath), value),
			clientv3.OpDelete(getKey(frag.ID, getKey(peer.ContainerID, storesPath)))).
		Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (s *defaultStorage) removeFragment(id uint64) error {
	value, err := s.getValue(getKey(id, fragmentsPath))
	if err != nil {
		return err
	}

	var frag meta.Fragment
	json.MustUnmarshal(&frag, value)

	var ops []clientv3.Op
	for _, p := range frag.Peers {
		ops = append(ops, clientv3.OpDelete(getKey(id, getKey(p.ContainerID, storesPath))))
	}

	resp, err := s.etcdCli.Txn(s.etcdCli.Ctx()).
		Then(ops...).
		Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errTxnFailed
	}

	return nil
}

func (s *defaultStorage) getKV(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.etcdCli.Ctx(), defaultRequestTimeout)
	defer cancel()

	return clientv3.NewKV(s.etcdCli).Get(ctx, key, opts...)
}

func (s *defaultStorage) getValue(key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(s.etcdCli.Ctx(), defaultRequestTimeout)
	defer cancel()

	resp, err := clientv3.NewKV(s.etcdCli).Get(ctx, key)
	if nil != err {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	return resp.Kvs[0].Value, nil
}

func (s *defaultStorage) set(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *defaultStorage) get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}

			return err
		}

		value, err = item.Value()
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func getKey(id uint64, base string) string {
	return fmt.Sprintf("%s/%020d", base, id)
}

// just for test
type emptyStorage struct {
}

func (s *emptyStorage) get(key []byte) ([]byte, error)        { return nil, nil }
func (s *emptyStorage) set(key, value []byte) error           { return nil }
func (s *emptyStorage) countFragments(id uint64) (int, error) { return 0, nil }
func (s *emptyStorage) loadFragments(store uint64, handleFunc func(value []byte) (uint64, error)) error {
	return nil
}
func (s *emptyStorage) createFragment(frag meta.Fragment, peer prophet.Peer) error        { return nil }
func (s *emptyStorage) updateFragment(storeID uint64, frag meta.Fragment) error           { return nil }
func (s *emptyStorage) removeFragmentOnStore(frag meta.Fragment, peer prophet.Peer) error { return nil }
func (s *emptyStorage) removeFragment(id uint64) error                                    { return nil }
