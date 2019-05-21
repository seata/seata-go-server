package etcd

import (
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/fagongzi/log"
)

var (
	registryKeyPrefix = "registry-seata-"
)

// Registry etcd registry
type Registry struct {
	opts   options
	client *clientv3.Client
	lessor clientv3.Lease
}

// NewRegistry returns a etcd registry
func NewRegistry(client *clientv3.Client, opts ...Option) (*Registry, error) {
	reg := &Registry{}
	for _, opt := range opts {
		opt(&reg.opts)
	}

	reg.opts.adjust()

	reg.client = client
	reg.lessor = clientv3.NewLease(client)
	return reg, nil
}

// Register register the addr to etcd, and keepalive with a lease
func (r *Registry) Register(addr string) error {
	ch, err := r.doRegistry(addr)
	if err != nil {
		return err
	}

	go func() {
		for {
			if ch == nil {
				ch, err = r.doRegistry(addr)
				if err != nil {
					log.Errorf("[registry-etcd]: retry failed with %+v, retry after 10s", err)
					time.Sleep(time.Second * 10)
					continue
				}

				log.Errorf("[registry-etcd]: retry registry succeed")
			}

			select {
			case _, ok := <-ch:
				if !ok {
					log.Errorf("[registry-etcd]: lease keepalive failed, retry")
					break
				}
			case <-r.client.Ctx().Done():
				log.Errorf("[registry-etcd]: etcd server closed, retry")
				break
			}

			ch = nil
		}
	}()

	return nil
}

func (r *Registry) doRegistry(addr string) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	resp, err := r.lessor.Grant(r.client.Ctx(), r.opts.leaseTTL)
	if err != nil {
		return nil, err
	}

	_, err = r.client.KV.Put(r.client.Ctx(), r.registryKey(addr), addr, clientv3.WithLease(resp.ID))
	if err != nil {
		return nil, err
	}

	return r.lessor.KeepAlive(r.client.Ctx(), resp.ID)
}

func (r *Registry) registryKey(addr string) string {
	return fmt.Sprintf("%s%s-%s", registryKeyPrefix, r.opts.group, addr)
}
