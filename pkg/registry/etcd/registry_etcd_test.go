package etcd

import (
	"testing"

	"github.com/coreos/etcd/clientv3"
)

func TestRegistry(t *testing.T) {
	c, err := clientv3.NewFromURL("http://127.0.0.1:2379")
	if err != nil {
		t.Errorf("create etcd client failed with %+v", err)
		return
	}

	reg, err := NewRegistry(c, WithLease(1))
	if err != nil {
		t.Errorf("create etcd registry failed with error %+v", err)
		return
	}

	err = reg.Register("127.0.0.1:9093")
	if err != nil {
		t.Errorf("registry to etcd registry failed with error %+v", err)
		return
	}
}
