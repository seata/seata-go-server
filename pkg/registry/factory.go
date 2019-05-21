package registry

import (
	"net/url"

	"github.com/fagongzi/log"
)

var (
	protocolEtcd = "etcd"
)

// NewRegistry returns a registry by url
func NewRegistry(addr string) (Registry, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case protocolEtcd:
		return newEtcdRegistry(u)
	}

	log.Fatalf("the schema %s is not support", u.Scheme)
	return nil, nil
}
